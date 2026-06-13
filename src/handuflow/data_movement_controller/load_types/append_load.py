# inbuilt
import logging

# external
from pyspark.sql import SparkSession

# internal
from handuflow.data_movement_controller.audit_columns import (
    AuditColumns,
    TargetLoadKind,
)
from handuflow.data_movement_controller.data_class.load_result import LoadResult
from handuflow.data_movement_controller.data_class.load_config import LoadConfig
from handuflow.data_movement_controller.base_load_strategy import BaseLoadStrategy
from handuflow.exception.data_load_exception import DataLoadException


class AppendLoad(BaseLoadStrategy):

    def __init__(self, config: LoadConfig, spark: SparkSession) -> None:
        super().__init__(config=config, spark=spark)
        self.logger = logging.getLogger(__name__)
        self.logger.info("Initializing APPEND_LOAD data transfer component...")

    def load(self):
        try:
            self._enforce_load_type_consistency()
            is_staging_layer_created = self._create_staging_layer()
            if not is_staging_layer_created:
                self.logger.warning(
                    "APPEND LOAD skipped for %s: source unchanged. "
                    "No staging or target writes.",
                    self._current_target_table_name,
                )
                return LoadResult(
                    feed_id=self.config.master_specs["feed_id"],
                    success=True,
                    skipped=True,
                    total_rows_inserted=0,
                    total_rows_deleted=0,
                    total_rows_updated=0,
                )

            full_snapshot_df = AuditColumns.prepare_full_load_snapshot(
                self._current_staging_table_df,
                self.config.feed_specs,
            )
            if self._target_partition_mismatch(self._current_target_table_name):
                # Rebuild from the append target itself so deletes/updates semantics
                # are preserved (staging t_full mirrors current source, not append history).
                if self.spark.catalog.tableExists(self._current_target_table_name):
                    rebuild_df = AuditColumns.enforce_business_schema(
                        self.spark.table(self._current_target_table_name),
                        self.config.feed_specs,
                    )
                else:
                    rebuild_df = full_snapshot_df
                if not self._rebuild_target_partition_layout(
                    self._current_target_table_name,
                    rebuild_df,
                    load_type="APPEND_LOAD",
                ):
                    raise DataLoadException(
                        message=(
                            f"APPEND_LOAD partition layout mismatch on "
                            f"{self._current_target_table_name} could not be rebuilt"
                        ),
                        original_exception=None,
                    )
                row_count = self.spark.table(
                    self._current_target_table_name
                ).count()
                self._post_load_verify(expected_row_count=row_count)
                return LoadResult(
                    feed_id=self.config.master_specs["feed_id"],
                    success=True,
                    total_rows_inserted=row_count,
                    total_rows_deleted=0,
                    total_rows_updated=0,
                )

            incremental_df = AuditColumns.prepare_append_inserts(
                self._current_staging_incremental_table_df,
                self.config.feed_specs,
            )
            inc_count = incremental_df.count()

            if inc_count == 0:
                self.logger.info(
                    "No new inserts to append for %s.",
                    self._current_target_table_name,
                )
                return LoadResult(
                    feed_id=self.config.master_specs["feed_id"],
                    success=True,
                    skipped=True,
                    total_rows_inserted=0,
                    total_rows_deleted=0,
                    total_rows_updated=0,
                )

            self.logger.info(
                "Appending %s record(s) to %s.",
                inc_count,
                self._current_target_table_name,
            )
            rows_before = (
                self.spark.table(self._current_target_table_name).count()
                if self.spark.catalog.tableExists(self._current_target_table_name)
                else 0
            )
            self._write_delta_table(
                incremental_df,
                self._current_target_table_name,
                mode="append",
            )
            self.spark.sql(
                f"ALTER TABLE {self._current_target_table_name} "
                f"SET TBLPROPERTIES ('data.load_type' = '{self.config.master_specs['load_type']}')"
            )
            AuditColumns.assert_target_schema(
                self.spark,
                self._current_target_table_name,
                self.config.feed_specs,
                TargetLoadKind.APPEND_LOAD,
            )
            self._post_load_verify(expected_row_count=rows_before + inc_count)

            self.logger.info(
                "APPEND LOAD completed for %s.",
                self._current_target_table_name,
            )
            return LoadResult(
                feed_id=self.config.master_specs["feed_id"],
                success=True,
                total_rows_inserted=inc_count,
                total_rows_deleted=0,
                total_rows_updated=0,
            )

        except Exception as e:
            raise DataLoadException(
                original_exception=e,
                message=f"Error during APPEND_LOAD for {self._current_target_table_name}: {e}",
                error_code="HF041",
            )
