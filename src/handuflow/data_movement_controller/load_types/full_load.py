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


class FullLoad(BaseLoadStrategy):

    def __init__(self, config: LoadConfig, spark: SparkSession) -> None:
        super().__init__(config=config, spark=spark)
        self.logger = logging.getLogger(__name__)
        self.logger.info("Initializing FULL_LOAD data transfer component...")

    def load(self) -> LoadResult:
        try:
            self._enforce_load_type_consistency()
            staging_full_table = (
                f"{self._staging_schema}.t_full_{self.config.target_table_name}"
            )
            self.logger.warning(
                "FULL_LOAD: staging (%s) and target (%s) use mode=overwrite; "
                "prior contents are replaced each run and are not restored "
                "incrementally.",
                staging_full_table,
                self._current_target_table_name,
            )
            is_staging_layer_created = self._create_staging_layer()

            if not is_staging_layer_created:
                self.logger.error(
                    "FULL LOAD failed for %s: staging layer was not created.",
                    self._current_target_table_name,
                )
                return LoadResult(
                    feed_id=self.config.master_specs["feed_id"],
                    success=False,
                    total_rows_inserted=0,
                    total_rows_deleted=0,
                    total_rows_updated=0,
                )

            if not self._staging_source_changed:
                self.logger.info(
                    "FULL LOAD skipped: source matches staging for %s.",
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

            full_staging_df = AuditColumns.prepare_full_load_snapshot(
                self._current_staging_table_df,
                self.config.feed_specs,
            )
            record_count = full_staging_df.count()
            if record_count == 0:
                self.logger.info(
                    "FULL LOAD skipped: staging empty for %s.",
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
            target = self._current_target_table_name
            previous_count = (
                self.spark.table(target).count()
                if self.spark.catalog.tableExists(target)
                else 0
            )
            if self._target_partition_mismatch(target):
                self._rebuild_target_partition_layout(
                    target,
                    full_staging_df,
                    load_type="FULL_LOAD",
                )
            else:
                self._write_delta_table(
                    full_staging_df,
                    target,
                    mode="overwrite",
                    overwrite_schema=not self.spark.catalog.tableExists(target),
                )
            AuditColumns.assert_target_schema(
                self.spark,
                self._current_target_table_name,
                self.config.feed_specs,
                TargetLoadKind.FULL_LOAD,
            )
            self._post_load_verify(expected_row_count=record_count)
            self.logger.info(
                "FULL LOAD completed for %s (%s records).",
                self._current_target_table_name,
                record_count,
            )
            return LoadResult(
                feed_id=self.config.master_specs["feed_id"],
                success=True,
                total_rows_inserted=record_count,
                total_rows_updated=0,
                total_rows_deleted=previous_count,
            )
        except Exception as e:
            raise DataLoadException(
                error_code="HF044",
                message=(
                    f"Feed ID: {self.config.master_specs['feed_id']}, "
                    f"Error during FULL LOAD for {self._current_target_table_name}: {e}"
                ),
                original_exception=e,
            )
