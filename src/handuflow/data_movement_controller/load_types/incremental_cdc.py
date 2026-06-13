# inbuilt
import logging

# external
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

# internal
from handuflow.data_movement_controller.audit_columns import (
    AuditColumns,
    CDC_STREAM_COLUMN,
    TARGET_ROW_HASH_COLUMN,
    TargetLoadKind,
)
from handuflow.data_movement_controller.data_class.load_result import LoadResult
from handuflow.data_movement_controller.data_class.load_config import LoadConfig
from handuflow.data_movement_controller.base_load_strategy import BaseLoadStrategy
from handuflow.data_movement_controller.load_integrity import LoadIntegrityVerifier
from handuflow.exception.data_load_exception import DataLoadException


class IncrementalCDC(BaseLoadStrategy):
    def __init__(self, config: LoadConfig, spark: SparkSession) -> None:
        super().__init__(config=config, spark=spark)
        self.logger = logging.getLogger(__name__)
        self.logger.info("Initializing INCREMENTAL_CDC data transfer component...")

    def load(self) -> LoadResult:
        try:
            self._enforce_load_type_consistency()
            if not self._create_staging_layer():
                self.logger.warning(
                    "Incremental CDC skipped for %s: source unchanged. "
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

            target_table = self._current_target_table_name
            try:
                all_keys = LoadIntegrityVerifier.require_primary_keys(
                    self.config.feed_specs, target_table
                )
            except DataLoadException:
                return LoadResult(
                    feed_id=self.config.master_specs["feed_id"],
                    success=False,
                    skipped=False,
                    total_rows_inserted=0,
                    total_rows_deleted=0,
                    total_rows_updated=0,
                )

            incr_df = AuditColumns.prepare_cdc_stream(
                self._current_staging_incremental_table_df,
                self.config.feed_specs,
                all_keys,
            )
            if incr_df.count() == 0:
                self.logger.warning(
                    "No incremental changes for %s.", target_table
                )
                return LoadResult(
                    feed_id=self.config.master_specs["feed_id"],
                    success=False,
                    skipped=True,
                    total_rows_inserted=0,
                    total_rows_deleted=0,
                    total_rows_updated=0,
                )

            non_key_columns = AuditColumns.non_key_business_columns(
                self.config.feed_specs, all_keys
            )

            if self._rebuild_target_partition_layout(
                target_table,
                AuditColumns.prepare_cdc_full_snapshot(
                    self._current_staging_table_df,
                    self.config.feed_specs,
                    all_keys,
                ),
                load_type="INCREMENTAL_CDC",
            ):
                row_count = self.spark.table(target_table).count()
                self._post_load_verify(minimum_row_count=row_count)
                return LoadResult(
                    feed_id=self.config.master_specs["feed_id"],
                    success=True,
                    skipped=False,
                    total_rows_inserted=row_count,
                    total_rows_deleted=0,
                    total_rows_updated=0,
                )

            if not self.spark.catalog.tableExists(target_table):
                self.logger.info("Creating CDC target table %s.", target_table)
                initial_df = AuditColumns.prepare_cdc_initial_table(
                    incr_df, self.config.feed_specs
                )
                self._write_delta_table(
                    initial_df,
                    target_table,
                    mode="overwrite",
                    overwrite_schema=True,
                )
                inserted = initial_df.count()
                self._post_load_verify(minimum_row_count=inserted)
                return LoadResult(
                    feed_id=self.config.master_specs["feed_id"],
                    success=True,
                    skipped=False,
                    total_rows_inserted=inserted,
                    total_rows_deleted=0,
                    total_rows_updated=0,
                )

            AuditColumns.ensure_row_hash_on_target(
                self.spark, target_table, all_keys, non_key_columns
            )
            AuditColumns.assert_target_schema(
                self.spark,
                target_table,
                self.config.feed_specs,
                TargetLoadKind.INCREMENTAL_CDC,
            )

            delta_target = DeltaTable.forName(self.spark, target_table)
            key_condition = " AND ".join(
                [f"target.{k} = source.{k}" for k in all_keys]
            )
            merge_condition = key_condition
            self.logger.info(
                "CDC MERGE on %s | keys=%s", target_table, merge_condition
            )
            update_cols = AuditColumns.merge_update_columns(list(incr_df.columns))
            insert_cols = AuditColumns.merge_insert_values(list(incr_df.columns))
            (
                delta_target.alias("target")
                .merge(incr_df.alias("source"), merge_condition)
                .whenMatchedUpdate(
                    condition=(
                        f"source.{CDC_STREAM_COLUMN} IN ('update', 'insert') "
                        f"AND target.{TARGET_ROW_HASH_COLUMN} "
                        f"<> source.{TARGET_ROW_HASH_COLUMN}"
                    ),
                    set={c: f"source.{c}" for c in update_cols},
                )
                .whenMatchedDelete(
                    condition=f"source.{CDC_STREAM_COLUMN} = 'delete'"
                )
                .whenNotMatchedInsert(
                    values={c: f"source.{c}" for c in insert_cols}
                )
                .execute()
            )
            self.spark.sql(
                f"ALTER TABLE {target_table} "
                f"SET TBLPROPERTIES ('data.load_type' = "
                f"'{self.config.master_specs['load_type']}')"
            )
            change_count = incr_df.count()
            if self.spark.table(target_table).count() < 1:
                raise DataLoadException(
                    message=f"CDC merge left target empty: {target_table}",
                    original_exception=None,
                )
            self.logger.info(
                "Incremental CDC completed for %s (%s changes).",
                target_table,
                change_count,
            )
            return LoadResult(
                feed_id=self.config.master_specs["feed_id"],
                success=True,
                skipped=False,
                total_rows_inserted=change_count,
                total_rows_deleted=0,
                total_rows_updated=0,
            )
        except DataLoadException:
            raise
        except Exception as e:
            raise DataLoadException(
                original_exception=e,
                error_code="HF042",
                message=(
                    f"Error during Incremental CDC load for "
                    f"{self._current_target_table_name}: {e}"
                ),
            )
