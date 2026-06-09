# inbuilt
from __future__ import annotations

import os
import logging
import configparser
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

# external
import pandas as pd
from handuflow.config.config_paths import cfg_get, cfg_get_int, global_vacuum_hours
from handuflow.config.run_logger import log_step
from handuflow.system_shared.delta_utils import is_delta_table, quote_table
from handuflow.system_shared.spec_tables import collect_master_spec_table_entries

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

# Prefer modification time, then CDC commit timestamp for row-level retention.
_RETENTION_TIMESTAMP_COLUMNS = (
    "_x_last_modification_timestamp",
    "_x_commit_timestamp",
)


class SystemCleanup:
    def __init__(
        self,
        config: configparser.ConfigParser,
        master_specs: pd.DataFrame,
        spark: SparkSession | None = None,
    ) -> None:
        self.config = config
        self.master_specs = master_specs
        self.spark = spark
        self.global_vacuum_hours = global_vacuum_hours(config)
        self.retention_days = cfg_get_int(
            config,
            "retention_policy_in_days",
            cfg_get_int(config, "log_retention_policy_in_days", 7),
        )
        log_dir_name = cfg_get(config, "log_directory_name", "handuflow_logs")
        file_hunt = cfg_get(config, "file_hunt_path")
        self.final_log_dir = os.path.join(file_hunt, log_dir_name)
        temp = cfg_get(config, "temp_location") or cfg_get(config, "temp_log_location")
        temp = temp.replace("/dbfs", "")
        self.temp_log_dir = os.path.join(temp, log_dir_name)
        self.outbound_dir = os.path.join(
            file_hunt,
            cfg_get(config, "outbound_directory_name"),
        )
        self.logger = logging.getLogger(__name__)

    def run(self):
        log_step(
            self.logger,
            "system_cleanup",
            status="START",
            retention_days=self.retention_days,
            global_vacuum_hours=self.global_vacuum_hours,
        )
        removed_logs = self.__remove_old_logs()
        removed_outputs = self.__remove_old_outputs()
        delta_stats = self.__enforce_delta_retention_for_all_tables()
        log_step(
            self.logger,
            "system_cleanup",
            status="OK",
            removed_logs=removed_logs,
            removed_outputs=removed_outputs,
            **delta_stats,
        )

    def __enforce_delta_retention_for_all_tables(self) -> dict:
        if self.spark is None:
            self.logger.warning(
                "Skipping Delta retention: Spark session not provided to SystemCleanup."
            )
            return {
                "delta_tables_processed": 0,
                "delta_tables_skipped": 0,
                "delta_rows_deleted": 0,
            }

        tables = self._collect_master_spec_tables()
        processed = 0
        skipped = 0
        total_deleted = 0

        self.logger.info(
            "Delta retention starting | global_vacuum_hours=%s | table_count=%s",
            self.global_vacuum_hours,
            len(tables),
        )

        for table_name in sorted(tables):
            try:
                result = self._apply_delta_retention(table_name)
                if result is None:
                    skipped += 1
                    continue
                processed += 1
                total_deleted += result
            except Exception as exc:
                skipped += 1
                self.logger.error(
                    "Delta retention failed | table=%s | retention_hours=%s | error=%s",
                    table_name,
                    self.global_vacuum_hours,
                    exc,
                    exc_info=True,
                )

        return {
            "delta_tables_processed": processed,
            "delta_tables_skipped": skipped,
            "delta_rows_deleted": total_deleted,
        }

    def _collect_master_spec_tables(self) -> set[str]:
        if self.master_specs is None or self.master_specs.empty:
            return set()
        return {
            name
            for name, _ in collect_master_spec_table_entries(
                self.master_specs, self.config
            )
        }

    def _apply_delta_retention(self, table_name: str) -> int | None:
        hours = self.global_vacuum_hours
        quoted = quote_table(table_name)

        if not self.spark.catalog.tableExists(table_name):
            self.logger.info(
                "Delta retention skip | table=%s | reason=table_not_found",
                table_name,
            )
            return None

        if not is_delta_table(self.spark, table_name):
            self.logger.info(
                "Delta retention skip | table=%s | reason=not_delta_table",
                table_name,
            )
            return None

        deleted = 0
        ts_col = self._resolve_timestamp_column(table_name)
        if ts_col:
            deleted = self._delete_expired_rows(quoted, ts_col, hours)
            self.logger.info(
                "Delta DELETE complete | table=%s | retention_hours=%s | "
                "timestamp_column=%s | rows_deleted=%s | status=OK",
                table_name,
                hours,
                ts_col,
                deleted,
            )
        else:
            self.logger.warning(
                "Delta DELETE skip | table=%s | retention_hours=%s | "
                "reason=no_timestamp_column | candidates=%s",
                table_name,
                hours,
                list(_RETENTION_TIMESTAMP_COLUMNS),
            )

        self.spark.sql(f"OPTIMIZE {quoted}")
        self.logger.info(
            "Delta OPTIMIZE complete | table=%s | retention_hours=%s | status=OK",
            table_name,
            hours,
        )

        self.spark.sql(f"VACUUM {quoted} RETAIN {hours} HOURS")
        self.logger.info(
            "Delta VACUUM complete | table=%s | retention_hours=%s | status=OK",
            table_name,
            hours,
        )
        return deleted

    def _delete_expired_rows(
        self, quoted_table: str, timestamp_column: str, hours: int
    ) -> int:
        quoted_col = self._quote_identifier(timestamp_column)
        cutoff_predicate = (
            f"{quoted_col} < current_timestamp() - INTERVAL {hours} HOURS"
        )
        count_sql = (
            f"SELECT COUNT(*) AS cnt FROM {quoted_table} WHERE {cutoff_predicate}"
        )
        deleted = int(self.spark.sql(count_sql).collect()[0]["cnt"])
        if deleted > 0:
            delete_sql = f"DELETE FROM {quoted_table} WHERE {cutoff_predicate}"
            self.spark.sql(delete_sql)
        return deleted

    def _resolve_timestamp_column(self, table_name: str) -> str | None:
        columns = {f.name for f in self.spark.table(table_name).schema.fields}
        for candidate in _RETENTION_TIMESTAMP_COLUMNS:
            if candidate in columns:
                return candidate
        return None

    @staticmethod
    def _quote_identifier(name: str) -> str:
        return f"`{name.replace('`', '``')}`"

    def __remove_old_logs(self) -> int:
        cutoff = datetime.now() - timedelta(days=self.retention_days)
        removed = 0
        for directory in (self.final_log_dir, self.temp_log_dir):
            if not os.path.isdir(directory):
                self.logger.info("Cleanup skip (not a directory): %s", directory)
                continue
            removed += self.__remove_old_files_in_dir(directory, cutoff)
        return removed

    def __remove_old_files_in_dir(self, directory: str, cutoff: datetime) -> int:
        removed = 0
        for f in os.listdir(directory):
            path = os.path.join(directory, f)
            if os.path.isfile(path):
                if datetime.fromtimestamp(os.path.getmtime(path)) < cutoff:
                    os.remove(path)
                    removed += 1
                    self.logger.info("Removed old file: %s", path)
        return removed

    def __remove_old_outputs(self) -> int:
        if not os.path.isdir(self.outbound_dir):
            self.logger.info(
                "Cleanup skip outbound (not a directory): %s", self.outbound_dir
            )
            return 0
        cutoff = datetime.now() - timedelta(days=self.retention_days)
        removed = 0
        for f in os.listdir(self.outbound_dir):
            path = os.path.join(self.outbound_dir, f)
            if os.path.isfile(path):
                if datetime.fromtimestamp(os.path.getmtime(path)) < cutoff:
                    os.remove(path)
                    removed += 1
                    self.logger.info("Removed old output file: %s", path)
        return removed
