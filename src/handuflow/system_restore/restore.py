"""
System Restore — Delta Lake version-based global restore points for Unity Catalog tables.

Metadata tables (under ``[DEFAULT] system_schema``):

* ``SYSTEM_RESTORE_POINTS`` — one row per table per restore point
* ``SYSTEM_RESTORE_AUDIT`` — restore request audit trail

Public API::

    from handuflow.system_restore.restore import (
        create_restore_point,
        list_restore_points,
        get_restore_point_details,
        initiate_restore,
    )
"""

from __future__ import annotations

import re
import uuid
import logging
import configparser
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

import pandas as pd
import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from handuflow.config.catalog_resolver import CatalogResolver
from handuflow.config.config_paths import global_vacuum_hours, runtime_mode, system_schema
from handuflow.config.run_logger import log_step
from handuflow.exception.system_error import SystemError
from handuflow.system_shared.delta_utils import (
    append_delta_table,
    is_delta_table,
    overwrite_delta_table,
    quote_table,
)
from handuflow.system_shared.spec_tables import (
    collect_restore_point_table_entries,
    expected_restore_table_set,
    load_master_specs_from_config,
    master_specs_to_dataframe,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

RESTORE_POINTS_TABLE = "SYSTEM_RESTORE_POINTS"
RESTORE_AUDIT_TABLE = "SYSTEM_RESTORE_AUDIT"
RESTORE_POINT_PREFIX = "HFRP"
RESTORE_POINT_PATTERN = re.compile(r"^HFRP(\d{4})$")

STATUS_REQUESTED = "REQUESTED"
STATUS_IN_PROGRESS = "IN_PROGRESS"
STATUS_COMPLETED = "COMPLETED"
STATUS_FAILED = "FAILED"


def _utc_now() -> datetime:
    """Timezone-naive UTC timestamp for Spark/Delta metadata columns."""
    return datetime.now(UTC).replace(tzinfo=None)


class SystemRestore:
    """Create, list, and execute global Delta restore points."""

    def __init__(
        self,
        spark: SparkSession,
        config: configparser.ConfigParser,
        master_specs: pd.DataFrame | list[dict],
        *,
        catalog_hint: str = "",
    ) -> None:
        self.spark = spark
        self.config = config
        self.master_specs = master_specs_to_dataframe(master_specs)
        self.catalog_hint = catalog_hint.strip()
        self.schema_name = system_schema(config)
        if not self.schema_name:
            raise SystemError(
                message="[DEFAULT] system_schema is required for System Restore",
                error_code="HF097",
            )
        self.vacuum_hours = global_vacuum_hours(config)
        self.restore_points_table = self._qualify_system_table(RESTORE_POINTS_TABLE)
        self.restore_audit_table = self._qualify_system_table(RESTORE_AUDIT_TABLE)
        self.logger = logger
        self._ensure_metadata_tables()

    def create_restore_point(self, created_by: str) -> str:
        """Capture Delta versions for target and staging tables."""
        return self._capture_restore_point(created_by, allow_reuse=True)

    def _capture_restore_point(self, created_by: str, *, allow_reuse: bool) -> str:
        """Persist table versions as a restore point; optionally reuse when unchanged."""
        entries = self._delta_table_entries_for_snapshot()
        if not entries:
            raise SystemError(
                message="No Delta tables available to include in restore point",
                error_code="HF097",
            )
        self._validate_complete_table_set({e["table_name"] for e in entries})

        current_versions = {
            e["table_name"]: int(e["delta_version"]) for e in entries
        }
        if allow_reuse:
            existing_id = self._find_reusable_restore_point(current_versions)
            if existing_id is not None:
                self._mark_restore_point_as_latest(existing_id)
                log_step(
                    self.logger,
                    "system_restore.create",
                    status="SKIP",
                    restore_point_id=existing_id,
                    reason="delta_versions_unchanged",
                    table_count=len(entries),
                )
                self.logger.info(
                    "Restore point unchanged; reusing %s "
                    "(target/staging delta versions match current state).",
                    existing_id,
                )
                return existing_id

        restore_point_id = self._next_restore_point_id()
        now = _utc_now()
        self._clear_latest_restore_point_flags()
        rows = [
            {
                "restore_point_id": restore_point_id,
                "table_name": e["table_name"],
                "table_type": e["table_type"],
                "delta_version": int(e["delta_version"]),
                "created_timestamp": now,
                "created_by": created_by,
                "vacuum_retention_hours": self.vacuum_hours,
                "is_latest": True,
            }
            for e in entries
        ]
        self._append_rows(self.restore_points_table, rows, _restore_points_schema())

        log_step(
            self.logger,
            "system_restore.create",
            status="OK",
            restore_point_id=restore_point_id,
            table_count=len(rows),
            vacuum_retention_hours=self.vacuum_hours,
            created_by=created_by,
        )
        for row in rows:
            self.logger.info(
                "Restore point version captured | restore_point_id=%s | table=%s | "
                "table_type=%s | delta_version=%s | is_latest=%s",
                restore_point_id,
                row["table_name"],
                row["table_type"],
                row["delta_version"],
                row["is_latest"],
            )
        return restore_point_id

    def list_restore_points(self) -> list[str]:
        """Return restore point IDs that are complete and within retention."""
        if not self.spark.catalog.tableExists(self.restore_points_table):
            return []
        df = self.spark.table(self.restore_points_table)
        ids = [
            row["restore_point_id"]
            for row in df.select("restore_point_id").distinct().collect()
        ]
        valid = []
        for rp_id in sorted(ids):
            if self._is_restore_point_valid(rp_id):
                valid.append(rp_id)
        return valid

    def get_restore_point_details(self, restore_point_id: str) -> dict[str, Any]:
        """Return metadata and validation status for a restore point."""
        rows = self._load_restore_point_rows(restore_point_id)
        if not rows:
            raise SystemError(
                message=f"Restore point not found: {restore_point_id}",
                error_code="HF097",
            )
        expected = expected_restore_table_set(self.master_specs, self.config)
        tables_in_point = {r["table_name"] for r in rows}
        validation_errors = self._validate_restore_point_rows(rows, expected)
        return {
            "restore_point_id": restore_point_id,
            "table_count": len(rows),
            "tables": rows,
            "is_latest": bool(rows[0].get("is_latest")),
            "expected_table_count": len(expected),
            "includes_all_targets": expected <= tables_in_point,
            "includes_all_tables": expected <= tables_in_point,
            "within_retention": not any(
                "retention window expired" in e for e in validation_errors
            ),
            "versions_available": not any(
                "version unavailable" in e for e in validation_errors
            ),
            "is_valid": len(validation_errors) == 0,
            "validation_errors": validation_errors,
        }

    def initiate_restore(self, restore_point_id: str, requested_by: str) -> str:
        """Restore all tables to the versions captured in a restore point."""
        request_id = uuid.uuid4().hex
        now = _utc_now()
        self._insert_audit_row(
            {
                "request_id": request_id,
                "restore_point_id": restore_point_id,
                "requested_by": requested_by,
                "request_timestamp": now,
                "status": STATUS_REQUESTED,
                "start_timestamp": None,
                "end_timestamp": None,
                "error_details": None,
            }
        )
        self.logger.info(
            "Restore requested | request_id=%s | restore_point_id=%s | requested_by=%s",
            request_id,
            restore_point_id,
            requested_by,
        )

        rows = self._load_restore_point_rows(restore_point_id)
        expected = expected_restore_table_set(self.master_specs, self.config)
        validation_errors = self._validate_restore_point_rows(rows, expected)
        if validation_errors:
            msg = "; ".join(validation_errors)
            self._update_audit(
                request_id,
                status=STATUS_FAILED,
                end_timestamp=_utc_now(),
                error_details=msg,
            )
            raise SystemError(
                message=f"Restore point validation failed: {msg}",
                error_code="HF097",
            )

        pre_restore_id = self._capture_restore_point(
            requested_by,
            allow_reuse=False,
        )
        self.logger.info(
            "Pre-restore snapshot captured | request_id=%s | restore_point_id=%s",
            request_id,
            pre_restore_id,
        )

        start = _utc_now()
        self._update_audit(
            request_id,
            status=STATUS_IN_PROGRESS,
            start_timestamp=start,
        )

        try:
            for row in sorted(rows, key=lambda r: r["table_name"]):
                self._restore_table_version(
                    row["table_name"], int(row["delta_version"])
                )
        except Exception as exc:
            self._update_audit(
                request_id,
                status=STATUS_FAILED,
                end_timestamp=_utc_now(),
                error_details=str(exc),
            )
            self.logger.error(
                "Restore failed | request_id=%s | restore_point_id=%s | error=%s",
                request_id,
                restore_point_id,
                exc,
                exc_info=True,
            )
            raise SystemError(
                message=f"Restore failed for {restore_point_id}: {exc}",
                error_code="HF097",
                original_exception=exc,
            ) from exc

        self._update_audit(
            request_id,
            status=STATUS_COMPLETED,
            end_timestamp=_utc_now(),
        )
        log_step(
            self.logger,
            "system_restore.complete",
            status="OK",
            request_id=request_id,
            restore_point_id=restore_point_id,
            pre_restore_point_id=pre_restore_id,
            table_count=len(rows),
        )
        return request_id

    def get_latest_restore_point_id(self) -> str | None:
        """Return the restore point ID marked ``is_latest``, if any."""
        if not self.spark.catalog.tableExists(self.restore_points_table):
            return None
        if "is_latest" not in {
            f.name for f in self.spark.table(self.restore_points_table).schema.fields
        }:
            ids = self._list_restore_point_ids_newest_first()
            return ids[0] if ids else None
        latest = (
            self.spark.table(self.restore_points_table)
            .filter(F.col("is_latest") == True)  # noqa: E712
            .select("restore_point_id")
            .distinct()
            .collect()
        )
        if not latest:
            return None
        if len(latest) > 1:
            self.logger.warning(
                "Multiple restore points marked is_latest; using newest sequence."
            )
            return sorted(
                [row["restore_point_id"] for row in latest],
                key=self._restore_point_sequence,
                reverse=True,
            )[0]
        return latest[0]["restore_point_id"]

    def _mark_restore_point_as_latest(self, restore_point_id: str) -> None:
        self._clear_latest_restore_point_flags()
        quoted = quote_table(self.restore_points_table)
        self.spark.sql(
            f"UPDATE {quoted} SET is_latest = true "
            f"WHERE restore_point_id = '{restore_point_id}'"
        )

    def _clear_latest_restore_point_flags(self) -> None:
        if not self.spark.catalog.tableExists(self.restore_points_table):
            return
        if "is_latest" not in {
            f.name for f in self.spark.table(self.restore_points_table).schema.fields
        }:
            return
        quoted = quote_table(self.restore_points_table)
        self.spark.sql(f"UPDATE {quoted} SET is_latest = false")

    def _restore_table_version(self, table_name: str, version: int) -> None:
        quoted = quote_table(table_name)
        self.logger.info(
            "Restoring table | table=%s | delta_version=%s",
            table_name,
            version,
        )
        self.spark.sql(f"RESTORE TABLE {quoted} TO VERSION AS OF {version}")
        self.logger.info(
            "Restore table complete | table=%s | delta_version=%s | status=OK",
            table_name,
            version,
        )

    def _delta_table_entries_for_snapshot(self) -> list[dict[str, Any]]:
        entries: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        for table_name, table_type in collect_restore_point_table_entries(
            self.master_specs, self.config
        ):
            key = (table_name, table_type)
            if key in seen:
                continue
            seen.add(key)
            if not self.spark.catalog.tableExists(table_name):
                self.logger.warning(
                    "Restore point skip | table=%s | reason=table_not_found",
                    table_name,
                )
                continue
            if not is_delta_table(self.spark, table_name):
                self.logger.warning(
                    "Restore point skip | table=%s | reason=not_delta_table",
                    table_name,
                )
                continue
            version = self._current_delta_version(table_name)
            if version is None:
                continue
            entries.append(
                {
                    "table_name": table_name,
                    "table_type": table_type,
                    "delta_version": version,
                }
            )
        return entries

    def _current_delta_version(self, table_name: str) -> int | None:
        history = (
            DeltaTable.forName(self.spark, table_name)
            .history(1)
            .select("version")
            .collect()
        )
        if not history:
            return None
        return int(history[0]["version"])

    def _restore_point_sequence(self, restore_point_id: str) -> int:
        match = RESTORE_POINT_PATTERN.match(str(restore_point_id))
        return int(match.group(1)) if match else 0

    def _list_restore_point_ids_newest_first(self) -> list[str]:
        if not self.spark.catalog.tableExists(self.restore_points_table):
            return []
        ids = [
            row["restore_point_id"]
            for row in self.spark.table(self.restore_points_table)
            .select("restore_point_id")
            .distinct()
            .collect()
        ]
        return sorted(ids, key=self._restore_point_sequence, reverse=True)

    def _find_reusable_restore_point(
        self, current_versions: dict[str, int]
    ) -> str | None:
        """
        Return the newest restore point whose table versions match ``current_versions``.

        Skips creating duplicate restore points when loads were skipped and Delta
        table versions did not change.
        """
        for restore_point_id in self._list_restore_point_ids_newest_first():
            rows = self._load_restore_point_rows(restore_point_id)
            if not rows:
                continue
            point_versions = {
                row["table_name"]: int(row["delta_version"]) for row in rows
            }
            if point_versions != current_versions:
                continue
            if not self._is_restore_point_valid(restore_point_id):
                continue
            return restore_point_id
        return None

    def _is_restore_point_valid(self, restore_point_id: str) -> bool:
        try:
            details = self.get_restore_point_details(restore_point_id)
            return bool(details.get("is_valid"))
        except SystemError:
            return False

    def _validate_restore_point_rows(
        self,
        rows: list[dict[str, Any]],
        expected_tables: set[str],
    ) -> list[str]:
        errors: list[str] = []
        if not rows:
            return ["restore point has no table records"]

        tables_in_point = {r["table_name"] for r in rows}
        missing = expected_tables - tables_in_point
        if missing:
            errors.append(
                f"restore point missing target tables: {sorted(missing)}"
            )

        created = rows[0].get("created_timestamp")
        retention_hours = int(
            rows[0].get("vacuum_retention_hours") or self.vacuum_hours
        )
        if created and self._retention_expired(created, retention_hours):
            errors.append(
                f"restore point retention window expired ({retention_hours} hours)"
            )

        for row in rows:
            table_name = row["table_name"]
            version = int(row["delta_version"])
            if not self.spark.catalog.tableExists(table_name):
                errors.append(f"version unavailable: table {table_name} no longer exists")
                continue
            if not self._delta_version_available(table_name, version):
                errors.append(
                    f"version unavailable: {table_name} version {version} "
                    f"not in history (possibly vacuumed)"
                )
        return errors

    def _delta_version_available(self, table_name: str, version: int) -> bool:
        if not is_delta_table(self.spark, table_name):
            return False
        versions = {
            int(r["version"])
            for r in DeltaTable.forName(self.spark, table_name)
            .history()
            .select("version")
            .collect()
        }
        return version in versions

    def _retention_expired(self, created: datetime, retention_hours: int) -> bool:
        if isinstance(created, str):
            created = datetime.fromisoformat(created.replace("Z", ""))
        cutoff = _utc_now() - timedelta(hours=retention_hours)
        return created < cutoff

    def _validate_complete_table_set(self, captured: set[str]) -> None:
        expected = expected_restore_table_set(self.master_specs, self.config)
        missing = expected - captured
        if missing:
            raise SystemError(
                message=(
                    "Restore point incomplete; missing target Delta tables: "
                    f"{sorted(missing)}"
                ),
                error_code="HF097",
            )

    def _load_restore_point_rows(self, restore_point_id: str) -> list[dict[str, Any]]:
        if not self.spark.catalog.tableExists(self.restore_points_table):
            return []
        df = self.spark.table(self.restore_points_table).filter(
            F.col("restore_point_id") == restore_point_id
        )
        return [row.asDict() for row in df.collect()]

    def _next_restore_point_id(self) -> str:
        if not self.spark.catalog.tableExists(self.restore_points_table):
            return f"{RESTORE_POINT_PREFIX}0001"
        ids = [
            row["restore_point_id"]
            for row in self.spark.table(self.restore_points_table)
            .select("restore_point_id")
            .distinct()
            .collect()
        ]
        max_num = 0
        for rp_id in ids:
            match = RESTORE_POINT_PATTERN.match(str(rp_id))
            if match:
                max_num = max(max_num, int(match.group(1)))
        return f"{RESTORE_POINT_PREFIX}{max_num + 1:04d}"

    def _ensure_metadata_tables(self) -> None:
        self._ensure_schema_exists()
        self._ensure_table(
            self.restore_points_table,
            _restore_points_schema(),
        )
        self._ensure_restore_points_columns()
        self._ensure_table(
            self.restore_audit_table,
            _restore_audit_schema(),
        )

    def _ensure_restore_points_columns(self) -> None:
        if not self.spark.catalog.tableExists(self.restore_points_table):
            return
        columns = {
            f.name for f in self.spark.table(self.restore_points_table).schema.fields
        }
        if "is_latest" in columns:
            return
        quoted = quote_table(self.restore_points_table)
        self.spark.sql(f"ALTER TABLE {quoted} ADD COLUMNS (is_latest BOOLEAN)")
        self.logger.info(
            "Added is_latest column to %s; backfilling latest restore point.",
            self.restore_points_table,
        )
        latest_id = self._list_restore_point_ids_newest_first()
        if latest_id:
            self._mark_restore_point_as_latest(latest_id[0])

    def _ensure_schema_exists(self) -> None:
        parts = [p.strip("`") for p in self.schema_name.split(".") if p.strip("`")]
        if len(parts) == 2:
            catalog, schema = parts
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
        else:
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS `{parts[0]}`")

    def _ensure_table(self, qualified_name: str, schema: StructType) -> None:
        if self.spark.catalog.tableExists(qualified_name):
            if is_delta_table(self.spark, qualified_name):
                return
            self.logger.warning(
                "Replacing non-Delta system metadata table: %s", qualified_name
            )
        empty = self.spark.createDataFrame([], schema)
        overwrite_delta_table(self.spark, qualified_name, empty)
        self.logger.info("Created system metadata table: %s", qualified_name)

    def _append_rows(
        self,
        qualified_name: str,
        rows: list[dict[str, Any]],
        schema: StructType,
    ) -> None:
        df = self.spark.createDataFrame(rows, schema=schema)
        append_delta_table(self.spark, qualified_name, df)

    def _insert_audit_row(self, row: dict[str, Any]) -> None:
        self._append_rows(self.restore_audit_table, [row], _restore_audit_schema())

    def _update_audit(
        self,
        request_id: str,
        *,
        status: str | None = None,
        start_timestamp: datetime | None = None,
        end_timestamp: datetime | None = None,
        error_details: str | None = None,
    ) -> None:
        if not self.spark.catalog.tableExists(self.restore_audit_table):
            return
        sets = []
        if status is not None:
            sets.append(f"status = '{status}'")
        if start_timestamp is not None:
            sets.append(f"start_timestamp = timestamp('{start_timestamp.isoformat()}')")
        if end_timestamp is not None:
            sets.append(f"end_timestamp = timestamp('{end_timestamp.isoformat()}')")
        if error_details is not None:
            escaped = error_details.replace("'", "''")[:8000]
            sets.append(f"error_details = '{escaped}'")
        if not sets:
            return
        quoted_audit = quote_table(self.restore_audit_table)
        self.spark.sql(
            f"UPDATE {quoted_audit} SET {', '.join(sets)} "
            f"WHERE request_id = '{request_id}'"
        )
        self.logger.info(
            "Restore audit updated | request_id=%s | status=%s",
            request_id,
            status,
        )

    def _qualify_system_table(self, table_short_name: str) -> str:
        if "." in self.schema_name:
            return f"{self.schema_name}.{table_short_name}"
        if runtime_mode(self.config) == "unity_catalog" and self.catalog_hint:
            resolver = CatalogResolver(self.catalog_hint, config=self.config)
            return resolver.qualified_table(self.schema_name, table_short_name)
        return f"{self.schema_name}.{table_short_name}"

def _restore_points_schema() -> StructType:
    return StructType(
        [
            StructField("restore_point_id", StringType(), False),
            StructField("table_name", StringType(), False),
            StructField("table_type", StringType(), False),
            StructField("delta_version", LongType(), False),
            StructField("created_timestamp", TimestampType(), False),
            StructField("created_by", StringType(), False),
            StructField("vacuum_retention_hours", IntegerType(), False),
            StructField("is_latest", BooleanType(), False),
        ]
    )


def _restore_audit_schema() -> StructType:
    return StructType(
        [
            StructField("request_id", StringType(), False),
            StructField("restore_point_id", StringType(), False),
            StructField("requested_by", StringType(), False),
            StructField("request_timestamp", TimestampType(), False),
            StructField("status", StringType(), False),
            StructField("start_timestamp", TimestampType(), True),
            StructField("end_timestamp", TimestampType(), True),
            StructField("error_details", StringType(), True),
        ]
    )


def _service(
    spark: SparkSession,
    config: configparser.ConfigParser,
    catalog_hint: str = "",
) -> SystemRestore:
    resolved = load_master_specs_from_config(config)
    hint = catalog_hint or _default_catalog_hint(resolved)
    return SystemRestore(spark, config, resolved, catalog_hint=hint)


def _default_catalog_hint(master_specs: pd.DataFrame | list[dict]) -> str:
    df = master_specs_to_dataframe(master_specs)
    if df.empty:
        return ""
    for row in df.to_dict(orient="records"):
        catalog = str(row.get("target_unity_catalog", "") or "").strip()
        if catalog and catalog.lower() not in CatalogResolver.LOCAL_CATALOG_ALIASES:
            return catalog
    return ""


def create_restore_point(
    spark: SparkSession,
    config: configparser.ConfigParser,
    created_by: str,
    *,
    catalog_hint: str = "",
) -> str:
    """
    Create a new global restore point (HFRP####).

    Master specs are always loaded from ``config.ini`` (``file_hunt_path`` +
    ``[FILES] master_spec_name``).
    """
    return _service(spark, config, catalog_hint).create_restore_point(created_by)


def list_restore_points(
    spark: SparkSession,
    config: configparser.ConfigParser,
    *,
    catalog_hint: str = "",
) -> list[str]:
    """List valid restore point IDs (master specs from ``config.ini``)."""
    return _service(spark, config, catalog_hint).list_restore_points()


def get_restore_point_details(
    spark: SparkSession,
    config: configparser.ConfigParser,
    restore_point_id: str,
    *,
    catalog_hint: str = "",
) -> dict[str, Any]:
    """Return restore point metadata and validation status."""
    return _service(spark, config, catalog_hint).get_restore_point_details(
        restore_point_id
    )


def get_latest_restore_point_id(
    spark: SparkSession,
    config: configparser.ConfigParser,
    *,
    catalog_hint: str = "",
) -> str | None:
    """Return the restore point ID marked ``is_latest`` in ``SYSTEM_RESTORE_POINTS``."""
    return _service(spark, config, catalog_hint).get_latest_restore_point_id()


def initiate_restore(
    spark: SparkSession,
    config: configparser.ConfigParser,
    restore_point_id: str,
    requested_by: str,
    *,
    catalog_hint: str = "",
) -> str:
    """Execute a full system restore; returns audit ``request_id``."""
    return _service(spark, config, catalog_hint).initiate_restore(
        restore_point_id, requested_by
    )
