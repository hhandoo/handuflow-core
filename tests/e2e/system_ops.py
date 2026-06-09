"""Vacuum cleanup and system-restore validation for E2E QA."""

from __future__ import annotations

import configparser
from datetime import UTC, datetime, timedelta
from typing import Any

import pandas as pd

from handuflow.config.config_paths import (
    KEY_GLOBAL_VACUUM_HOURS,
    KEY_SYSTEM_SCHEMA,
    cfg_get,
)
from handuflow.system_cleanup.cleanup import SystemCleanup
from handuflow.system_restore.restore import (
    create_restore_point,
    get_restore_point_details,
    initiate_restore,
    list_restore_points,
)
from tests.e2e.data_generator import business_projection
VACUUM_HOURS_VALUES = [168, 720, 8760]

RESTORE_SCENARIOS: dict[str, tuple[int, int]] = {
    "single": (1, 0),
    "double_first": (2, 0),
    "double_last": (2, 1),
    "triple_first": (3, 0),
    "triple_middle": (3, 1),
    "triple_last": (3, 2),
}


def config_for_system_ops(
    base: configparser.ConfigParser,
    feed_id: int,
    *,
    vacuum_hours: int | None = None,
) -> configparser.ConfigParser:
    """Isolated system_schema per test; optional global_vacuum_hours override."""
    import io

    buf = io.StringIO()
    base.write(buf)
    buf.seek(0)
    cfg = configparser.ConfigParser()
    cfg.read_file(buf)
    schema = f"qaft_sys_{feed_id}"
    cfg.set("DEFAULT", KEY_SYSTEM_SCHEMA, schema)
    if vacuum_hours is not None:
        cfg.set("DEFAULT", KEY_GLOBAL_VACUUM_HOURS, str(vacuum_hours))
    elif not cfg_get(cfg, KEY_GLOBAL_VACUUM_HOURS):
        cfg.set("DEFAULT", KEY_GLOBAL_VACUUM_HOURS, "168")
    return cfg


def ensure_system_schema(spark, schema: str) -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema}")
    for suffix in ("SYSTEM_RESTORE_POINTS", "SYSTEM_RESTORE_AUDIT"):
        spark.sql(f"DROP TABLE IF EXISTS {schema}.{suffix}")


def master_specs_dataframe(master: dict) -> pd.DataFrame:
    return pd.DataFrame([master])


def _timestamp_column(spark, table: str) -> str | None:
    if not spark.catalog.tableExists(table):
        return None
    cols = {f.name for f in spark.table(table).schema.fields}
    for candidate in ("_x_last_modification_timestamp", "_x_commit_timestamp"):
        if candidate in cols:
            return candidate
    return None


def _ensure_retention_timestamp_column(spark, table: str) -> str:
    """Add audit timestamp to source tables that lack one (mirrors production targets)."""
    existing = _timestamp_column(spark, table)
    if existing is not None:
        return existing
    spark.sql(
        f"ALTER TABLE {table} ADD COLUMNS "
        "(_x_last_modification_timestamp TIMESTAMP)"
    )
    spark.sql(
        f"UPDATE {table} SET _x_last_modification_timestamp = current_timestamp()"
    )
    return "_x_last_modification_timestamp"


def inject_stale_rows(
    spark,
    table: str,
    *,
    hours_old: int,
    row_fraction: float = 0.01,
    ensure_timestamp_column: bool = False,
) -> int:
    """Mark ~1% of rows with an expired modification timestamp for vacuum tests."""
    if not spark.catalog.tableExists(table):
        return 0
    ts_col = _timestamp_column(spark, table)
    if ts_col is None and ensure_timestamp_column:
        ts_col = _ensure_retention_timestamp_column(spark, table)
    if ts_col is None:
        return 0
    old_ts = (datetime.now(UTC) - timedelta(hours=hours_old)).replace(tzinfo=None)
    ts_literal = old_ts.strftime("%Y-%m-%d %H:%M:%S")
    mod = max(1, int(1 / max(row_fraction, 0.001)))
    stale_ids = [
        int(r["id"])
        for r in spark.sql(
            f"SELECT id FROM {table} WHERE id % {mod} = 0 LIMIT 50"
        ).collect()
    ]
    if stale_ids:
        id_csv = ", ".join(str(i) for i in stale_ids)
        spark.sql(
            f"""
            UPDATE {table}
            SET {ts_col} = timestamp('{ts_literal}')
            WHERE id IN ({id_csv})
            """
        )
    return spark.sql(
        f"SELECT COUNT(*) AS c FROM {table} WHERE {ts_col} < current_timestamp() - INTERVAL 1 HOURS"
    ).collect()[0]["c"]


def save_target_snapshot(spark, target_fqn: str, snapshot_table: str) -> None:
    spark.sql(f"DROP TABLE IF EXISTS {snapshot_table}")
    business_projection(spark.table(target_fqn)).write.format("delta").mode(
        "overwrite"
    ).saveAsTable(snapshot_table)


def validate_target_matches_snapshot(spark, target_fqn: str, snapshot_table: str) -> tuple[bool, str]:
    if not spark.catalog.tableExists(snapshot_table):
        return False, f"snapshot missing: {snapshot_table}"
    tgt = business_projection(spark.table(target_fqn))
    snap = business_projection(spark.table(snapshot_table))
    tc = tgt.count()
    sc = snap.count()
    if tc != sc:
        return False, f"snapshot row_count target={tc} snapshot={sc}"
    diff = tgt.exceptAll(snap).limit(1).count()
    if diff:
        return False, "snapshot exceptAll mismatch"
    rev = snap.exceptAll(tgt).limit(1).count()
    if rev:
        return False, "snapshot reverse exceptAll mismatch"
    return True, "OK"


def run_vacuum_cleanup(
    spark,
    cfg: configparser.ConfigParser,
    master: dict,
    *,
    target_fqn: str,
    source: str,
    inject_stale: bool,
    vacuum_hours: int,
) -> dict[str, Any]:
    """Run SystemCleanup and verify recent data survives; stale rows pruned when injected."""
    ops_cfg = config_for_system_ops(cfg, master["feed_id"], vacuum_hours=vacuum_hours)
    schema = ops_cfg.get("DEFAULT", KEY_SYSTEM_SCHEMA)
    ensure_system_schema(spark, schema)

    master_df = master_specs_dataframe(master)
    before_target = spark.table(target_fqn).count()
    before_source = spark.table(source).count() if spark.catalog.tableExists(source) else 0
    stale_target = 0
    stale_source = 0

    if inject_stale:
        stale_target = inject_stale_rows(
            spark,
            target_fqn,
            hours_old=vacuum_hours + 48,
        )
        if spark.catalog.tableExists(source):
            stale_source = inject_stale_rows(
                spark,
                source,
                hours_old=vacuum_hours + 48,
                ensure_timestamp_column=True,
            )

    SystemCleanup(config=ops_cfg, master_specs=master_df, spark=spark).run()

    after_target = spark.table(target_fqn).count()
    after_source = (
        spark.table(source).count() if spark.catalog.tableExists(source) else 0
    )

    ok = True
    notes: list[str] = []
    if inject_stale:
        if stale_target > 0 and after_target >= before_target:
            ok = False
            notes.append("expected stale row deletion on target")
        if stale_source > 0 and after_source >= before_source:
            ok = False
            notes.append("expected stale row deletion on source")
        if stale_target == 0 and stale_source == 0:
            notes.append("no retention timestamp column available for stale injection")
    else:
        if after_target != before_target:
            ok = False
            notes.append(f"target count changed {before_target}->{after_target}")
        if before_source and after_source != before_source:
            ok = False
            notes.append(f"source count changed {before_source}->{after_source}")

    return {
        "passed": ok,
        "vacuum_hours": vacuum_hours,
        "inject_stale": inject_stale,
        "stale_rows_marked": stale_target + stale_source,
        "stale_rows_marked_target": stale_target,
        "stale_rows_marked_source": stale_source,
        "target_before": before_target,
        "target_after": after_target,
        "source_before": before_source,
        "source_after": after_source,
        "notes": "; ".join(notes) if notes else "vacuum OK",
    }


def run_restore_cycle(
    spark,
    cfg: configparser.ConfigParser,
    master: dict,
    *,
    target_fqn: str,
    restore_point_count: int,
    restore_target_index: int,
    mutate_fn,
) -> dict[str, Any]:
    """
    Create N restore points with mutations between each; restore to target index.

    ``mutate_fn`` is called after each snapshot (except before first RP) to change data.
    """
    if restore_point_count < 1:
        return {"passed": True, "notes": "no restore requested"}

    ops_cfg = config_for_system_ops(cfg, master["feed_id"])
    schema = ops_cfg.get("DEFAULT", KEY_SYSTEM_SCHEMA)
    ensure_system_schema(spark, schema)
    master_df = master_specs_dataframe(master)
    feed_id = master["feed_id"]

    snapshots: list[str] = []
    restore_ids: list[str] = []

    snap0 = f"_qaft_rp_snap_{feed_id}_0"
    save_target_snapshot(spark, target_fqn, snap0)
    snapshots.append(snap0)

    for i in range(restore_point_count):
        rp_id = create_restore_point(
            spark, ops_cfg, master_df, created_by=f"qaft_e2e_{feed_id}"
        )
        restore_ids.append(rp_id)
        details = get_restore_point_details(spark, ops_cfg, master_df, rp_id)
        if not details.get("is_valid"):
            return {
                "passed": False,
                "notes": f"restore point {rp_id} invalid",
                "restore_ids": restore_ids,
            }
        if i < restore_point_count - 1:
            mutate_fn()
            snap = f"_qaft_rp_snap_{feed_id}_{i + 1}"
            save_target_snapshot(spark, target_fqn, snap)
            snapshots.append(snap)

    mutate_fn()

    target_rp = restore_ids[restore_target_index]
    target_snap = snapshots[restore_target_index]
    request_id = initiate_restore(
        spark,
        ops_cfg,
        master_df,
        target_rp,
        requested_by=f"qaft_e2e_{feed_id}",
    )
    if not request_id:
        return {
            "passed": False,
            "notes": "initiate_restore returned empty request_id",
            "restore_ids": restore_ids,
        }

    ok, msg = validate_target_matches_snapshot(spark, target_fqn, target_snap)
    listed = list_restore_points(spark, ops_cfg, master_df)

    for snap in snapshots:
        spark.sql(f"DROP TABLE IF EXISTS {snap}")

    return {
        "passed": ok,
        "notes": msg,
        "restore_ids": restore_ids,
        "restored_to": target_rp,
        "request_id": request_id,
        "listed_points": len(listed),
    }


def drop_system_ops_artifacts(spark, feed_id: int) -> None:
    schema = f"qaft_sys_{feed_id}"
    for suffix in ("SYSTEM_RESTORE_POINTS", "SYSTEM_RESTORE_AUDIT"):
        spark.sql(f"DROP TABLE IF EXISTS {schema}.{suffix}")
    for i in range(8):
        spark.sql(f"DROP TABLE IF EXISTS _qaft_rp_snap_{feed_id}_{i}")
