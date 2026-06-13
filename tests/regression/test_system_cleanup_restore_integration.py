"""
Integration tests for GLOBAL_VACUUM_HOURS cleanup and System Restore.

Run: pytest tests/regression/test_system_cleanup_restore_integration.py -m integration -v
"""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime, timedelta

import pandas as pd
import pytest

from handuflow.config.config_paths import cfg_get

from handuflow.system_cleanup.cleanup import SystemCleanup
from handuflow.system_restore.restore import (
    create_restore_point,
    get_restore_point_details,
    initiate_restore,
    list_restore_points,
)

pytestmark = pytest.mark.integration

SELECTION_SCHEMA = {
    "type": "struct",
    "fields": [
        {"name": "id", "type": "long", "nullable": False, "metadata": {}},
        {"name": "value", "type": "string", "nullable": True, "metadata": {}},
        {
            "name": "_x_last_modification_timestamp",
            "type": "timestamp",
            "nullable": True,
            "metadata": {},
        },
    ],
}


def _feed_specs(source_table: str, target_schema: str) -> dict:
    return {
        "primary_key": "id",
        "composite_key": [],
        "partition_keys": [],
        "vacuum_hours": 168,
        "source_table_name": source_table,
        "selection_query": None,
        "selection_schema": SELECTION_SCHEMA,
        "standard_checks": [],
        "comprehensive_checks": [],
    }


def _master_spec_row(
    feed_id: int,
    source_table: str,
    target_schema: str,
    target_table: str,
) -> dict:
    return {
        "feed_id": feed_id,
        "target_unity_catalog": "local",
        "target_schema_name": target_schema,
        "target_table_name": target_table,
        "load_type": "FULL_LOAD",
        "data_flow_direction": "WITHIN_UNITY_CATALOG",
        "is_active": True,
        "feed_specs": json.dumps(_feed_specs(source_table, target_schema)),
    }


def _write_master_specs_for_config(config, df: pd.DataFrame) -> None:
    if "is_active" not in df.columns:
        df = df.copy()
        df["is_active"] = True
    file_hunt_path = cfg_get(config, "file_hunt_path")
    master_spec_name = cfg_get(
        config, "master_spec_name", "master_specs.xlsx", section="FILES"
    )
    path = os.path.join(file_hunt_path, master_spec_name)
    os.makedirs(file_hunt_path, exist_ok=True)
    df.to_excel(path, sheet_name="master_specs", index=False)


def _write_delta_table(spark, table: str, rows: list[tuple]) -> None:
    spark.createDataFrame(rows, ["id", "value", "_x_last_modification_timestamp"]).write.format(
        "delta"
    ).mode("overwrite").saveAsTable(table)


@pytest.fixture
def retention_config(local_config):
    local_config.set("DEFAULT", "global_vacuum_hours", "168")
    local_config.set("DEFAULT", "system_schema", "system_admin")
    return local_config


@pytest.fixture
def restore_env(spark, retention_config):
    schema = "hf_retention"
    source = f"{schema}.source_tbl"
    target = f"{schema}.target_tbl"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS system_admin")
    for table in (source, target):
        spark.sql(f"DROP TABLE IF EXISTS {table}")

    now = datetime.now(UTC).replace(tzinfo=None)
    old = now - timedelta(hours=200)
    initial = [(1, "keep", now), (2, "delete_me", old)]
    _write_delta_table(spark, source, initial)
    _write_delta_table(spark, target, initial)

    master_specs = pd.DataFrame(
        [_master_spec_row(1, source, schema, "target_tbl")]
    )
    _write_master_specs_for_config(retention_config, master_specs)
    yield {
        "spark": spark,
        "config": retention_config,
        "master_specs": master_specs,
        "source": source,
        "target": target,
        "schema": schema,
    }

    for table in (source, target):
        spark.sql(f"DROP TABLE IF EXISTS {table}")
    for meta in (
        "system_admin.SYSTEM_RESTORE_POINTS",
        "system_admin.SYSTEM_RESTORE_AUDIT",
    ):
        spark.sql(f"DROP TABLE IF EXISTS {meta}")


class TestGlobalVacuumHoursCleanup:
    def test_deletes_rows_older_than_retention_window(self, restore_env):
        spark = restore_env["spark"]
        source = restore_env["source"]
        target = restore_env["target"]

        SystemCleanup(
            config=restore_env["config"],
            master_specs=restore_env["master_specs"],
            spark=spark,
        ).run()

        for table in (source, target):
            rows = spark.table(table).collect()
            assert len(rows) == 1
            assert rows[0]["value"] == "keep"

    def test_respects_different_global_vacuum_hours_value(self, spark, retention_config):
        schema = "hf_vacuum720"
        table = f"{schema}.only_target"
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema}")
        spark.sql(f"DROP TABLE IF EXISTS {table}")

        now = datetime.now(UTC).replace(tzinfo=None)
        old = now - timedelta(hours=800)
        recent = now - timedelta(hours=100)
        _write_delta_table(
            spark,
            table,
            [(1, "recent", recent), (2, "old", old)],
        )

        retention_config.set("DEFAULT", "global_vacuum_hours", "720")
        master_specs = pd.DataFrame(
            [
                _master_spec_row(
                    99,
                    table,
                    schema,
                    "only_target",
                )
            ]
        )

        SystemCleanup(
            config=retention_config,
            master_specs=master_specs,
            spark=spark,
        ).run()

        values = {r["value"] for r in spark.table(table).collect()}
        assert values == {"recent"}

        spark.sql(f"DROP TABLE IF EXISTS {table}")


LOAD_TYPES = ["FULL_LOAD", "APPEND_LOAD", "INCREMENTAL_CDC", "SCD_TYPE_2"]
VACUUM_HOURS_MATRIX = [168, 720, 8760]
RESTORE_SCENARIOS = [
    ("single", 1),
    ("double", 2),
    ("triple", 3),
]


@pytest.fixture(params=LOAD_TYPES)
def load_type_env(spark, retention_config, request):
    """Per load-type source/target with audit timestamp column for system ops."""
    load_type = request.param
    schema = f"hf_sys_{load_type.lower()}"
    source = f"{schema}.source_tbl"
    target = f"{schema}.target_tbl"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema}")
    spark.sql("CREATE DATABASE IF NOT EXISTS system_admin")
    for table in (source, target):
        spark.sql(f"DROP TABLE IF EXISTS {table}")

    now = datetime.now(UTC).replace(tzinfo=None)
    initial = [(1, "keep", now), (2, "row_b", now), (3, "row_c", now)]
    _write_delta_table(spark, source, initial)
    _write_delta_table(spark, target, initial)

    master_specs = pd.DataFrame(
        [
            {
                **_master_spec_row(1, source, schema, "target_tbl"),
                "load_type": load_type,
            }
        ]
    )
    _write_master_specs_for_config(retention_config, master_specs)
    yield {
        "spark": spark,
        "config": retention_config,
        "master_specs": master_specs,
        "source": source,
        "target": target,
        "load_type": load_type,
    }
    for table in (source, target):
        spark.sql(f"DROP TABLE IF EXISTS {table}")


class TestSystemRestore:
    def test_create_list_and_restore(self, restore_env):
        spark = restore_env["spark"]
        cfg = restore_env["config"]
        specs = restore_env["master_specs"]
        source = restore_env["source"]
        target = restore_env["target"]

        rp_id = create_restore_point(spark, cfg, created_by="regression")
        assert rp_id == "HFRP0001"

        details = get_restore_point_details(spark, cfg, rp_id)
        assert details["is_valid"] is True
        assert details["includes_all_tables"] is True
        assert details["table_count"] == 1

        now = datetime.now(UTC).replace(tzinfo=None)
        _write_delta_table(
            spark,
            source,
            [(1, "after_snapshot", now)],
        )
        _write_delta_table(
            spark,
            target,
            [(1, "after_snapshot", now)],
        )

        request_id = initiate_restore(spark, cfg, rp_id, requested_by="regression")
        assert request_id

        source_rows = spark.table(source).collect()
        assert len(source_rows) == 1
        assert source_rows[0]["value"] == "after_snapshot"

        target_rows = spark.table(target).collect()
        assert len(target_rows) == 2
        assert {r["value"] for r in target_rows} == {"keep", "delete_me"}

        points = list_restore_points(spark, cfg)
        assert rp_id in points

    def test_restore_includes_staging_when_present(self, restore_env):
        spark = restore_env["spark"]
        cfg = restore_env["config"]
        specs = restore_env["master_specs"]
        target = restore_env["target"]
        staging = "staging.t_full_target_tbl"

        spark.sql("CREATE DATABASE IF NOT EXISTS staging")
        spark.sql(f"DROP TABLE IF EXISTS {staging}")
        _write_delta_table(
            spark,
            staging,
            [(1, "staging_row", datetime.now(UTC).replace(tzinfo=None))],
        )

        rp_id = create_restore_point(spark, cfg, created_by="regression")
        details = get_restore_point_details(spark, cfg, rp_id)
        captured = {row["table_name"] for row in details["tables"]}
        assert target in captured
        assert staging in captured
        assert restore_env["source"] in captured

        spark.sql(f"DROP TABLE IF EXISTS {staging}")

    def test_create_restore_point_reuses_when_versions_unchanged(self, restore_env):
        spark = restore_env["spark"]
        cfg = restore_env["config"]
        target = restore_env["target"]

        rp_first = create_restore_point(spark, cfg, created_by="regression")
        rp_second = create_restore_point(spark, cfg, created_by="regression")
        assert rp_second == rp_first

        latest_rows = (
            spark.table("system_admin.SYSTEM_RESTORE_POINTS")
            .filter("is_latest = true")
            .select("restore_point_id")
            .distinct()
            .collect()
        )
        assert len(latest_rows) == 1
        assert latest_rows[0]["restore_point_id"] == rp_first

        distinct_ids = [
            row["restore_point_id"]
            for row in spark.table("system_admin.SYSTEM_RESTORE_POINTS")
            .select("restore_point_id")
            .distinct()
            .collect()
        ]
        assert distinct_ids == [rp_first]

        now = datetime.now(UTC).replace(tzinfo=None)
        _write_delta_table(
            spark,
            target,
            [(1, "changed", now), (2, "changed2", now)],
        )
        rp_after_change = create_restore_point(spark, cfg, created_by="regression")
        assert rp_after_change != rp_first

    def test_restore_validation_fails_for_unknown_point(self, restore_env):
        spark = restore_env["spark"]
        from handuflow.exception import SystemError

        count_before = (
            spark.table("system_admin.SYSTEM_RESTORE_POINTS")
            .select("restore_point_id")
            .distinct()
            .count()
        )
        with pytest.raises(SystemError):
            initiate_restore(
                spark,
                restore_env["config"],
                "HFRP9999",
                requested_by="regression",
            )
        count_after = (
            spark.table("system_admin.SYSTEM_RESTORE_POINTS")
            .select("restore_point_id")
            .distinct()
            .count()
        )
        assert count_after == count_before

    def test_initiate_restore_creates_pre_restore_snapshot(self, restore_env):
        spark = restore_env["spark"]
        cfg = restore_env["config"]
        source = restore_env["source"]
        target = restore_env["target"]

        rp_id = create_restore_point(spark, cfg, created_by="regression")
        count_before = (
            spark.table("system_admin.SYSTEM_RESTORE_POINTS")
            .select("restore_point_id")
            .distinct()
            .count()
        )

        now = datetime.now(UTC).replace(tzinfo=None)
        _write_delta_table(
            spark,
            source,
            [(1, "before_restore", now)],
        )
        _write_delta_table(
            spark,
            target,
            [(1, "before_restore", now)],
        )

        initiate_restore(spark, cfg, rp_id, requested_by="regression")

        count_after = (
            spark.table("system_admin.SYSTEM_RESTORE_POINTS")
            .select("restore_point_id")
            .distinct()
            .count()
        )
        assert count_after == count_before + 1

        latest_rows = (
            spark.table("system_admin.SYSTEM_RESTORE_POINTS")
            .filter("is_latest = true")
            .select("restore_point_id")
            .distinct()
            .collect()
        )
        assert len(latest_rows) == 1
        assert latest_rows[0]["restore_point_id"] != rp_id


@pytest.mark.parametrize("vacuum_hours", VACUUM_HOURS_MATRIX)
def test_vacuum_hours_boundary_values_do_not_remove_recent_rows(
    spark, retention_config, vacuum_hours: int
):
    schema = f"hf_vacuum_boundary_{vacuum_hours}"
    table = f"{schema}.recent_only"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema}")
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    now = datetime.now(UTC).replace(tzinfo=None)
    recent = now - timedelta(hours=min(24, vacuum_hours // 2))
    _write_delta_table(
        spark,
        table,
        [(1, "recent_a", recent), (2, "recent_b", now)],
    )
    retention_config.set("DEFAULT", "global_vacuum_hours", str(vacuum_hours))
    master_specs = pd.DataFrame(
        [_master_spec_row(50, table, schema, "recent_only")]
    )
    before = spark.table(table).count()
    SystemCleanup(
        config=retention_config,
        master_specs=master_specs,
        spark=spark,
    ).run()
    after = spark.table(table).count()
    assert after == before == 2
    spark.sql(f"DROP TABLE IF EXISTS {table}")


def test_restore_point_per_load_type(load_type_env):
    spark = load_type_env["spark"]
    cfg = load_type_env["config"]
    specs = load_type_env["master_specs"]
    source = load_type_env["source"]
    target = load_type_env["target"]

    rp_id = create_restore_point(spark, cfg, created_by="regression")
    assert rp_id.startswith("HFRP")

    now = datetime.now(UTC).replace(tzinfo=None)
    mutated = [(1, "mutated", now)]
    _write_delta_table(spark, source, mutated)
    _write_delta_table(spark, target, mutated)

    initiate_restore(spark, cfg, rp_id, requested_by="regression")
    values = {r["value"] for r in spark.table(target).collect()}
    assert values == {"keep", "row_b", "row_c"}


@pytest.fixture(params=RESTORE_SCENARIOS)
def restore_scenario(request):
    return request.param


def test_multiple_restore_points_per_load_type(load_type_env, restore_scenario):
    scenario, point_count = restore_scenario
    spark = load_type_env["spark"]
    cfg = load_type_env["config"]
    specs = load_type_env["master_specs"]
    source = load_type_env["source"]
    target = load_type_env["target"]

    restore_ids: list[str] = []
    snapshots: list[set[str]] = []

    def _snapshot() -> set[str]:
        return {r["value"] for r in spark.table(target).collect()}

    snapshots.append(_snapshot())
    for i in range(point_count):
        restore_ids.append(
            create_restore_point(spark, cfg, created_by=f"regression_{scenario}")
        )
        now = datetime.now(UTC).replace(tzinfo=None)
        _write_delta_table(
            spark,
            source,
            [(10 + i, f"mut_{i}", now)],
        )
        _write_delta_table(
            spark,
            target,
            [(10 + i, f"mut_{i}", now)],
        )
        if i < point_count - 1:
            snapshots.append(_snapshot())

    target_index = 0 if scenario == "single" else (point_count - 1 if scenario == "triple" else 1)
    target_index = min(target_index, len(restore_ids) - 1)
    initiate_restore(
        spark, cfg, restore_ids[target_index], requested_by="regression"
    )
    assert _snapshot() == snapshots[min(target_index, len(snapshots) - 1)]
