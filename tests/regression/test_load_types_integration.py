"""
Regression tests for all HanduFlow load types.

Requires Java + PySpark: pip install -e ".[spark]"
Run: pytest tests/regression -m integration -v
"""

import json

import pytest

from handuflow.data_movement_controller.load_dispatcher import LoadDispatcher
from handuflow.data_movement_controller.data_class.load_config import LoadConfig
from handuflow.data_movement_controller.load_types.full_load import FullLoad
from handuflow.data_movement_controller.load_types.append_load import AppendLoad
from handuflow.data_movement_controller.load_types.incremental_cdc import IncrementalCDC
from handuflow.data_movement_controller.load_types.scd_type_2 import SCDType2

pytestmark = pytest.mark.integration

SELECTION_SCHEMA = {
    "type": "struct",
    "fields": [
        {"name": "id", "type": "long", "nullable": False, "metadata": {}},
        {"name": "value", "type": "string", "nullable": True, "metadata": {}},
    ],
}


def _feed_specs(source_table: str, **extra) -> dict:
    base = {
        "primary_key": "id",
        "composite_key": [],
        "partition_keys": [],
        "vacuum_hours": 168,
        "source_table_name": source_table,
        "selection_query": None,
        "selection_schema": SELECTION_SCHEMA,
        "standard_checks": [],
        "comprehensive_checks": [],
        "allow_unmatched_deletes": True,
    }
    base.update(extra)
    return base


def _master_spec(feed_id: int, load_type: str, target_table: str, feed_specs: dict) -> dict:
    return {
        "feed_id": feed_id,
        "target_unity_catalog": "local",
        "target_schema_name": "regression",
        "target_table_name": target_table,
        "load_type": load_type,
        "feed_specs": json.dumps(feed_specs),
        "data_flow_direction": "WITHIN_UNITY_CATALOG",
    }


@pytest.fixture
def source_table(spark):
    spark.sql("CREATE DATABASE IF NOT EXISTS staging")
    spark.sql("CREATE DATABASE IF NOT EXISTS regression")
    table = "regression.source_customers"
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    spark.createDataFrame(
        [(1, "alpha"), (2, "beta"), (3, "gamma")],
        ["id", "value"],
    ).write.format("delta").mode("overwrite").saveAsTable(table)
    spark.sql(
        f"ALTER TABLE {table} SET TBLPROPERTIES "
        "(delta.enableChangeDataFeed = true)"
    )
    yield table
    spark.sql(f"DROP TABLE IF EXISTS {table}")


def _drop_staging(spark, name: str) -> None:
    """Remove staging snapshots so a re-run is not treated as unchanged source."""
    for suffix in (
        f"t_full_{name}",
        f"t_incr_{name}",
        f"t_incr_cdf_changes_{name}",
    ):
        spark.sql(f"DROP TABLE IF EXISTS staging.{suffix}")


def _drop_target(spark, name: str) -> None:
    spark.sql(f"DROP TABLE IF EXISTS regression.{name}")
    _drop_staging(spark, name)


class TestFullLoad:
    def test_full_load_row_count_matches_source(self, spark, local_config, source_table):
        target = "tgt_full"
        _drop_target(spark, target)
        spec = _master_spec(
            1,
            "FULL_LOAD",
            target,
            _feed_specs(source_table),
        )
        result = LoadDispatcher(spec, spark, local_config).dispatch()
        assert result.success is True
        assert result.skipped is False
        assert result.total_rows_inserted == 3
        assert spark.table(f"regression.{target}").count() == 3


class TestAppendLoad:
    def test_append_increases_row_count(self, spark, local_config, source_table):
        target = "tgt_append"
        _drop_target(spark, target)
        spark.createDataFrame([(10, "seed")], ["id", "value"]).write.format(
            "delta"
        ).mode("overwrite").saveAsTable(f"regression.{target}")
        spark.sql(
            f"ALTER TABLE regression.{target} "
            f"SET TBLPROPERTIES ('data.load_type' = 'APPEND_LOAD')"
        )
        spec = _master_spec(
            2,
            "APPEND_LOAD",
            target,
            _feed_specs(source_table),
        )
        result = LoadDispatcher(spec, spark, local_config).dispatch()
        assert result.success is True
        assert result.total_rows_inserted == 3
        assert spark.table(f"regression.{target}").count() == 4


class TestIncrementalCDC:
    def test_incremental_cdc_creates_target(self, spark, local_config, source_table):
        target = "tgt_cdc"
        _drop_target(spark, target)
        spec = _master_spec(
            3,
            "INCREMENTAL_CDC",
            target,
            _feed_specs(source_table),
        )
        result = LoadDispatcher(spec, spark, local_config).dispatch()
        assert result.success is True
        assert spark.catalog.tableExists(f"regression.{target}")
        assert spark.table(f"regression.{target}").count() >= 1


class TestSCDType2:
    def test_scd2_initial_load(self, spark, local_config, source_table):
        target = "tgt_scd2"
        _drop_target(spark, target)
        spec = _master_spec(
            4,
            "SCD_TYPE_2",
            target,
            _feed_specs(source_table),
        )
        result = LoadDispatcher(spec, spark, local_config).dispatch()
        assert result.success is True
        assert spark.catalog.tableExists(f"regression.{target}")
        active = spark.table(f"regression.{target}").filter("_x_is_active = 1").count()
        assert active >= 1


class TestStagingSafety:
    def test_empty_source_blocks_staging_without_allow_empty(
        self, spark, local_config, source_table
    ):
        spark.sql("CREATE DATABASE IF NOT EXISTS regression")
        spark.sql("DROP TABLE IF EXISTS regression.empty_src")
        spark.createDataFrame([], "id long, value string").write.format(
            "delta"
        ).mode("overwrite").saveAsTable("regression.empty_src")
        config = LoadConfig(
            config=local_config,
            master_specs=_master_spec(5, "FULL_LOAD", "tgt_empty_guard", {}),
            feed_specs=_feed_specs(
                "regression.empty_src",
                allow_empty_source=False,
            ),
            target_unity_catalog="local",
            target_schema_name="regression",
            target_table_name="tgt_empty_guard",
        )
        loader = FullLoad(config=config, spark=spark)
        with pytest.raises(Exception):
            loader.load()

    def test_load_type_conflict_rejected(self, spark, local_config, source_table):
        target = "tgt_conflict"
        _drop_target(spark, target)
        spark.createDataFrame([(1, "x")], ["id", "value"]).write.format(
            "delta"
        ).mode("overwrite").saveAsTable(f"regression.{target}")
        spark.sql(
            f"ALTER TABLE regression.{target} "
            f"SET TBLPROPERTIES ('data.load_type' = 'FULL_LOAD')"
        )
        spec = _master_spec(
            6,
            "APPEND_LOAD",
            target,
            _feed_specs(source_table),
        )
        result = LoadDispatcher(spec, spark, local_config).dispatch()
        assert result.success is False
        assert result.exception_if_any is not None


def _table_version(spark, table: str) -> int | None:
    if not spark.catalog.tableExists(table):
        return None
    row = spark.sql(f"DESCRIBE HISTORY {table}").orderBy("version", ascending=False).first()
    return int(row["version"]) if row and row["version"] is not None else None


def _seed_append_target(spark, target: str) -> None:
    spark.createDataFrame([(10, "seed")], ["id", "value"]).write.format(
        "delta"
    ).mode("overwrite").saveAsTable(f"regression.{target}")
    spark.sql(
        f"ALTER TABLE regression.{target} "
        f"SET TBLPROPERTIES ('data.load_type' = 'APPEND_LOAD')"
    )


class TestSourceUnchangedSkip:
    @pytest.mark.parametrize(
        "load_type,target",
        [
            ("FULL_LOAD", "tgt_skip_full"),
            ("INCREMENTAL_CDC", "tgt_skip_cdc"),
            ("SCD_TYPE_2", "tgt_skip_scd2"),
        ],
    )
    def test_second_run_skips_without_touching_target_or_staging(
        self, spark, local_config, source_table, load_type, target
    ):
        _drop_target(spark, target)
        spec = _master_spec(
            100,
            load_type,
            target,
            _feed_specs(source_table),
        )
        first = LoadDispatcher(spec, spark, local_config).dispatch()
        assert first.success is True
        assert first.skipped is False

        target_fqn = f"regression.{target}"
        staging_full = f"staging.t_full_{target}"
        target_version = _table_version(spark, target_fqn)
        staging_version = _table_version(spark, staging_full)
        assert target_version is not None
        assert staging_version is not None

        second = LoadDispatcher(spec, spark, local_config).dispatch()
        assert second.success is True
        assert second.skipped is True
        assert second.total_rows_inserted == 0
        assert _table_version(spark, target_fqn) == target_version
        assert _table_version(spark, staging_full) == staging_version

    def test_append_second_run_skips_without_touching_target_or_staging(
        self, spark, local_config, source_table
    ):
        target = "tgt_skip_append"
        _drop_target(spark, target)
        _seed_append_target(spark, target)
        spec = _master_spec(
            101,
            "APPEND_LOAD",
            target,
            _feed_specs(source_table),
        )
        first = LoadDispatcher(spec, spark, local_config).dispatch()
        assert first.success is True
        assert first.skipped is False

        target_fqn = f"regression.{target}"
        staging_full = f"staging.t_full_{target}"
        target_version = _table_version(spark, target_fqn)
        staging_version = _table_version(spark, staging_full)
        assert target_version is not None
        assert staging_version is not None

        second = LoadDispatcher(spec, spark, local_config).dispatch()
        assert second.success is True
        assert second.skipped is True
        assert second.total_rows_inserted == 0
        assert _table_version(spark, target_fqn) == target_version
        assert _table_version(spark, staging_full) == staging_version


class TestLoadDispatcher:
    def test_unsupported_load_type_raises(self, spark, local_config):
        spec = _master_spec(99, "INVALID_TYPE", "x", _feed_specs("regression.source_customers"))
        result = LoadDispatcher(spec, spark, local_config).dispatch()
        assert result.success is False
        assert result.error_code == "HF030"
