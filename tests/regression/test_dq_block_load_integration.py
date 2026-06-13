"""
Integration tests: pre-load DQ failures must block feed load.

Requires Java + PySpark: pip install -e ".[spark]"
"""

import json

import pytest

from handuflow.data_quality.runner.feed_data_quality_runner import FeedDataQualityRunner

pytestmark = pytest.mark.integration

SELECTION_SCHEMA = {
    "type": "struct",
    "fields": [
        {"name": "alpha3_b", "type": "string", "nullable": True, "metadata": {}},
        {"name": "alpha3_t", "type": "string", "nullable": True, "metadata": {}},
        {"name": "alpha2", "type": "string", "nullable": True, "metadata": {}},
        {"name": "english", "type": "string", "nullable": True, "metadata": {}},
    ],
}


def _master_row(feed_specs: dict) -> dict:
    return {
        "feed_id": 1,
        "feed_name": "iso_language_codes",
        "target_unity_catalog": "local",
        "target_schema_name": "silver",
        "target_table_name": "t_iso_language_codes",
        "load_type": "FULL_LOAD",
        "feed_specs": json.dumps(feed_specs),
        "data_flow_direction": "WITHIN_UNITY_CATALOG",
    }


@pytest.fixture
def dq_source_table(spark):
    spark.sql("CREATE DATABASE IF NOT EXISTS regression")
    spark.sql("CREATE DATABASE IF NOT EXISTS staging")
    table = "regression.dq_block_source"
    target = "regression.dq_block_target"
    staging = "staging.t_full_dq_block_target"
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    spark.sql(f"DROP TABLE IF EXISTS {target}")
    spark.sql(f"DROP TABLE IF EXISTS {staging}")

    spark.createDataFrame(
        [
            ("USA1", "US1", "US", "Germany"),
            ("USA1", "US1b", "US", "Germany"),
            ("USA3", "US3", "US", "Canada"),
        ],
        ["alpha3_b", "alpha3_t", "alpha2", "english"],
    ).write.format("delta").mode("overwrite").saveAsTable(table)

    spark.createDataFrame(
        [("STALE", "US0", "US", "Germany")],
        ["alpha3_b", "alpha3_t", "alpha2", "english"],
    ).write.format("delta").mode("overwrite").saveAsTable(target)

    yield table, target


def test_standard_check_failure_blocks_ingest_and_leaves_target(spark, dq_source_table):
    source_table, target_table = dq_source_table
    feed_specs = {
        "primary_key": "alpha3_b",
        "composite_key": [],
        "partition_keys": [],
        "vacuum_hours": 168,
        "source_table_name": source_table,
        "selection_query": None,
        "selection_schema": SELECTION_SCHEMA,
        "standard_checks": [
            {
                "check_sequence": ["_check_primary_key"],
                "column_name": "alpha3_b",
                "threshold": 0,
            }
        ],
        "comprehensive_checks": [],
    }

    runner = FeedDataQualityRunner(spark, [_master_row(feed_specs)])
    runner.run()
    manifest = runner.finalize()

    assert len(manifest) == 1
    row = manifest[0]
    assert row["can_ingest"] is False
    assert row["ingest_block_reason"] == "standard_checks_failed"
    assert row["standard_checks_passed"] is False

    ingestible = [entry["feed_id"] for entry in manifest if entry.get("can_ingest") is True]
    assert ingestible == []

    stale = spark.table(target_table).collect()[0]["alpha3_b"]
    assert stale == "STALE"


def test_pre_load_comprehensive_failure_blocks_ingest(spark, dq_source_table):
    source_table, target_table = dq_source_table
    feed_specs = {
        "primary_key": "alpha3_b",
        "composite_key": [],
        "partition_keys": [],
        "vacuum_hours": 168,
        "source_table_name": source_table,
        "selection_query": None,
        "selection_schema": SELECTION_SCHEMA,
        "standard_checks": [],
        "comprehensive_checks": [
            {
                "check_name": "always_fail",
                "load_stage": "PRE_LOAD",
                "query": f"SELECT * FROM {source_table} WHERE alpha3_b = 'USA1'",
                "threshold": 0,
                "severity": "ERROR",
            }
        ],
    }

    runner = FeedDataQualityRunner(spark, [_master_row(feed_specs)])
    runner.run()
    manifest = runner.finalize()

    row = manifest[0]
    assert row["can_ingest"] is False
    assert row["ingest_block_reason"] == "pre_load_comprehensive_checks_failed"
    assert row["comprehensive_pre_load_passed"] is False

    stale = spark.table(target_table).collect()[0]["alpha3_b"]
    assert stale == "STALE"


def test_orchestrator_gate_excludes_blocked_feeds(spark, dq_source_table):
    source_table, _target_table = dq_source_table
    feed_specs = {
        "primary_key": "alpha3_b",
        "composite_key": [],
        "partition_keys": [],
        "vacuum_hours": 168,
        "source_table_name": source_table,
        "selection_query": None,
        "selection_schema": SELECTION_SCHEMA,
        "standard_checks": [
            {
                "check_sequence": ["_check_primary_key"],
                "column_name": "alpha3_b",
                "threshold": 0,
            }
        ],
        "comprehensive_checks": [],
    }
    master_row = _master_row(feed_specs)

    runner = FeedDataQualityRunner(spark, [master_row])
    runner.run()
    manifest = runner.finalize()
    ingestible_ids = {
        entry["feed_id"] for entry in manifest if entry.get("can_ingest") is True
    }
    allowed_rows = [master_row] if master_row["feed_id"] in ingestible_ids else []
    assert allowed_rows == []
