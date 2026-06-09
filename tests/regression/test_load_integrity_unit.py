"""Unit tests for load integrity helpers (no Spark required)."""

import pytest
from pyspark.sql.types import StructField, StringType, StructType

from handuflow.data_movement_controller.load_integrity import LoadIntegrityVerifier
from handuflow.exception.data_load_exception import DataLoadException


@pytest.mark.integration
def test_enforce_schema_adds_missing_column(spark):
    df = spark.createDataFrame([("a",)], ["alpha3_b"])
    schema = StructType(
        [
            StructField("alpha3_b", StringType(), True),
            StructField("english", StringType(), True),
        ]
    )
    out = LoadIntegrityVerifier.enforce_schema(df, schema)
    assert out.columns == ["alpha3_b", "english"]
    row = out.collect()[0]
    assert row["alpha3_b"] == "a"
    assert row["english"] is None


def test_require_primary_keys_raises_when_missing():
    with pytest.raises(DataLoadException):
        LoadIntegrityVerifier.require_primary_keys(
            {"primary_key": None, "composite_key": []},
            "silver.target",
        )


def test_require_primary_keys_returns_composite():
    keys = LoadIntegrityVerifier.require_primary_keys(
        {"primary_key": None, "composite_key": ["a", "b"]},
        "silver.target",
    )
    assert keys == ["a", "b"]


def test_sanitize_sql_identifier():
    assert LoadIntegrityVerifier.sanitize_sql_identifier("12-abc", "feed") == "feed_12_abc"


@pytest.mark.integration
def test_verify_source_not_empty_blocks_empty_snapshot(spark):
    empty = spark.createDataFrame([], "id string")
    with pytest.raises(DataLoadException):
        LoadIntegrityVerifier.verify_source_not_empty_for_sync(
            empty, operation="test", allow_empty=False
        )
