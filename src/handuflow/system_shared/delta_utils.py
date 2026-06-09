"""Delta table helpers shared by cleanup and restore."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def quote_table(table_name: str) -> str:
    parts = [p.strip() for p in table_name.split(".") if p.strip()]
    return ".".join(f"`{part.replace('`', '``')}`" for part in parts)


def is_delta_table(spark: SparkSession, table_name: str) -> bool:
    """Return True when the metastore table is Delta (works with local Hive catalog)."""
    if not spark.catalog.tableExists(table_name):
        return False
    try:
        row = spark.sql(f"DESCRIBE DETAIL {quote_table(table_name)}").collect()[0]
        return str(row["format"]).lower() == "delta"
    except Exception:
        return False
