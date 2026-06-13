"""Delta table helpers shared by cleanup and restore."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urlparse

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


def quote_table(table_name: str) -> str:
    parts = [p.strip() for p in table_name.split(".") if p.strip()]
    return ".".join(f"`{part.replace('`', '``')}`" for part in parts)


def resolve_hive_table_path(spark: SparkSession, table_name: str) -> Path:
    """Hive-style warehouse path for a catalog table name (db.table)."""
    db_name, short_name = table_name.split(".", 1)
    warehouse = spark.conf.get("spark.sql.warehouse.dir", "spark-warehouse")
    warehouse_path = Path(urlparse(str(warehouse)).path or warehouse).resolve()
    return warehouse_path / f"{db_name}.db" / short_name


def drop_delta_table(spark: SparkSession, table_name: str) -> None:
    """Drop catalog entry and remove on-disk Delta files."""
    location: str | None = None
    if spark.catalog.tableExists(table_name):
        try:
            rows = spark.sql(f"DESCRIBE DETAIL {quote_table(table_name)}").collect()
            if rows:
                location = rows[0]["location"]
        except Exception:
            location = None
        spark.catalog.clearCache()
        spark.sql(f"DROP TABLE IF EXISTS {quote_table(table_name)}")
    if location:
        path = Path(urlparse(str(location)).path)
        if path.exists():
            shutil.rmtree(path, ignore_errors=True)
    elif "." in table_name:
        table_dir = resolve_hive_table_path(spark, table_name)
        if table_dir.exists():
            shutil.rmtree(table_dir, ignore_errors=True)


def overwrite_delta_table(spark: SparkSession, table_name: str, df: DataFrame) -> None:
    """
    Drop + path overwrite + CREATE TABLE.

    Avoids Spark V2 saveAsTable(overwrite) truncate-in-batch-mode failures
    on local Hive/Delta catalogs.
    """
    drop_delta_table(spark, table_name)
    table_path = resolve_hive_table_path(spark, table_name)
    table_path.parent.mkdir(parents=True, exist_ok=True)
    df.write.format("delta").mode("overwrite").save(str(table_path))
    location = table_path.resolve().as_uri()
    spark.sql(
        f"CREATE TABLE {quote_table(table_name)} USING DELTA LOCATION '{location}'"
    )


def append_delta_table(spark: SparkSession, table_name: str, df: DataFrame) -> None:
    """Append rows to an existing Delta table."""
    df.write.format("delta").mode("append").saveAsTable(table_name)


def is_delta_table(spark: SparkSession, table_name: str) -> bool:
    """Return True when the metastore table is Delta (works with local Hive catalog)."""
    if not spark.catalog.tableExists(table_name):
        return False
    try:
        row = spark.sql(f"DESCRIBE DETAIL {quote_table(table_name)}").collect()[0]
        return str(row["format"]).lower() == "delta"
    except Exception:
        return False
