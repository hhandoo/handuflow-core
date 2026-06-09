"""Synthetic Spark datasets for E2E QA (Spark-native generation)."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

BUSINESS_COLUMNS = [
    "id",
    "business_key",
    "name",
    "country",
    "city",
    "category",
    "amount",
    "status",
    "created_date",
    "modified_date",
]

SELECTION_SCHEMA = {
    "type": "struct",
    "fields": [
        {"name": "id", "type": "long", "nullable": False, "metadata": {}},
        {"name": "business_key", "type": "string", "nullable": True, "metadata": {}},
        {"name": "name", "type": "string", "nullable": True, "metadata": {}},
        {"name": "country", "type": "string", "nullable": True, "metadata": {}},
        {"name": "city", "type": "string", "nullable": True, "metadata": {}},
        {"name": "category", "type": "string", "nullable": True, "metadata": {}},
        {"name": "amount", "type": "double", "nullable": True, "metadata": {}},
        {"name": "status", "type": "string", "nullable": True, "metadata": {}},
        {"name": "created_date", "type": "date", "nullable": True, "metadata": {}},
        {"name": "modified_date", "type": "date", "nullable": True, "metadata": {}},
    ],
}


def generate_dataset(
    spark: SparkSession,
    row_count: int,
    *,
    seed: int = 42,
    inject_edge_cases: bool = True,
) -> DataFrame:
    """Build a Spark DataFrame using range + SQL expressions (no Python rows)."""
    if row_count <= 0:
        return spark.createDataFrame([], "id long, business_key string")
    df = spark.range(1, row_count + 1).toDF("id")
    df = (
        df.withColumn(
            "business_key", F.concat(F.lit("BK-"), F.format_string("%08d", F.col("id")))
        )
        .withColumn(
            "name",
            F.when(
                F.lit(inject_edge_cases) & (F.col("id") % 17 == 0),
                F.concat(F.lit("x" * 200), F.lit("🎉")),
            ).otherwise(F.concat(F.lit("Entity-"), F.col("id").cast("string"))),
        )
        .withColumn(
            "country",
            F.when(F.col("id") % 6 == 0, F.lit("🇮🇳"))
            .when(F.col("id") % 6 == 1, F.lit("中国"))
            .when(F.col("id") % 6 == 2, F.lit("US"))
            .when(F.col("id") % 6 == 3, F.lit("DE"))
            .when(F.col("id") % 6 == 4, F.lit("IN"))
            .otherwise(F.lit("BR")),
        )
        .withColumn(
            "city",
            F.when(F.col("id") % 11 == 0, F.lit(""))
            .when(F.col("id") % 11 == 1, F.lit("  "))
            .when(F.col("id") % 11 == 2, F.lit("東京"))
            .otherwise(
                F.element_at(
                    F.array(F.lit("NYC"), F.lit("Berlin"), F.lit("Mumbai")),
                    ((F.col("id") % 3) + 1).cast("int"),
                )
            ),
        )
        .withColumn(
            "category",
            F.element_at(
                F.array(
                    F.lit("A"),
                    F.lit("B"),
                    F.lit("premium"),
                    F.lit("β-test"),
                    F.lit("emoji-🚀"),
                ),
                ((F.col("id") % 5) + 1).cast("int"),
            ),
        )
        .withColumn(
            "amount",
            F.when(F.col("id") % 10 == 0, F.lit(None).cast("double"))
            .when(F.col("id") % 13 == 0, F.lit(-9999.99))
            .when(F.col("id") % 17 == 0, F.lit(1e12))
            .otherwise(F.col("id").cast("double") * 1.5),
        )
        .withColumn(
            "status",
            F.when(F.col("id") % 9 == 0, F.lit(None).cast("string"))
            .when(F.col("id") % 9 == 1, F.lit(""))
            .otherwise(
                F.element_at(
                    F.array(F.lit("active"), F.lit("inactive"), F.lit("pending")),
                    ((F.col("id") % 3) + 1).cast("int"),
                )
            ),
        )
        .withColumn(
            "created_date",
            F.when(F.col("id") % 19 == 0, F.lit(None).cast("date"))
            .otherwise(
                F.date_add(
                    F.lit("2024-01-01").cast("date"),
                    (F.col("id") % 365).cast("int"),
                )
            ),
        )
        .withColumn(
            "modified_date",
            F.when(F.col("id") % 23 == 0, F.lit(None).cast("date"))
            .when(
                F.col("id") % 29 == 0,
                F.date_add(F.lit("2030-01-01").cast("date"), F.col("id").cast("int") % 30),
            )
            .otherwise(
                F.date_add(
                    F.lit("2024-06-01").cast("date"),
                    (F.col("id") % 180).cast("int"),
                )
            ),
        )
    )
    if inject_edge_cases:
        df = df.withColumn(
            "business_key",
            F.when(
                F.col("id") % 31 == 0,
                F.concat(F.lit("BK-DUP-"), (F.col("id") % 7).cast("string")),
            ).otherwise(F.col("business_key")),
        )
    return df


def write_source_table(
    spark: SparkSession,
    table_name: str,
    df: DataFrame,
    *,
    partition_keys: list[str] | None = None,
) -> None:
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    writer = df.write.format("delta").mode("overwrite")
    if partition_keys:
        writer = writer.partitionBy(*partition_keys)
    writer.saveAsTable(table_name)
    spark.sql(
        f"ALTER TABLE {table_name} SET TBLPROPERTIES "
        "(delta.enableChangeDataFeed = true)"
    )


def business_projection(df: DataFrame) -> DataFrame:
    cols = [c for c in BUSINESS_COLUMNS if c in df.columns]
    return df.select(*cols)
