#!/usr/bin/env python3
"""Run HanduFlow Orchestrator locally against files_dev/config.ini."""

from __future__ import annotations

import configparser
import random
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

from handuflow import Orchestrator

config = configparser.ConfigParser()
config.read(PROJECT_ROOT / "files_dev" / "config.ini")

spark = (
    SparkSession.builder.appName("HanduFlow")
    .enableHiveSupport()
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "4g")
    .config(
        "spark.jars.packages",
        "io.delta:delta-spark_2.12:3.1.0,com.databricks:spark-xml_2.12:0.17.0",
    )
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension",
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .getOrCreate()
)

for schema in ("demo", "staging", "silver"):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema}")

spark.sql("DROP TABLE IF EXISTS demo.test")
df = (
    spark.range(1, 101)
    .toDF("row_id")
    .withColumn("alpha3_b", expr("concat('USA', cast(row_id as string))"))
    .withColumn("alpha3_t", expr("concat('US', cast(row_id as string))"))
    .withColumn("alpha2", expr("substring('US', 1, 2)"))
    .withColumn(
        "english",
        expr(
            """
            CASE
                WHEN row_id % 4 = 0 THEN 'United States'
                WHEN row_id % 4 = 1 THEN 'Germany'
                WHEN row_id % 4 = 2 THEN 'India'
                ELSE 'Canada'
            END
        """
        ),
    )
    .drop("row_id")
)
df.write.format("delta").mode("overwrite").saveAsTable("demo.test")

spark.sql(
    f"""
    INSERT INTO demo.test VALUES (
        'USA{random.randint(1000, 9999)}',
        'US{random.randint(1000, 9999)}',
        'US',
        '{random.choice(["United States", "Germany", "India", "Canada"])}'
    )
    """
)

result = Orchestrator(spark=spark, config=config).run()
print(f"HanduFlow run finished: {result.status}")
