from pyspark.sql import SparkSession
from pyspark.sql.functions import expr



spark = (
    SparkSession.builder.appName("HanduFlow")
    .enableHiveSupport()
    # .config("spark.driver.memory", "4g")
    # .config("spark.executor.memory", "4g")
    # .config("spark.default.parallelism", "4")
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


spark.sql("CREATE DATABASE IF NOT EXISTS demo")
spark.sql("CREATE DATABASE IF NOT EXISTS staging")
spark.sql("CREATE DATABASE IF NOT EXISTS silver")



spark.sql("DROP TABLE IF EXISTS demo.test")
df = spark.range(1, 101).toDF("row_id")
df = (
    df.withColumn("alpha3_b", expr("concat('USA', cast(row_id as string))"))
    .withColumn("alpha3_t", expr("concat('US', cast(row_id as string))"))
    .withColumn("alpha2", expr("substring('US', 1, 2)"))
    .withColumn(
        "english",
        expr("""
            CASE
                WHEN row_id % 4 = 0 THEN 'United States'
                WHEN row_id % 4 = 1 THEN 'Germany'
                WHEN row_id % 4 = 2 THEN 'India'
                ELSE 'Canada'
            END
        """),
    )
    .drop("row_id")
)

df.write.format("delta").saveAsTable("demo.test")





from handuflow import run




result = run(spark, config_path="/home/handu/Documents/handuflow-core/tests/uat_testing/handuflow_dir_full_load/config.ini")
print(result.status)        # COMPLETED, COMPLETED_WITH_ERRORS, etc.
print(result.load_results)  # per-feed outcomes
print(result.run_id)        # for logs/reports