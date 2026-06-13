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



# cd ~/Documents/handuflow-core/tests/uat_testing
# source .venvuat/bin/activate
# pip install -e '/home/handu/Documents/handuflow-core/.[spark]'
# python full_load_test.py

spark.sql("CREATE DATABASE IF NOT EXISTS demo")
spark.sql("CREATE DATABASE IF NOT EXISTS staging")
spark.sql("CREATE DATABASE IF NOT EXISTS silver")



# spark.sql("DROP TABLE IF EXISTS demo.test")
# df = spark.range(1, 101).toDF("row_id")
# df = (
#     df.withColumn("alpha3_b", expr("concat('USA', cast(row_id as string))"))
#     .withColumn("alpha3_t", expr("concat('US', cast(row_id as string))"))
#     .withColumn("alpha2", expr("substring('US', 1, 2)"))
#     .withColumn(
#         "english",
#         expr("""
#             CASE
#                 WHEN row_id % 4 = 0 THEN 'United States'
#                 WHEN row_id % 4 = 1 THEN 'Germany'
#                 WHEN row_id % 4 = 2 THEN 'India'
#                 ELSE 'Canada'
#             END
#         """),
#     )
#     .drop("row_id")
# )

# df.write.format("delta").saveAsTable("demo.test")




# # update operation

#spark.sql("delete from demo.test where english = 'India';")


# spark.sql("update demo.test set english = 'Updated_USA' where alpha3_b = 'USA2';")


spark.sql("""
INSERT INTO demo.test
VALUES (
    'USA102341',
    'US102341',
    'US',
    'United States'
)
""")





from handuflow import run, load_config, create_restore_point

cfg = load_config("/home/handu/Documents/handuflow-core/tests/uat_testing/handuflow_dir_full_load/config.ini")

result = run(spark, config=cfg)

print(result.status)        # COMPLETED, COMPLETED_WITH_ERRORS, etc.
# print(result.load_results)  # per-feed outcomes
# print(result.run_id)        # for logs/reports


if result.succeeded:
    rp_id = create_restore_point(
        spark,
        cfg,
        created_by="uat_test",
    )
    print(rp_id)







my_df = spark.sql("select * from demo.test")
my_df.orderBy('alpha3_b').show(truncate=False)
print(my_df.count())




my_df_staging = spark.sql("select * from staging.t_full_t_iso_language_codes")
my_df_staging.orderBy('alpha3_b').show(truncate=False)
print(my_df_staging.count())





my_df_op = spark.sql("select * from silver.t_iso_language_codes")
my_df_op.orderBy('alpha3_b').show(truncate=False)
print(my_df_op.count())





my_df_op = spark.sql("""
select * from silver.t_iso_language_codes
except
select * from demo.test
""")
my_df_op.show(truncate=False)
print(my_df_op.count())




my_df_op = spark.sql("""
select * from demo.test
except
select * from silver.t_iso_language_codes
""")
my_df_op.show(truncate=False)
print(my_df_op.count())






my_df_restore_audit = spark.sql("select * from system_admin.SYSTEM_RESTORE_AUDIT")
my_df_restore_audit.show(truncate=False)
print(my_df_restore_audit.count())






my_df_restore_audit = spark.sql("select * from system_admin.SYSTEM_RESTORE_POINTS")
my_df_restore_audit.show(truncate=False)
print(my_df_restore_audit.count())