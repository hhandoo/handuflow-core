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






from handuflow import run, load_config, create_restore_point, list_restore_points, get_restore_point_details, initiate_restore

cfg = load_config("/home/handu/Documents/handuflow-core/tests/uat_testing/handuflow_dir_full_load/config.ini")

# result = run(spark, config=cfg)

# print(result.status)        # COMPLETED, COMPLETED_WITH_ERRORS, etc.
# # print(result.load_results)  # per-feed outcomes
# # print(result.run_id)        # for logs/reports


# if result.succeeded and result.master_specs is not None:
#     rp_id = create_restore_point(
#         spark,
#         cfg,
#         result.master_specs,   # same validated specs the run used
#         created_by="uat_test",
#     )
#     print(rp_id)



print(list_restore_points(spark, cfg))


request_id = initiate_restore(spark, cfg, "HFRP0001", requested_by="uat_test")


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