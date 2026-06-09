"""Spark session bootstrap tuned for full cluster resource utilisation."""

from __future__ import annotations

import configparser
import os
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import SparkSession

from handuflow.config.config_paths import system_schema
from handuflow.config.validate import validate_handuflow_config

from tests.helpers.spark_isolation import spark_data_dirs, with_isolated_hive

PROJECT_ROOT = Path(__file__).resolve().parents[2]
FILES_DEV = PROJECT_ROOT / "files_dev"
CONFIG_PATH = FILES_DEV / "config.ini"
MASTER_SPECS_PATH = FILES_DEV / "master_specs.xlsx"

_SPARK_LOG_LEVEL = "ERROR"


@dataclass(frozen=True)
class ClusterResources:
    cpu_cores: int
    memory_gb: int
    driver_memory_gb: int
    executor_memory_gb: int
    shuffle_partitions: int
    default_parallelism: int
    master_url: str

    def summary(self) -> str:
        return (
            f"cores={self.cpu_cores} mem={self.memory_gb}GB "
            f"driver={self.driver_memory_gb}g executor={self.executor_memory_gb}g "
            f"shuffle={self.shuffle_partitions} parallelism={self.default_parallelism} "
            f"master={self.master_url}"
        )


def _available_memory_gb() -> int:
    try:
        with open("/proc/meminfo", encoding="utf-8") as fh:
            for line in fh:
                if line.startswith(("MemAvailable:", "MemTotal:")):
                    kb = int(line.split()[1])
                    return max(4, kb // (1024 * 1024))
    except OSError:
        pass
    return 8


def detect_cluster_resources(*, heavy: bool = False, extreme: bool = False) -> ClusterResources:
    """Detect host resources and derive Spark settings that use them efficiently."""
    cores = max(2, os.cpu_count() or 4)
    mem_gb = _available_memory_gb()

    if extreme:
        driver_gb = min(32, max(8, int(mem_gb * 0.35)))
        executor_gb = min(32, max(8, int(mem_gb * 0.45)))
        shuffle = cores * 8
        parallelism = cores * 4
    elif heavy:
        driver_gb = min(24, max(6, int(mem_gb * 0.3)))
        executor_gb = min(24, max(6, int(mem_gb * 0.4)))
        shuffle = cores * 6
        parallelism = cores * 3
    else:
        driver_gb = min(12, max(4, int(mem_gb * 0.25)))
        executor_gb = min(12, max(4, int(mem_gb * 0.35)))
        shuffle = cores * 4
        parallelism = cores * 2

    return ClusterResources(
        cpu_cores=cores,
        memory_gb=mem_gb,
        driver_memory_gb=driver_gb,
        executor_memory_gb=executor_gb,
        shuffle_partitions=shuffle,
        default_parallelism=parallelism,
        master_url=f"local[{cores}]",
    )


def load_config(*, validate: bool = True) -> configparser.ConfigParser:
    if not CONFIG_PATH.is_file():
        raise FileNotFoundError(
            f"HanduFlow config not found: {CONFIG_PATH}. "
            "Ensure files_dev/config.ini exists and paths match your checkout."
        )
    cfg = configparser.ConfigParser()
    cfg.read(CONFIG_PATH)
    if validate:
        validate_handuflow_config(cfg, check_paths_exist=True)
    return cfg


def create_spark(
    app_name: str = "HanduFlowE2E",
    *,
    default_parallelism: int | None = None,
    heavy: bool = False,
    extreme: bool = False,
) -> tuple[SparkSession, ClusterResources]:
    """Create a Spark session configured for maximum local-cluster throughput."""
    resources = detect_cluster_resources(heavy=heavy, extreme=extreme)
    parallelism = default_parallelism or resources.default_parallelism
    warehouse, metastore = spark_data_dirs("e2e")

    builder = (
        with_isolated_hive(
            SparkSession.builder.appName(app_name).master(resources.master_url),
            warehouse,
            metastore,
        )
        .enableHiveSupport()
        .config("spark.driver.memory", f"{resources.driver_memory_gb}g")
        .config("spark.executor.memory", f"{resources.executor_memory_gb}g")
        .config("spark.driver.cores", str(resources.cpu_cores))
        .config("spark.executor.cores", str(max(1, resources.cpu_cores - 1)))
        .config("spark.default.parallelism", str(parallelism))
        .config("spark.sql.shuffle.partitions", str(resources.shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.adaptive.localShuffleReader.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.memory.fraction", "0.8")
        .config("spark.memory.storageFraction", "0.3")
        .config("spark.sql.files.maxPartitionBytes", "134217728")
        .config(
            "spark.sql.autoBroadcastJoinThreshold",
            "-1" if extreme else "64m",
        )
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoCompact.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
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
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(_SPARK_LOG_LEVEL)
    return spark, resources


def ensure_qaft_schemas(spark: SparkSession, config: configparser.ConfigParser | None = None) -> None:
    """Create QA and system metadata schemas for full local pipeline capabilities."""
    schemas = ["qaft_source", "qaft_silver", "staging", "qaft_ref"]
    if config is not None:
        sys_schema = system_schema(config)
        if sys_schema:
            schemas.append(sys_schema.split(".")[-1])
    for schema in schemas:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema}")
