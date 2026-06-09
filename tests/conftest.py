# inbuilt
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

# Reuse Spark session across tests (expensive to start).
_spark_session = None


def _spark_available() -> bool:
    try:
        from pyspark.sql import SparkSession  # noqa: F401
        return True
    except ImportError:
        return False


@pytest.fixture(scope="session")
def spark():
    if not _spark_available():
        pytest.skip("PySpark not installed; use pip install -e '.[spark]'")
    if sys.version_info >= (3, 14):
        pytest.skip(
            "Integration tests require Python 3.10–3.12 (PySpark 3.5 + Python 3.14 has known issues)"
        )
    global _spark_session
    if _spark_session is None:
        from pyspark.sql import SparkSession

        from tests.helpers.spark_isolation import spark_data_dirs, with_isolated_hive

        warehouse, metastore = spark_data_dirs("regression")
        _spark_session = with_isolated_hive(
            SparkSession.builder.appName("HanduFlowRegression").master("local[2]"),
            warehouse,
            metastore,
        ).enableHiveSupport().config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.12:3.1.0,com.databricks:spark-xml_2.12:0.17.0",
        ).config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        ).config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        ).getOrCreate()
        try:
            _spark_session.range(1).count()
        except Exception as exc:
            pytest.skip(f"Spark session could not start: {exc}")
    return _spark_session


@pytest.fixture
def local_config(tmp_path):
    import configparser

    root = tmp_path / "handuflow_dir"
    (root / "temp").mkdir(parents=True)
    (root / "dmc_temp").mkdir(parents=True)
    cfg = configparser.ConfigParser()
    cfg.read_dict(
        {
            "DEFAULT": {
                "file_hunt_path": str(root),
                "outbound_directory_name": "outbound",
                "log_directory_name": "logs",
                "temp_log_location": str(root / "temp"),
                "log_retention_policy_in_days": "1",
                "max_concurrent_batches": "2",
                "global_vacuum_hours": "168",
                "system_schema": "system_admin",
            },
            "PLATFORM": {"runtime_mode": "local"},
            "DMC_CONFIG": {"temp": str(root / "dmc_temp")},
            "FILES": {"master_spec_name": "master_specs.xlsx"},
            "LINEAGE_DIAGRAM": {
                "BOX_WIDTH": "4.4",
                "BOX_HEIGHT": "2.2",
                "X_GAP": "2.0",
                "Y_GAP": "2.5",
                "ROOT_GAP": "2.0",
            },
        }
    )
    return cfg


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "integration: tests requiring a live Spark session (deselect with -m 'not integration')",
    )
