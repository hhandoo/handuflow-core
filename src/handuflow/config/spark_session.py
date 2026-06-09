# inbuilt
import os


def is_databricks_runtime() -> bool:
    """True when running on a Databricks cluster (used for config path checks only)."""
    return bool(os.environ.get("DATABRICKS_RUNTIME_VERSION"))
