"""
Stable public API for HanduFlow.

Import from the package root::

    import handuflow
    from handuflow import Orchestrator, run, RunResult

Errors live under :mod:`handuflow.errors` (same types as re-exported here).
"""

from __future__ import annotations

import configparser
from pathlib import Path
from typing import TYPE_CHECKING

from handuflow.config import (
    CatalogResolver,
    is_databricks_runtime,
    load_config,
    validate_handuflow_config,
)
from handuflow.constants import (
    ALLOWED_DATA_FLOW_DIRECTIONS,
    INGESTION,
    SOURCE_TO_BRONZE,
    SUPPORTED_LOAD_TYPES,
    WITHIN_UNITY_CATALOG,
    is_ingestion_direction,
    is_within_unity_catalog_direction,
)
from handuflow.exception import (
    ERROR_CODES,
    BaseException,
    ConfigError,
    DataLoadException,
    DataQualityException,
    ExtractionException,
    ResultGenerationException,
    StorageFetchException,
    SystemError,
    ValidationError,
    exception_message,
    exception_to_record,
    format_error_label,
    wrap_exception,
)
from handuflow.orchestrator import Orchestrator, RunResult, RunStatus
from handuflow.system_restore import (
    SystemRestore,
    create_restore_point,
    get_latest_restore_point_id,
    get_restore_point_details,
    initiate_restore,
    list_restore_points,
)
from handuflow._version import __version__

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

__all__ = [
    "__version__",
    "run",
    "Orchestrator",
    "RunResult",
    "RunStatus",
    "CatalogResolver",
    "load_config",
    "validate_handuflow_config",
    "is_databricks_runtime",
    "INGESTION",
    "WITHIN_UNITY_CATALOG",
    "ALLOWED_DATA_FLOW_DIRECTIONS",
    "is_ingestion_direction",
    "is_within_unity_catalog_direction",
    "SOURCE_TO_BRONZE",
    "SUPPORTED_LOAD_TYPES",
    "BaseException",
    "ConfigError",
    "ValidationError",
    "DataLoadException",
    "DataQualityException",
    "ExtractionException",
    "StorageFetchException",
    "ResultGenerationException",
    "SystemError",
    "ERROR_CODES",
    "wrap_exception",
    "exception_to_record",
    "exception_message",
    "format_error_label",
    "SystemRestore",
    "create_restore_point",
    "list_restore_points",
    "get_restore_point_details",
    "get_latest_restore_point_id",
    "initiate_restore",
]


def run(
    spark: SparkSession,
    config: configparser.ConfigParser | None = None,
    *,
    config_path: str | Path | None = None,
    validate_config: bool = True,
    check_paths_exist: bool | None = None,
) -> RunResult:
    """
    Run the full HanduFlow pipeline (convenience entry point).

    Parameters
    ----------
    spark:
        Active Spark session (Delta + Hive or Unity Catalog configured by caller).
    config:
        Parsed ``config.ini``. Use this **or** ``config_path``.
    config_path:
        Path to ``config.ini``. Loaded via :func:`load_config`.
    validate_config:
        Validate config before the run (default ``True``).
    check_paths_exist:
        When loading from ``config_path``, verify directories exist.
        Defaults to ``True`` locally, ``False`` on Databricks.

    Returns
    -------
    RunResult
        Terminal status, per-feed load results, and phase errors.

    Examples
    --------
    ::

        from pyspark.sql import SparkSession
        from handuflow import run

        spark = SparkSession.builder.enableHiveSupport().getOrCreate()
        result = run(spark, config_path="/path/to/handuflow_dir/config.ini")
        print(result.status)
    """
    if config is None:
        if config_path is None:
            raise ConfigError(
                message="Provide config (parsed config.ini) or config_path",
                error_code="HF020",
            )
        if check_paths_exist is None:
            check_paths_exist = not is_databricks_runtime()
        config = load_config(config_path, check_paths_exist=check_paths_exist)

    return Orchestrator(
        spark,
        config=config,
        validate_config=validate_config,
    ).run()
