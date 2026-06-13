# API reference

Stable public surface — import from `handuflow` (not internal modules).

---

## Quick start

```python
from pyspark.sql import SparkSession
from handuflow import run, Orchestrator, RunResult, RunStatus

spark = SparkSession.builder.appName("HanduFlow").enableHiveSupport().getOrCreate()

# Option A — one-liner
result = run(spark, config_path="/path/to/handuflow_dir/config.ini")

# Option B — explicit orchestrator
import configparser
cfg = configparser.ConfigParser()
cfg.read("/path/to/handuflow_dir/config.ini")
result = Orchestrator(spark, config=cfg).run()

print(result.status)          # RunStatus enum
print(result.succeeded)       # True if COMPLETED or COMPLETED_WITH_ERRORS
print(result.load_results)    # per-feed LoadResult list
print(result.phase_errors)    # structured dicts with error_code
```

---

## `run()`

```python
handuflow.run(
    spark,
    config=None,
    *,
    config_path=None,
    validate_config=True,
    check_paths_exist=None,
) -> RunResult
```

| Parameter | Description |
|-----------|-------------|
| `spark` | Active SparkSession (caller-owned) |
| `config` | Parsed `config.ini` |
| `config_path` | Path to load via `load_config()` |
| `validate_config` | Validate before run (default `True`) |
| `check_paths_exist` | Path checks when loading file; auto `False` on Databricks |

---

## `Orchestrator`

```python
Orchestrator(spark, config, *, validate_config=True)
```

| Method | Returns |
|--------|---------|
| `.run()` | `RunResult` |

---

## `RunResult`

| Field | Type | Description |
|-------|------|-------------|
| `status` | `RunStatus` | `COMPLETED`, `COMPLETED_WITH_ERRORS`, `VALIDATION_FAILED`, `FAILED` |
| `run_id` | `str` | UUID for logs and reports |
| `load_results` | `list[LoadResult]` | Per-feed outcomes |
| `phase_errors` | `list[dict]` | Phase failures (`error_code`, `message`, `traceback`) |
| `archived_log_path` | `str \| None` | Final log file path |
| `message` | `str` | Human summary |
| `succeeded` | `bool` | Property — completed with or without phase errors |

`str(result)` → status value (backward compatible).

---

## Config helpers

```python
from handuflow import load_config, validate_handuflow_config, CatalogResolver, is_databricks_runtime

cfg = load_config("/path/to/config.ini", check_paths_exist=True)
validate_handuflow_config(cfg)

resolver = CatalogResolver("local", config=cfg)
table = resolver.target_table("silver", "country_codes")
```

---

## Constants

```python
from handuflow import SOURCE_TO_BRONZE, SUPPORTED_LOAD_TYPES
```

| Name | Value |
|------|--------|
| `SOURCE_TO_BRONZE` | `"SOURCE_TO_BRONZE"` |
| `SUPPORTED_LOAD_TYPES` | `FULL_LOAD`, `APPEND_LOAD`, `INCREMENTAL_CDC`, `SCD_TYPE_2`, `API_EXTRACTOR`, `STORAGE_FETCH` |

---

## Errors

```python
from handuflow import ConfigError, ValidationError, DataLoadException, wrap_exception
# or
from handuflow.errors import ConfigError, ERROR_CODES, format_error_label
```

| Helper | Use |
|--------|-----|
| `wrap_exception(exc)` | Convert any exception → typed HanduFlow error |
| `exception_to_record(exc)` | Dict for logs / APIs |
| `format_error_label("HF004")` | `"HF004: Primary key validation failed"` |

Full code list: [ERROR_CODES.md](ERROR_CODES.md).

---

## System Restore

```python
from handuflow import (
    create_restore_point,
    list_restore_points,
    get_restore_point_details,
    initiate_restore,
)

from handuflow import run, load_config, create_restore_point, initiate_restore

cfg = load_config("/path/to/config.ini")
result = run(spark, config=cfg)

rp_id = create_restore_point(spark, cfg, created_by="user")
request_id = initiate_restore(
    spark, cfg, rp_id, requested_by="user"
)
```

See [SYSTEM_RESTORE.md](SYSTEM_RESTORE.md).

---

## Version

```python
import handuflow
handuflow.__version__
```

---

## Module reference

Full documentation for all 74 Python modules: [MODULES.md](MODULES.md)
