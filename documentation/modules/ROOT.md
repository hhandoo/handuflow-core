# Package root modules

Paths relative to `src/handuflow/`.

---

## `__init__.py`

| | |
|---|---|
| **Visibility** | Public |
| **Purpose** | Package entry point; re-exports the stable public API from `api.py`. |

**Exports:** Everything in `api.__all__` via `from handuflow.api import *`.

**Usage:**

```python
import handuflow
from handuflow import run, Orchestrator, RunResult
```

**Dependencies:** `handuflow.api`

---

## `api.py`

| | |
|---|---|
| **Visibility** | Public |
| **Purpose** | Stable public API surface — orchestration, config helpers, errors, restore. |

### Functions

| Name | Description |
|------|-------------|
| `run(spark, config=None, *, config_path=None, validate_config=True, check_paths_exist=None)` | Convenience entry: load config (if needed), construct `Orchestrator`, return `RunResult`. |

### Re-exports

| Category | Names |
|----------|-------|
| Orchestration | `Orchestrator`, `RunResult`, `RunStatus` |
| Config | `CatalogResolver`, `load_config`, `validate_handuflow_config`, `is_databricks_runtime` |
| Constants | `SOURCE_TO_BRONZE`, `SUPPORTED_LOAD_TYPES` |
| Exceptions | `BaseException`, `ConfigError`, `ValidationError`, `DataLoadException`, `DataQualityException`, `ExtractionException`, `StorageFetchException`, `ResultGenerationException`, `SystemError` |
| Error helpers | `ERROR_CODES`, `wrap_exception`, `exception_to_record`, `exception_message`, `format_error_label` |
| Restore | `SystemRestore`, `create_restore_point`, `list_restore_points`, `get_restore_point_details`, `initiate_restore` |
| Version | `__version__` |

**Dependencies:** `config`, `constants`, `exception`, `orchestrator`, `system_restore`

---

## `constants.py`

| | |
|---|---|
| **Visibility** | Public |
| **Purpose** | Shared pipeline constants. |

### Constants

| Name | Value | Description |
|------|-------|-------------|
| `SOURCE_TO_BRONZE` | `"SOURCE_TO_BRONZE"` | Data-flow direction for bronze ingest feeds |
| `SUPPORTED_LOAD_TYPES` | `frozenset({...})` | Allowed `load_type` values: `FULL_LOAD`, `APPEND_LOAD`, `INCREMENTAL_CDC`, `SCD_TYPE_2`, `API_EXTRACTOR`, `STORAGE_FETCH` |

**Dependencies:** None

---

## `errors.py`

| | |
|---|---|
| **Visibility** | Public |
| **Purpose** | Stable alias for `handuflow.exception` — alternative import path for error types. |

**Exports:** Full exception hierarchy, `ERROR_CODES`, `VALIDATION_RULE_CODES`, `wrap_exception`, `exception_to_record`, `exception_message`, `format_error_label`, `get_error_description`, `get_error_category`, `resolve_error_code`.

```python
from handuflow.errors import ConfigError, wrap_exception
# equivalent to:
from handuflow import ConfigError, wrap_exception
```

**Dependencies:** `handuflow.exception`
