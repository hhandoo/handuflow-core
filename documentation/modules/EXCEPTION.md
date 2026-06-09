# `exception/` package

Enterprise error management: typed exceptions, HF### error codes, structured error records.

**Import:** `from handuflow import ConfigError` or `from handuflow.errors import ConfigError`

Full code registry: [ERROR_CODES.md](../ERROR_CODES.md)

---

## `exception/__init__.py`

| | |
|---|---|
| **Visibility** | Semi-public |
| **Purpose** | Re-export exception hierarchy and error utilities. |

---

## `exception/base_exception.py`

| | |
|---|---|
| **Visibility** | Public |
| **Purpose** | Unified base exception with error codes, feed context, and serialization. |

### Class: `BaseException`

| Attribute / method | Description |
|--------------------|-------------|
| `error_code` | HF### string (e.g. `HF020`) |
| `feed_id` | Optional feed identifier |
| `message` | Human-readable message |
| `short_message` | Truncated message for logs |
| `to_pretty_text()` | Formatted multi-line output |
| `to_dict()` | Structured dict for `RunResult.phase_errors` |

All typed exceptions inherit from this class.

**Dependencies:** `exception.error_codes`

---

## Typed exceptions

Each subclass sets a default `error_code` and is raised in its domain.

| Module | Class | Default code | Domain |
|--------|-------|-------------|--------|
| `config_error.py` | `ConfigError` | HF020 | Config loading / validation |
| `validation_error.py` | `ValidationError` | HF013 | Master-spec launch validation |
| `data_load_exception.py` | `DataLoadException` | HF039 | Delta load / merge failures |
| `data_quality_exception.py` | `DataQualityException` | HF073 | DQ check failures |
| `extraction_exception.py` | `ExtractionException` | HF050 | API extraction failures |
| `storage_fetch_exception.py` | `StorageFetchException` | HF060 | File/storage ingest failures |
| `result_generation_exception.py` | `ResultGenerationException` | HF080 | Excel report generation |
| `system_error.py` | `SystemError` | HF099 | Orchestrator / system failures |

---

## `exception/error_codes.py`

| | |
|---|---|
| **Visibility** | Semi-public |
| **Purpose** | HF### error code registry with descriptions and categories. |

### Types and constants

| Name | Description |
|------|-------------|
| `ErrorCodeEntry` | Named tuple: code, description, category |
| `ERROR_CODES` | Dict mapping HF### → `ErrorCodeEntry` |
| `DEFAULT_ERROR_CODE` | Fallback code (`HF099`) |
| `VALIDATION_RULE_CODES` | Map validation rule classes → HF001–HF011 |

### Functions

| Name | Description |
|------|-------------|
| `get_error_description(code)` | Lookup description |
| `get_error_category(code)` | Lookup category (CONFIG, VALIDATION, LOAD, etc.) |
| `format_error_label(code)` | `"HF020: Config error"` style label |

**Dependencies:** None

---

## `exception/error_handler.py`

| | |
|---|---|
| **Visibility** | Public (helpers via root) |
| **Purpose** | Wrap raw exceptions, resolve codes, build structured error records. |

| Function | Description |
|----------|-------------|
| `resolve_error_code(exc)` | Extract or infer HF### from any exception |
| `wrap_exception(exc, *, feed_id=None, error_code=None)` | Convert to typed `BaseException` subclass |
| `exception_to_record(exc)` | Dict for `phase_errors` / `LoadResult` |
| `exception_message(exc)` | Safe user-facing message (no stack leakage) |

Used throughout orchestrator, load dispatcher, and DQ runner to ensure consistent error shape.

**Dependencies:** All typed exceptions, `error_codes`
