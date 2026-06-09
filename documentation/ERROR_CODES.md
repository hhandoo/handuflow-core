# HanduFlow error codes

Stable `HF###` codes for logs, `RunResult.phase_errors`, `LoadResult.error_code`, and DQ manifests.

Format: **`HF###: <description>`**

Wrap unknown exceptions with `wrap_exception()` — never surfaces raw Python errors to callers.

---

## Validation (HF001–HF019)

| Code | Description |
|------|-------------|
| **HF001** | Feed specs JSON is invalid or not parseable |
| **HF002** | Master specs structure or required columns invalid |
| **HF003** | Master specs file missing or unreadable |
| **HF004** | Primary key validation failed |
| **HF005** | Composite key validation failed |
| **HF006** | Partition key validation failed |
| **HF007** | Column missing from selection query or schema |
| **HF008** | Standard checks structure invalid |
| **HF009** | Comprehensive checks structure invalid |
| **HF010** | Vacuum hours value invalid |
| **HF011** | Comprehensive checks dependency dataset invalid |
| **HF012** | Validation context or Spark table metadata error |
| **HF013** | System launch validation failed unexpectedly |

---

## Configuration (HF020–HF029)

| Code | Description |
|------|-------------|
| **HF020** | config.ini validation failed (missing keys or sections) |
| **HF021** | Configured path does not exist |
| **HF022** | config.ini file not found or not parseable |

---

## Data load (HF030–HF049)

| Code | Description |
|------|-------------|
| **HF030** | Unsupported load_type for feed |
| **HF031** | Feed specs JSON invalid at load dispatch |
| **HF032** | No load handler registered for load_type |
| **HF033** | Partition column preparation failed |
| **HF034** | Schema mismatch or enforcement failed |
| **HF035** | Row count verification failed |
| **HF036** | Empty source sync blocked |
| **HF037** | Primary key integrity check failed |
| **HF038** | Target table missing after reported successful load |
| **HF039** | Delta merge, staging, or partition layout error (default load error) |
| **HF040** | Unknown audit column load kind |
| **HF041** | Append load strategy failed |
| **HF042** | Incremental CDC load failed |
| **HF043** | SCD Type 2 load failed |
| **HF044** | Full load strategy failed |
| **HF045** | Load type conflict on existing target table |

---

## Extraction (HF050–HF059)

| Code | Description |
|------|-------------|
| **HF050** | API extraction failed |
| **HF051** | API response format unsupported |
| **HF052** | API content-type mismatch |
| **HF053** | API request failed after retries |

---

## Storage fetch (HF060–HF069)

| Code | Description |
|------|-------------|
| **HF060** | Storage fetch load failed |
| **HF061** | Invalid storage file_type |
| **HF062** | Invalid storage storage_type |

---

## Data quality (HF070–HF079)

| Code | Description |
|------|-------------|
| **HF070** | Comprehensive DQ dependency table missing |
| **HF071** | Unsupported standard DQ check method |
| **HF072** | Invalid DQ check parameters |
| **HF073** | DQ executor runtime error (default DQ error) |

---

## Reporting (HF080–HF089)

| Code | Description |
|------|-------------|
| **HF080** | Result generation failed |
| **HF081** | Excel report write failed |
| **HF082** | Result segregation failed |

---

## System / orchestration (HF090–HF099)

| Code | Description |
|------|-------------|
| **HF090** | Spark session is required but missing |
| **HF091** | Unexpected orchestrator failure |
| **HF092** | Pipeline phase failure |
| **HF093** | Parallel feed dispatch failure |
| **HF094** | Log archive or run summary failure |
| **HF095** | System cleanup failure |
| **HF096** | Lineage diagram generation failure |
| **HF097** | System restore failure or invalid restore point |
| **HF099** | Unknown or unclassified error |

---

## Where codes appear

| Location | Fields |
|----------|--------|
| `RunResult.phase_errors[]` | `error_code`, `error_label`, `message`, `phase`, `traceback` |
| `LoadResult` | `error_code`, `exception_if_any` |
| DQ manifest (pre-load failure) | `dq_error_code`, `dq_error` |
| Logs | `error_code=` in structured log steps |

---

## API

```python
from handuflow import wrap_exception, exception_to_record, ERROR_CODES, format_error_label
# or
from handuflow.errors import ConfigError, DataLoadException
```

Raise typed errors:

```python
from handuflow.exception import ValidationError, DataLoadException

raise ValidationError(message="...", error_code="HF004", feed_id=101)
```

Registry source: `src/handuflow/exception/error_codes.py`
