# `orchestrator/` package

Main batch pipeline controller: validation, ingest, DQ, loads, reporting, lineage, cleanup.

**Import:** `from handuflow import Orchestrator, RunResult, RunStatus`

---

## `orchestrator/__init__.py`

| | |
|---|---|
| **Visibility** | Semi-public |
| **Purpose** | Subpackage exports. |

**Exports:** `Orchestrator`, `RunResult`, `RunStatus`

---

## `orchestrator/orchestrator.py`

| | |
|---|---|
| **Visibility** | Public |
| **Purpose** | Main entry point for a HanduFlow batch run. |

### Class: `Orchestrator`

**Constructor:** `Orchestrator(spark, config, *, validate_config=True)`

| Attribute | Description |
|-----------|-------------|
| `run_id` | UUID hex for this run |
| `logging_config` | `LoggingConfig` instance |
| `spark` | Caller-provided SparkSession |
| `file_hunt_path` | Base path from config |
| `validated_master_specs` | Post-validation pandas DataFrame |

### `run() -> RunResult`

Executes the full pipeline:

1. System prerequisites (master-spec validation)
2. Bronze ingest (`SOURCE_TO_BRONZE` feeds)
3. Pre-load data quality (gates further ingest)
4. Medallion loads (parallel by `parallelism_group_number`)
5. Post-load data quality (report-only)
6. Excel result report
7. Lineage diagram PNG
8. System cleanup (retention + vacuum)

Individual feed failures are isolated; logs and cleanup always run in `finally`.

### Private methods (internal)

| Method | Phase |
|--------|-------|
| `_system_prerequisites()` | Validation |
| `_validate_and_load()` | Bronze + medallion loads |
| `_finalize_run()` | Reports, lineage, cleanup |

**Dependencies:** `constants`, `config.*`, `run_guard`, `result`, `result_generator`, `validation`, `data_quality`, `data_movement_controller`, `system_cleanup`, `data_flow_diagram_generator`, `exception`

---

## `orchestrator/result.py`

| | |
|---|---|
| **Visibility** | Public |
| **Purpose** | Structured terminal run outcome. |

### Enum: `RunStatus`

| Value | Meaning |
|-------|---------|
| `COMPLETED` | All feeds succeeded |
| `COMPLETED_WITH_ERRORS` | Some feeds failed; run finished |
| `VALIDATION_FAILED` | Master-spec validation did not pass |
| `FAILED` | Unrecoverable orchestrator failure |

### Dataclass: `RunResult`

| Field | Type | Description |
|-------|------|-------------|
| `status` | `RunStatus` | Terminal status |
| `load_results` | `list[LoadResult]` | Per-feed outcomes |
| `phase_errors` | `list[dict]` | Structured errors with `error_code` |
| `run_id` | `str` | Run identifier |
| `succeeded` | `bool` | Property: `COMPLETED` or `COMPLETED_WITH_ERRORS` |

**Dependencies:** `data_movement_controller.data_class.load_result`

---

## `orchestrator/run_guard.py`

| | |
|---|---|
| **Visibility** | Internal |
| **Purpose** | Execute a pipeline phase, capture errors without aborting the run. |

### `run_phase(logger, phase_name, fn, *, feed_id=None) -> tuple[Any, dict | None]`

Calls `fn()`, returns `(result, error_record)`. On exception, wraps via `error_handler` and returns `(None, record)` so later phases (cleanup, log archival) still execute.

**Dependencies:** `config.run_logger`, `exception.error_codes`, `exception.error_handler`
