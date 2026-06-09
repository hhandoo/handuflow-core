# `data_movement_controller/` package

Feed execution: dispatch load strategies, parallel groups, Delta staging, integrity checks.

**Import:** `from handuflow.data_movement_controller import DataLoadController, LoadDispatcher`

---

## `data_movement_controller/__init__.py`

| | |
|---|---|
| **Visibility** | Semi-public |
| **Exports** | `DataLoadController`, `LoadDispatcher` |

---

## `data_movement_controller/data_load_controller.py`

| | |
|---|---|
| **Visibility** | Semi-public |
| **Purpose** | Parallel feed execution grouped by `parallelism_group_number`. |

### Class: `DataLoadController`

| Method | Description |
|--------|-------------|
| `run(master_specs_df, data_flow_direction)` | Execute all feeds for a direction |
| `get_load_results()` | List of `LoadResult` from last run |

Feeds in the same parallelism group run concurrently (thread pool); groups run sequentially.

**Dependencies:** `config.config_paths`, `config.run_logger`, `load_dispatcher`, `load_result`, `exception.error_handler`

---

## `data_movement_controller/load_dispatcher.py`

| | |
|---|---|
| **Visibility** | Semi-public |
| **Purpose** | Route a single master-spec row to the correct load strategy. |

### Class: `LoadDispatcher`

| Method | Description |
|--------|-------------|
| `dispatch(row)` | Parse feed specs, instantiate strategy, return `LoadResult` |

Maps `load_type` → strategy class:

| `load_type` | Strategy |
|-------------|----------|
| `FULL_LOAD` | `FullLoad` |
| `APPEND_LOAD` | `AppendLoad` |
| `INCREMENTAL_CDC` | `IncrementalCDC` |
| `SCD_TYPE_2` | `SCDType2` |
| `API_EXTRACTOR` | `APIExtractor` |
| `STORAGE_FETCH` | `StorageFetch` |

Unsupported types return `LoadResult` with `error_code=HF030`.

**Dependencies:** `constants`, `config.catalog_resolver`, all `load_types.*`, `load_config`, `load_result`, `exception.*`

---

## `data_movement_controller/base_load_strategy.py`

| | |
|---|---|
| **Visibility** | Internal |
| **Purpose** | Abstract template for Delta loads. |

### Class: `BaseLoadStrategy`

Template method `execute()` orchestrates:

1. Staging table creation (`t_full_*` / `t_cdc_*`)
2. Source read and column selection
3. Audit column injection
4. Partition handling
5. Delta write (merge/overwrite/append per subclass)
6. Integrity verification

Subclasses implement `load()` for type-specific logic.

**Dependencies:** `config.catalog_resolver`, `load_config`, `load_result`, `load_integrity`, `exception.data_load_exception`

---

## `data_movement_controller/data_class/load_config.py`

| | |
|---|---|
| **Visibility** | Internal |
| **Purpose** | Dataclass bundling config, feed specs, and target identifiers. |

### `LoadConfig`

Fields: `config`, `feed_specs` (dict), `feed_id`, `catalog_resolver`, staging/target table names, partition keys, etc.

**Dependencies:** None

---

## `data_movement_controller/data_class/load_result.py`

| | |
|---|---|
| **Visibility** | Internal (consumed by public `RunResult`) |
| **Purpose** | Per-feed load outcome. |

### `LoadResult`

| Field | Description |
|-------|-------------|
| `feed_id` | Feed identifier |
| `load_type` | Strategy used |
| `status` | `SUCCESS` / `FAILED` |
| `duration_seconds` | Elapsed time |
| `rows_affected` | Row count when available |
| `error_code` | HF### on failure |
| `error_message` | Safe error text |

**Dependencies:** None

---

## `data_movement_controller/audit_columns.py`

| | |
|---|---|
| **Visibility** | Internal |
| **Purpose** | Manage staging vs target audit/lineage columns by load type. |

### Constants

| Name | Description |
|------|-------------|
| `STAGING_ONLY_COLUMNS` | Columns dropped before target write |
| `CDC_STREAM_COLUMN` | CDF stream identifier column |
| `TARGET_ROW_HASH_COLUMN` | Row hash for change detection |
| `SCD_TARGET_COLUMNS` | SCD Type 2 historization columns |

### Class: `AuditColumns`

Schema preparation, row hash computation, CDC/SCD column injection helpers.

**Dependencies:** `exception.data_load_exception`

---

## `data_movement_controller/load_integrity.py`

| | |
|---|---|
| **Visibility** | Internal |
| **Purpose** | Post-load integrity verification. |

### Class: `LoadIntegrityVerifier`

Static methods: schema match, row count sanity, primary-key null checks, empty-source detection. Raises `DataLoadException` on violation.

**Dependencies:** `audit_columns`, `exception.data_load_exception`

---

## Load strategies (`load_types/`)

All strategies extend `BaseLoadStrategy` and are **internal**.

### `load_types/full_load.py` — `FullLoad`

Full snapshot replace via staging `t_full_*` tables. Overwrites target partition or full table.

### `load_types/append_load.py` — `AppendLoad`

Append-only inserts from staging snapshot. No updates or deletes.

### `load_types/incremental_cdc.py` — `IncrementalCDC`

Incremental CDC merge from staging CDF stream to target. Uses change data feed columns.

### `load_types/scd_type_2.py` — `SCDType2`

SCD Type 2 historization: surrogate keys, `effective_from`/`effective_to`, active flag.

### `load_types/api_extractor.py` — `APIExtractor`

HTTP/API extraction into bronze Delta tables. Supports REST GET with JSON/Parquet response parsing.

### `load_types/storage_fetch.py` — `StorageFetch`

Fetch files from storage paths (CSV, JSON, Parquet, XML) into bronze Delta tables.
