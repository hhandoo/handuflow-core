# Master specs (`master_specs.xlsx`)

One row = one feed. File lives at `{file_hunt_path}/{master_spec_name}` (see `config.ini` `[FILES]`). Sheet name must be **`master_specs`**. Only rows with `is_active = True` run.

## Required columns

| Column | Type | Notes |
|--------|------|--------|
| `feed_id` | string/int | Unique; referenced in logs and reports |
| `system_name` | string | Reporting / lineage grouping |
| `subsystem_name` | string | |
| `category` | string | |
| `sub_category` | string | |
| `data_flow_direction` | string | See [Flow directions](#flow-directions) |
| `residing_layer` | string | e.g. bronze, silver |
| `feed_name` | string | Display name |
| `feed_type` | string | Bronze: `API_EXTRACTOR`, `STORAGE_FETCH`. Medallion: often same column but load driven by `load_type` |
| `feed_specs` | string | **JSON** (see [FEED_SPECS.md](FEED_SPECS.md)) |
| `load_type` | string | See [Load types](#load-types) |
| `target_unity_catalog` | string | `local` / `testing` locally; real catalog on Databricks |
| `target_schema_name` | string | Target schema |
| `target_table_name` | string | Target table (bronze table name for extract feeds) |
| `suggested_feed_name` | string | Lineage label |
| `parallelism_group_number` | int | Feeds in the same group run in parallel (cap: `max_concurrent_batches`) |
| `parent_feed_id` | string | Lineage parent (optional semantics in diagram) |
| `is_active` | bool | `True` to include |

No extra or missing columns — startup validation fails if the set differs.

## Flow directions

| Value | When it runs | DQ |
|-------|----------------|-----|
| `INGESTION` | First, for external/API/storage ingest | Skipped (reduced validation) |
| `WITHIN_UNITY_CATALOG` | Loads between Unity Catalog tables | Full [feed spec](FEED_SPECS.md) + [DQ](DATA_QUALITY.md) |

Only these two values are allowed in `data_flow_direction`.

`INGESTION` rows use `load_type` `API_EXTRACTOR` or `STORAGE_FETCH`. `WITHIN_UNITY_CATALOG` rows use `FULL_LOAD`, `APPEND_LOAD`, `INCREMENTAL_CDC`, or `SCD_TYPE_2`.

Source and target table names in `feed_specs` / master specs are used as-is (any Unity Catalog schema). The system does not infer bronze/silver/gold layer tables.

## Load types

| `load_type` | Role |
|-------------|------|
| `API_EXTRACTOR` | HTTP → temp parquet → Delta bronze table |
| `STORAGE_FETCH` | Files (XML/JSON/parquet) from `storage_config` → bronze |
| `FULL_LOAD` | Rebuild target from staging |
| `APPEND_LOAD` | Append staging into target |
| `INCREMENTAL_CDC` | Merge CDC from source Delta CDF |
| `SCD_TYPE_2` | Historize dimension changes |

### Target behavior (insert / update / delete)

No source changes → load **skipped**. One load type per target (Delta property lock).

| Load type | First load | Insert | Update | Delete |
|-----------|------------|--------|--------|--------|
| `FULL_LOAD` | Overwrite | Next overwrite | Replaced on overwrite | Removed on overwrite |
| `APPEND_LOAD` | Append all | Append new | Ignored | Ignored |
| `INCREMENTAL_CDC` | Create + load | MERGE insert | MERGE if hash changed | MERGE delete |
| `SCD_TYPE_2` | Active rows | New version | Close old + new active | Close active (history kept) |

## Parallelism

- Same `parallelism_group_number` → executed together up to `max_concurrent_batches` in `config.ini`.
- Different groups run sequentially (group order follows spec order).
- A failure in one feed does **not** stop other feeds in the batch.

## Minimal row example (medallion)

| feed_id | data_flow_direction | load_type | target_unity_catalog | feed_specs |
|---------|---------------------|-----------|----------------------|------------|
| 101 | WITHIN_UNITY_CATALOG | INCREMENTAL_CDC | local | `{"primary_key":"id",...}` (full JSON in cell) |

Paste JSON as a single line in Excel or use a formula/export tool; invalid JSON fails validation.

## Validation (startup)

`SystemLaunchValidator` checks file presence, column set, non-null required fields, parseable `feed_specs`, and feed-spec rules for non-bronze feeds. Fix the **System Readiness** sheet in the run Excel if validation fails.
