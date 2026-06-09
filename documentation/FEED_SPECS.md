# Feed specs (`feed_specs` JSON)

Stored as a JSON string in the master spec `feed_specs` column. Two shapes: **medallion** (downstream loads) and **bronze** (`SOURCE_TO_BRONZE`).

---

## Medallion feeds (required top-level keys)

Startup validation requires **all** keys below for `data_flow_direction != SOURCE_TO_BRONZE` (optional keys may also be present):

```json
{
  "primary_key": "iso2_code",
  "composite_key": [],
  "partition_keys": [],
  "vacuum_hours": 168,
  "source_table_name": "bronze.t_country_codes_raw",
  "selection_query": null,
  "selection_schema": { "type": "struct", "fields": [ ] },
  "standard_checks": [],
  "comprehensive_checks": []
}
```

### Keys

| Key | Required | Description |
|-----|----------|-------------|
| `primary_key` | Yes | Single column name, or `""` if only `composite_key` is used |
| `composite_key` | Yes | List of columns for composite uniqueness (may be `[]`) |
| `partition_keys` | Yes | List; `[]` = no partition. Changing partitions rebuilds staging and the target for all medallion load types (`FULL_LOAD`, `APPEND_LOAD`, `INCREMENTAL_CDC`, `SCD_TYPE_2`) |
| `vacuum_hours` | Yes | Delta vacuum retention hint (integer) |
| `source_table_name` | Yes | Spark table read for DQ and staging (`schema.table` or `catalog.schema.table`) |
| `selection_query` | Yes | SQL for staging snapshot; `null` or `""` → read full `source_table_name` |
| `selection_schema` | Yes | Spark `StructType` JSON; enforced on staging |
| `standard_checks` | Yes | List (may be `[]`); see [DATA_QUALITY.md](DATA_QUALITY.md) |
| `comprehensive_checks` | Yes | List (may be `[]`); see [DATA_QUALITY.md](DATA_QUALITY.md) |

### Optional (behavior)

| Key | Default | Purpose |
|-----|---------|---------|
| `allow_empty_source` | `false` | If `false`, empty staging aborts load (avoids accidental mass deletes) |
| `allow_unmatched_deletes` | `false` | If `false`, MERGE omits `WHEN NOT MATCHED BY SOURCE DELETE` |

Example: `documentation/examples/medallion/country_codes.json`.

---

## Bronze feeds (`SOURCE_TO_BRONZE`)

Not subject to the medallion key-set rule. Typical keys:

| Key | Used by |
|-----|---------|
| `selection_schema` | All bronze loads |
| `vacuum_hours`, `partition_keys` | Delta write |
| `ingestion_config` | `API_EXTRACTOR` — URL, method, params, retry, response format |
| `storage_config` | `STORAGE_FETCH` — `file_type`, `lookup_directory` |

Examples: `documentation/examples/bronze/simple_get_request.json`, `storage_fetch_xml.json`, `documentation/examples/medallion/country_codes_raw.json`.

---

## Staging & source

1. **Source** — `selection_query` if set, else `spark.read.table(source_table_name)`.
2. **Schema** — `selection_schema` applied in staging.
3. **Keys** — `primary_key` / `composite_key` drive MERGE and post-load integrity checks.
4. **Partitions** — `partition_keys`; nulls in partition columns fail the load.

`source_table_name` must exist before pre-load DQ (standard checks read this table).

---

## `selection_schema`

Spark struct JSON:

```json
{
  "type": "struct",
  "fields": [
    { "name": "iso2_code", "type": "string", "nullable": true, "metadata": {} }
  ]
}
```

Column names in checks and keys must appear in this schema (validated at startup for medallion feeds).

---

## Audit columns (staging vs target)

HanduFlow adds internal columns during **staging** (`staging.t_full_*`, `staging.t_incr_*`). Target tables only keep what each `load_type` needs:

| Column | Staging | FULL_LOAD target | APPEND target | INCREMENTAL_CDC target | SCD_TYPE_2 target |
|--------|---------|------------------|---------------|------------------------|-------------------|
| `_x_load_id` | Yes | No | No | No | No |
| `_x_commit_version` / `_x_commit_timestamp` | Yes | No | No | No | No |
| `_x_operation` | Yes (incr) | No | No | No (merge only) | No (merge only) |
| `_x_row_hash` | Yes | No | No | **Yes** | **Yes** |
| `_x_surrogate_key`, `_x_is_active`, `_x_date_from`, … | No | No | No | No | **Yes** |

Business columns come only from `selection_schema`. Do not list audit columns in `feed_specs`.

Implementation: `src/handuflow/data_movement_controller/audit_columns.py`.

---

## Quick checklist (new medallion feed)

1. Bronze table populated (`source_table_name`).
2. `feed_specs` JSON has all 9 required keys.
3. `primary_key` / `composite_key` match real columns.
4. `selection_schema` matches source columns.
5. Add `standard_checks` / `comprehensive_checks` only where needed ([DATA_QUALITY.md](DATA_QUALITY.md)).
6. Master row: correct `load_type`, `target_*`, `parallelism_group_number`, `is_active=True`.
