# Data quality

Applies to **medallion** feeds only (`data_flow_direction != SOURCE_TO_BRONZE`). Configured in `feed_specs.standard_checks` and `feed_specs.comprehensive_checks`.

## Pipeline order

```text
Pre-load (per feed, isolated)
  → standard checks (if any configured)
  → PRE_LOAD comprehensive SQL checks (if any configured)
  → can_ingest?
Medallion load (only feeds with can_ingest=true)
Post-load (report only)
  → POST_LOAD comprehensive SQL checks (only successfully loaded feeds)
```

| Check type | Blocks load? | When |
|------------|--------------|------|
| Standard | Yes, if configured and fail | Pre-load |
| Comprehensive `PRE_LOAD` | Yes, if configured and fail | Pre-load |
| Comprehensive `POST_LOAD` | No | After successful load |

If pre-load fails, the feed is **skipped**; other feeds continue.

---

## Standard checks

Spark aggregations on `source_table_name`. **Omitted list = skipped** (does not block).

### Check object

```json
{
  "check_sequence": ["_check_nulls", "_check_duplicates"],
  "column_name": "english",
  "threshold": 0
}
```

| Field | Type | Notes |
|-------|------|--------|
| `check_sequence` | string[] | Method names on `StandardDQExecutor` |
| `column_name` | string or string[] | Column(s) under test |
| `threshold` | int | Max **ratio** of bad rows (0 = zero tolerance) |

Pass rule: `(bad_count / total_count) <= threshold`.

### Built-in methods

| Method | `column_name` | What fails |
|--------|---------------|------------|
| `_check_nulls` | column | NULL values |
| `_check_duplicates` | column | duplicate groups |
| `_check_primary_key` | column or list | nulls + duplicates on key |
| `_check_composite_key` | list | nulls + duplicates on composite |
| `_check_value_range` | column | needs `range` in params (via executor defaults) |
| `_check_allowed_values` | column | needs `allowed_values` list |

Run methods in `check_sequence` order; all must pass for that check object to pass.

---

## Comprehensive checks

SQL-based. **Any row returned** counts as failures; fail when `failed_records > threshold` (usually `threshold: 0`).

### Check object

```json
{
  "check_name": "orphan_countries",
  "query": "SELECT c.iso2 FROM silver.countries c LEFT JOIN ref.countries r ON c.iso2 = r.iso2 WHERE r.iso2 IS NULL",
  "severity": "ERROR",
  "threshold": 0,
  "load_stage": "PRE_LOAD",
  "dependency_dataset": ["ref.countries"]
}
```

| Field | Required | Notes |
|-------|----------|--------|
| `check_name` | Recommended | Report label |
| `query` | Yes | Spark SQL; result row count = `failed_records` |
| `severity` | No | Default `ERROR`; stored in report (failures always block pre-load) |
| `threshold` | No | Default `0` |
| `load_stage` | No | `PRE_LOAD` (default) or `POST_LOAD` |
| `dependency_dataset` | No | Tables that must exist before query runs |

`dependency_dataset` entries must be valid table names (`catalog.schema.table` on UC).

### Stages

| `load_stage` | Runs with | On failure |
|--------------|-----------|------------|
| `PRE_LOAD` | Standard checks | Feed not loaded |
| `POST_LOAD` | After load | Report only; load kept |

Feeds not loaded get POST_LOAD rows with `status: NOT_RUN` in the Excel report.

---

## Gating (`can_ingest`)

| Configured | Must pass to load |
|------------|-------------------|
| `standard_checks` non-empty | All standard check objects |
| `comprehensive_checks` with `PRE_LOAD` | All PRE_LOAD SQL checks |
| Neither | Load proceeds (no DQ gate) |

POST_LOAD never affects `can_ingest`.

---

## Run report (Excel)

Under `{file_hunt_path}/{outbound_directory_name}/results_{run_id}_*.xlsx`:

| Sheet | Content |
|-------|---------|
| System Readiness | Startup validation |
| Load Report | Per-feed load result |
| Dashboard | Per-feed DQ summary flags |
| Standard Check Result | Exploded standard check rows |
| Comprehensive Check Result | All PRE_LOAD + POST_LOAD rows (`load_stage`, `status`, `failed_records`, …) |
| Feed Status (B-G) | Full feed DQ manifest |

Dashboard columns: `standard_checks_configured`, `standard_checks_passed`, `comprehensive_pre_load_configured`, `comprehensive_pre_load_passed`, `comprehensive_post_load_configured`, `comprehensive_post_load_passed`, `can_ingest`.

`*_passed` is blank when the corresponding checks were **not configured** (not run). `TRUE`/`FALSE` only appear after checks actually ran.

Statuses: `PASSED`, `FAILED`, `ERROR` (executor error), `NOT_RUN` (post-load skipped).

---

## Examples

- Standard only: `documentation/examples/medallion/country_codes.json`
- Pre + post comprehensive: `documentation/examples/bronze/` (pattern; fix table names for your env)
- Regression: `pytest tests/regression/test_dq_gating.py -v`
