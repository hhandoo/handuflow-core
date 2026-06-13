# System Restore

Delta Lake version-based restore points for tables declared in master specs.

**`WITHIN_UNITY_CATALOG` feeds:** source, target, and staging tables exactly as configured (no inferred medallion layers).

**`INGESTION` feeds:** landing target table only.

Schemas are **not** auto-created; create Unity Catalog / Hive databases before running loads or restore.

Metadata lives under **`[DEFAULT] system_schema`** (mandatory):

| Table | Purpose |
|-------|---------|
| `{SYSTEM_SCHEMA}.SYSTEM_RESTORE_POINTS` | One row per table per restore point (HFRP####); `is_latest` marks the current head |
| `{SYSTEM_SCHEMA}.SYSTEM_RESTORE_AUDIT` | Restore request audit trail |

Restore points respect **`global_vacuum_hours`** — expired or vacuumed versions are excluded.

---

## Config

```ini
[DEFAULT]
global_vacuum_hours=168
system_schema=system_admin
```

Unity Catalog: `system_schema=my_catalog.system_admin`

---

## API

```python
from handuflow import (
    run,
    load_config,
    create_restore_point,
    list_restore_points,
    get_restore_point_details,
    initiate_restore,
)

cfg = load_config("/path/to/handuflow_dir/config.ini")

# Run pipeline — master specs are validated and returned on the result
result = run(spark, config=cfg)
print(result.status)

# Snapshot using master_specs.xlsx from config.ini
rp_id = create_restore_point(
    spark, cfg, created_by="ops@corp"
)

# List / inspect
print(list_restore_points(spark, cfg))
print(get_restore_point_details(spark, cfg, rp_id))

# Roll back all target + staging tables to that point
request_id = initiate_restore(
    spark, cfg, rp_id, requested_by="ops@corp"
)
```

### Restore workflow

1. Compare current target/staging Delta versions to the newest restore point
2. If all versions match, return the existing restore point ID (no new row written)
3. Otherwise capture versions, append a new `HFRP####` record, and return the new ID

### Manual restore workflow

1. Audit row `REQUESTED`
2. Validate restore point (all target tables, versions in history, within retention)
3. **Capture a new pre-restore snapshot** (new `HFRP####` rows, marked `is_latest=true`)
4. Audit `IN_PROGRESS`
5. Per table: `RESTORE TABLE … TO VERSION AS OF <version>`
6. Audit `COMPLETED` or `FAILED` (stops on first table failure)

### `is_latest` column

Exactly one restore point ID is marked `is_latest=true` at a time (all rows for that ID).

- Set when a new restore point is created
- Updated when an unchanged snapshot is reused via `create_restore_point`
- A new restore point is **always** written when `initiate_restore` runs (pre-restore backup)

Query the latest point:

```python
from handuflow import get_latest_restore_point_id

print(get_latest_restore_point_id(spark, cfg))
```

Or in SQL:

```sql
SELECT * FROM system_admin.SYSTEM_RESTORE_POINTS WHERE is_latest = true;
```

---

## Restore point ID

Sequential: `HFRP0001`, `HFRP0002`, …

Implementation: `src/handuflow/system_restore/restore.py`
