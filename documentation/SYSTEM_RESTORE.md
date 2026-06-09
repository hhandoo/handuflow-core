# System Restore

Delta Lake version-based global restore points for all master-spec source and target tables.

Metadata lives under **`[DEFAULT] system_schema`** (mandatory):

| Table | Purpose |
|-------|---------|
| `{SYSTEM_SCHEMA}.SYSTEM_RESTORE_POINTS` | One row per table per restore point (HFRP####) |
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
    create_restore_point,
    list_restore_points,
    get_restore_point_details,
    initiate_restore,
)

# After a successful pipeline run
rp_id = create_restore_point(spark, cfg, master_specs_df, created_by="ops@corp")

# List valid restore points
points = list_restore_points(spark, cfg, master_specs_df)

# Inspect
details = get_restore_point_details(spark, cfg, master_specs_df, "HFRP0001")

# Full system rollback
request_id = initiate_restore(spark, cfg, master_specs_df, "HFRP0001", requested_by="ops@corp")
```

### Restore workflow

1. Audit row `REQUESTED`
2. Validate restore point (all tables, versions in history, within retention)
3. Audit `IN_PROGRESS`
4. Per table: `RESTORE TABLE … TO VERSION AS OF <version>`
5. Audit `COMPLETED` or `FAILED` (stops on first table failure)

---

## Restore point ID

Sequential: `HFRP0001`, `HFRP0002`, …

Implementation: `src/handuflow/system_restore/restore.py`
