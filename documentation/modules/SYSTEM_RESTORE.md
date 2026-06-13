# `system_restore/` package

Delta version-based global restore points with metadata and audit tables.

User guide: [SYSTEM_RESTORE.md](../SYSTEM_RESTORE.md)

---

## `system_restore/__init__.py`

| | |
|---|---|
| **Visibility** | Public |
| **Exports** | `SystemRestore`, `create_restore_point`, `list_restore_points`, `get_restore_point_details`, `initiate_restore` |

---

## `system_restore/restore.py`

| | |
|---|---|
| **Visibility** | Public |
| **Purpose** | Create, list, inspect, and execute Delta restore points. |

### Constants

| Name | Description |
|------|-------------|
| `RESTORE_POINTS_TABLE` | `{SYSTEM_SCHEMA}.SYSTEM_RESTORE_POINTS` |
| `RESTORE_AUDIT_TABLE` | `{SYSTEM_SCHEMA}.SYSTEM_RESTORE_AUDIT` |
| Restore point IDs | `HFRP0001`, `HFRP0002`, … (auto-increment) |

### Class: `SystemRestore`

| Method | Description |
|--------|-------------|
| `create_restore_point(description)` | Snapshot target and staging Delta tables; return restore point ID |
| `list_restore_points()` | List all restore points with metadata |
| `get_restore_point_details(restore_point_id)` | Per-table version map for a point |
| `initiate_restore(restore_point_id)` | Restore all tables to recorded versions |

### Module-level functions

Convenience wrappers that construct `SystemRestore` from `spark` + `config`:

| Function | Description |
|----------|-------------|
| `create_restore_point(spark, config, created_by)` | Create restore point (specs from config) |
| `list_restore_points(spark, config)` | List restore points |
| `get_restore_point_details(spark, config, restore_point_id)` | Get details |
| `initiate_restore(spark, config, restore_point_id, requested_by)` | Execute restore |

Master specs are always loaded from `{file_hunt_path}/{master_spec_name}` via `config.ini`.

Also re-exported from `handuflow` root API.

### Metadata tables

| Table | Columns (key) |
|-------|---------------|
| `SYSTEM_RESTORE_POINTS` | `restore_point_id`, `description`, `created_at`, `table_count`, `status` |
| `SYSTEM_RESTORE_AUDIT` | `restore_point_id`, `table_name`, `version_before`, `version_after`, `restored_at` |

### Behavior

1. Enumerate target and staging tables via `system_shared.spec_tables.collect_restore_point_table_entries`
2. Record current Delta version per table (`DESCRIBE HISTORY`)
3. On restore: `RESTORE TABLE ... TO VERSION AS OF <n>` per table
4. Write audit row for each table restored

Raises `SystemError` (HF099) on failure. Respects `GLOBAL_VACUUM_HOURS` — will not restore versions vacuumed past retention.

**Dependencies:** `config.catalog_resolver`, `config.config_paths`, `config.run_logger`, `exception.system_error`, `system_shared.*`

**Config:** `SYSTEM_SCHEMA` (mandatory). See [CONFIG.md](../CONFIG.md).
