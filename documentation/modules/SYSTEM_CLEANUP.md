# `system_cleanup/` package

Post-run retention: old logs/outputs, Delta row deletion, OPTIMIZE, and VACUUM.

---

## `system_cleanup/__init__.py`

| | |
|---|---|
| **Visibility** | Semi-public |
| **Exports** | `SystemCleanup` |

---

## `system_cleanup/cleanup.py`

| | |
|---|---|
| **Visibility** | Semi-public |
| **Purpose** | Retention cleanup driven by `GLOBAL_VACUUM_HOURS` from config. |

### Class: `SystemCleanup`

| Method | Description |
|--------|-------------|
| `run(validated_master_specs)` | Execute full cleanup pass |

### Cleanup operations

1. **Log/output retention** — remove files older than vacuum window from configured directories
2. **Delta row deletion** — `DELETE` rows where `_x_last_modification_timestamp` or `_x_commit_timestamp` is older than retention cutoff
3. **OPTIMIZE** — compact Delta files on all master-spec source/target tables (when `is_auto_vacuum_enabled=true`)
4. **VACUUM** — remove obsolete files with `retentionHours` = `GLOBAL_VACUUM_HOURS` (when `is_auto_vacuum_enabled=true`)

### Table discovery

Uses `system_shared.spec_tables.collect_cleanup_table_entries()` to enumerate source, target, and staging Delta tables from validated master specs.

### Delta detection

Uses `system_shared.delta_utils.is_delta_table()` (via `DESCRIBE DETAIL`) for compatibility with local Hive metastore.

**Dependencies:** `config.config_paths`, `config.run_logger`, `system_shared.delta_utils`, `system_shared.spec_tables`

**Called by:** `Orchestrator._finalize_run()` in `finally` block

**Config:** `global_vacuum_hours` (168–8760, default 168), `is_auto_vacuum_enabled` (default `true`). See [CONFIG.md](../CONFIG.md).
