# `config/` package

Configuration loading, validation, catalog naming, logging, and runtime detection.

**Import:** `from handuflow.config import CatalogResolver, load_config, ...`

---

## `config/__init__.py`

| | |
|---|---|
| **Visibility** | Semi-public |
| **Purpose** | Subpackage public exports. |

**Exports:** `CatalogResolver`, `cfg_get`, `cfg_get_int`, `dmc_temp_dir`, `global_vacuum_hours`, `system_schema`, `runtime_mode`, `GLOBAL_VACUUM_HOURS_*`, `is_databricks_runtime`, `validate_handuflow_config`, `load_config`

---

## `config/catalog_resolver.py`

| | |
|---|---|
| **Visibility** | Public (via root) |
| **Purpose** | Resolve Hive `schema.table` vs Unity Catalog `catalog.schema.table` identifiers. |

### Class: `CatalogResolver`

| Method / property | Description |
|-------------------|-------------|
| `is_local` | `True` when `runtime_mode` is `local` |
| `qualified_table(schema, table)` | Full table name for current runtime |
| `bronze_schema(feed_specs)` | Bronze schema from feed JSON |
| `staging_schema(feed_specs)` | Staging schema for medallion loads |
| `target_table(feed_specs)` | Target table qualified name |

**Dependencies:** `config.config_paths.runtime_mode`

---

## `config/config_paths.py`

| | |
|---|---|
| **Visibility** | Semi-public |
| **Purpose** | Typed accessors for `config.ini` keys. |

### Constants

| Name | Description |
|------|-------------|
| `GLOBAL_VACUUM_HOURS_DEFAULT` | `168` (7 days) |
| `GLOBAL_VACUUM_HOURS_MIN` | `168` |
| `GLOBAL_VACUUM_HOURS_MAX` | `8760` (1 year) |

### Functions

| Name | Description |
|------|-------------|
| `cfg_get(config, key, section=None)` | Read string config value |
| `cfg_get_int(config, key, section=None)` | Read integer config value |
| `runtime_mode(config)` | `"local"` or `"databricks"` |
| `dmc_temp_dir(config)` | Temp directory for data movement |
| `system_schema(config)` | Schema for system tables (restore, audit) |
| `global_vacuum_hours(config)` | Retention window for Delta vacuum |

**Dependencies:** None

---

## `config/load_config.py`

| | |
|---|---|
| **Visibility** | Public (via root) |
| **Purpose** | Load `config.ini` from disk with optional path validation. |

### `load_config(path, *, check_paths_exist=True) -> ConfigParser`

Raises `ConfigError` (HF020) on missing file or invalid paths.

**Dependencies:** `config.validate`, `exception.config_error`

---

## `config/validate.py`

| | |
|---|---|
| **Visibility** | Public (via root) |
| **Purpose** | Startup validation of required config sections, keys, paths, and vacuum hours. |

### `validate_handuflow_config(config, *, check_paths_exist=True) -> None`

Validates:

- Required sections and keys (`file_hunt_path`, output paths, `SYSTEM_SCHEMA`, `GLOBAL_VACUUM_HOURS`, etc.)
- `GLOBAL_VACUUM_HOURS` in range 168–8760
- Directory existence when `check_paths_exist=True`

**Dependencies:** `config.config_paths`, `exception.config_error`

---

## `config/spark_session.py`

| | |
|---|---|
| **Visibility** | Public (via root) |
| **Purpose** | Detect Databricks runtime environment. |

### `is_databricks_runtime() -> bool`

Returns `True` when `DATABRICKS_RUNTIME_VERSION` is set.

**Dependencies:** None

---

## `config/logging_config.py`

| | |
|---|---|
| **Visibility** | Internal |
| **Purpose** | Per-run logging: console + rotating file, summary writer. |

### Class: `LoggingConfig`

| Method | Description |
|--------|-------------|
| `configure()` | Set up handlers for the `handuflow` logger |
| `move_logs_to_final_location()` | Archive logs to configured output path |
| `write_run_summary(result)` | Write text summary alongside logs |

Constructed with `run_id` and parsed `config`. Used by `Orchestrator` for every batch run.

**Dependencies:** `config.config_paths.cfg_get`

---

## `config/logging_pretty_formatter.py`

| | |
|---|---|
| **Visibility** | Internal |
| **Purpose** | ANSI color formatter for terminal log output. |

### Class: `LoggingPrettyFormatter`

Extends `logging.Formatter` with level-based colors for local development.

**Dependencies:** None

---

## `config/run_logger.py`

| | |
|---|---|
| **Visibility** | Internal |
| **Purpose** | Structured `[STEP]` logging for pipeline phases and feeds. |

| Name | Description |
|------|-------------|
| `fmt_ctx(**kwargs)` | Format key=value context string |
| `log_step(logger, step, status, **ctx)` | Log a pipeline step event |
| `step(logger, name, **ctx)` | Context manager for timed steps |
| `log_feed_event(logger, feed_id, event, **ctx)` | Per-feed lifecycle logging |

**Dependencies:** None
