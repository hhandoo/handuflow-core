# config.ini reference

Single INI file per environment. Parsed with Python `configparser`.

---

## Path layout

```text
file_hunt_path/                    ← [DEFAULT] file_hunt_path
├── config.ini
├── master_specs.xlsx              ← [FILES] master_spec_name
├── handuflow_outbound/            ← [DEFAULT] outbound_directory_name
│   ├── results_<run_id>_*.xlsx
│   └── feed_lineage_<run_id>.png
└── handuflow_logs/                ← archived logs

temp_log_location/                 ← active logs during run
└── handuflow_logs/
    └── handuflow_log_<run_id>_*.log

DMC_CONFIG temp/                   ← API parquet downloads
```

---

## `[DEFAULT]`

| Key | Required | Description |
|-----|----------|-------------|
| `file_hunt_path` | Yes | Root directory (specs + outbound + archived logs) |
| `outbound_directory_name` | Yes | Subfolder under `file_hunt_path` for reports |
| `log_directory_name` | Yes | Log subfolder name (temp + archive) |
| `temp_log_location` | Yes* | Scratch for active logs. *Fallback: `[DMC_CONFIG] temp` |
| `log_retention_policy_in_days` | No | Delete old logs/reports (default `7`) |
| `max_concurrent_batches` | No | Max parallel feeds per group (default `4`) |
| `global_vacuum_hours` | No | Delta row retention + `VACUUM RETAIN` hours for all master-spec source/target tables (default `168`, min `168`, max `8760`) |
| `system_schema` | **Yes** | Schema for system metadata (`SYSTEM_RESTORE_POINTS`, `SYSTEM_RESTORE_AUDIT`). On Unity Catalog use `catalog.schema` or set feeds' catalog + `system_admin` |

**Legacy aliases:** `GLOBAL_VACUUM_HOURS`, `SYSTEM_SCHEMA` (still read if lowercase keys are absent).

**Legacy aliases:** `retention_policy_in_days`, `temp_location`, `temp_directory`.

---

## `[PLATFORM]`

| Key | Required | Values |
|-----|----------|--------|
| `runtime_mode` | No | `local` (default) → `schema.table` |
| | | `unity_catalog` / `uc` / `databricks` → `catalog.schema.table` |

Match `target_unity_catalog` in master specs (`local` locally, real catalog on Databricks).

---

## `[DMC_CONFIG]`

| Key | Required | Description |
|-----|----------|-------------|
| `temp` | Yes* | Writable scratch (API parquet). *Fallback: `temp_log_location` |

---

## `[FILES]`

| Key | Required | Description |
|-----|----------|-------------|
| `master_spec_name` | Yes | Excel filename under `file_hunt_path` (default `master_specs.xlsx`) |

---

## `[LINEAGE_DIAGRAM]`

| Key | Required | Description |
|-----|----------|-------------|
| `BOX_WIDTH` | Yes | Feed box width |
| `BOX_HEIGHT` | Yes | Feed box height |
| `X_GAP` | Yes | Horizontal gap |
| `Y_GAP` | Yes | Vertical gap |
| `ROOT_GAP` | Yes | Root feed spacing |

---

## `[LOGGING]` (optional legacy)

Use `[DEFAULT]` for new configs. If present: `log_base_path`, `temp`, `log_directory_name` map to same roles as above.

---

## Local example

```ini
[DEFAULT]
file_hunt_path=/home/user/handuflow_dir
outbound_directory_name=handuflow_outbound
log_directory_name=handuflow_logs
temp_log_location=/home/user/handuflow_dir/temp
log_retention_policy_in_days=7
max_concurrent_batches=4
global_vacuum_hours=168
system_schema=system_admin

[PLATFORM]
runtime_mode=local

[DMC_CONFIG]
temp=/home/user/handuflow_dir/dmc_temp

[FILES]
master_spec_name=master_specs.xlsx

[LINEAGE_DIAGRAM]
BOX_WIDTH=4.4
BOX_HEIGHT=2.2
X_GAP=2.0
Y_GAP=2.5
ROOT_GAP=2.0
```

## Databricks example

```ini
[DEFAULT]
file_hunt_path=/Volumes/my_catalog/config/handuflow
outbound_directory_name=handuflow_outbound
log_directory_name=handuflow_logs
temp_log_location=/tmp/handuflow
max_concurrent_batches=8
global_vacuum_hours=168
system_schema=my_catalog.system_admin

[PLATFORM]
runtime_mode=unity_catalog

[DMC_CONFIG]
temp=/Volumes/my_catalog/config/handuflow/dmc_temp

[FILES]
master_spec_name=master_specs.xlsx

[LINEAGE_DIAGRAM]
BOX_WIDTH=4.4
BOX_HEIGHT=2.2
X_GAP=2.0
Y_GAP=2.5
ROOT_GAP=2.0
```

Templates: `files_dev/config.ini` (local), `files_prod/config.ini` (Databricks / Unity Catalog).

Release & versioning: [DEPLOYMENT.md](DEPLOYMENT.md)
