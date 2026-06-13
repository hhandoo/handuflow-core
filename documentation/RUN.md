# How to run HanduFlow

Quick reference for triggering the pipeline, local dev runs, tests, and system restore.

**Prerequisites:** Python 3.10+, Java 11+, `pip install -e ".[spark]"`, and `files_dev/config.ini` paths matching your checkout.  
Full setup: [SETUP.md](SETUP.md) · Config keys: [CONFIG.md](CONFIG.md)

---

## At a glance

| What you want | Command / entry point | Duration |
|---------------|----------------------|----------|
| **Production pipeline** | `handuflow.run(spark, config_path=...)` | Minutes–hours (data size) |
| **Local dev (repo template)** | `python scripts/run_local_orchestrator.py` | Minutes |
| **Regression tests (CI)** | `pytest tests/regression -v` | ~1–15 min |
| **E2E smoke** | `python -m tests.e2e.run_e2e --smoke` | ~1 min |
| **E2E quick** | `python -m tests.e2e.run_e2e --quick` | ~30–120 min |
| **E2E full / heavy / extreme** | `--full` / `--heavy` / `--extreme` | Hours |
| **System restore (manual)** | `create_restore_point` / `initiate_restore` | Minutes |

---

## 1. Trigger the pipeline (recommended)

HanduFlow does **not** start Spark for you. Create a session, then call the public API.

### One-liner

```python
from pyspark.sql import SparkSession
from handuflow import run

spark = (
    SparkSession.builder.appName("HanduFlow")
    .enableHiveSupport()
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0,com.databricks:spark-xml_2.12:0.17.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

result = run(spark, config_path="/path/to/handuflow_dir/config.ini")

print(result.status)          # COMPLETED | COMPLETED_WITH_ERRORS | VALIDATION_FAILED | FAILED
print(result.succeeded)       # True if run finished (with or without feed errors)
print(result.load_results)    # per-feed outcomes
print(result.phase_errors)    # structured errors with HF### codes
```

### Explicit orchestrator

```python
import configparser
from handuflow import Orchestrator, load_config

config = load_config("/path/to/handuflow_dir/config.ini")
result = Orchestrator(spark, config=config).run()
```

### What runs automatically

```text
Validate config.ini + master_specs.xlsx + feed_specs JSON
  → Bronze ingest (SOURCE_TO_BRONZE feeds)
  → Pre-load data quality (gates medallion loads)
  → Medallion loads (parallel groups from master_specs)
  → Post-load data quality (report only)
  → Excel run report + lineage PNG
  → System cleanup (retention DELETE, OPTIMIZE, VACUUM per global_vacuum_hours)
  → Log archive to handuflow_logs/
```

Outputs: [SETUP.md §9](SETUP.md#9-verify-outputs)

### Databricks

```python
from pyspark.sql import SparkSession
from handuflow import run

spark = SparkSession.builder.getOrCreate()
result = run(spark, config_path="/dbfs/path/to/handuflow_dir/config.ini")
```

Set `[PLATFORM] runtime_mode=unity_catalog` and `system_schema` to your catalog schema. See [CONFIG.md](CONFIG.md).

---

## 2. Trigger local dev run (this repo)

Uses **`files_dev/config.ini`** and seeds `demo.test` for feeds defined in `files_dev/master_specs.xlsx`.

```bash
cd /path/to/handuflow-core
source .venv/bin/activate
pip install -e ".[spark]"

# Edit paths in files_dev/config.ini if your checkout is not /home/.../handuflow-core/files_dev
python scripts/run_local_orchestrator.py
```

This script:

1. Loads `files_dev/config.ini`
2. Starts a local Spark session (4g driver/executor)
3. Creates `demo`, `staging`, `silver` databases
4. Overwrites `demo.test` with 100 synthetic rows + one insert
5. Calls `Orchestrator(spark, config).run()`

**When to use:** quick manual validation after changing feeds or library code.  
**When not to use:** automated QA — use regression or E2E instead.

---

## 3. Trigger regression tests

Fast pytest suite (unit + Spark integration). No `master_specs.xlsx` required for most tests.

```bash
pip install -e ".[dev]"

# All regression tests
pytest tests/regression -v

# Integration only (Spark + Delta)
pytest tests/regression -m integration -v

# Fast unit subset (no Spark)
pytest tests/regression/test_load_integrity_unit.py -v
pytest tests/regression/test_feed_spec_validation.py -v
pytest tests/regression/test_global_vacuum_hours_config.py -v
```

**Covers:** load types, DQ gating, audit columns, config validation, system cleanup, system restore.

Layout: [tests/README.md](../tests/README.md)

---

## 4. Trigger E2E tests

Enterprise QA harness against the real `LoadDispatcher` and validators. Uses `files_dev/config.ini` via `tests/e2e/spark_setup.py`.

```bash
cd /path/to/handuflow-core
pip install -e ".[spark]"

# Smoke — 4 tests, all load types, vacuum + restore (~1 min)
PYTHONUNBUFFERED=1 .venv/bin/python -m tests.e2e.run_e2e --smoke

# Quick — ~144 tests, up to 10k rows
PYTHONUNBUFFERED=1 .venv/bin/python -m tests.e2e.run_e2e --quick

# Full — up to 100k rows
PYTHONUNBUFFERED=1 .venv/bin/python -m tests.e2e.run_e2e --full

# Heavy — up to 1M rows
PYTHONUNBUFFERED=1 .venv/bin/python -m tests.e2e.run_e2e --heavy

# Extreme — 1M–10M rows, 24×7 scenarios
PYTHONUNBUFFERED=1 .venv/bin/python -m tests.e2e.run_e2e --extreme
```

### Monitor progress

```bash
tail -f tests/e2e/e2e_progress.log
```

### Results

| Artifact | Path |
|----------|------|
| Excel report | `tests/e2e/test_results.xlsx` (gitignored, written after each test) |
| Progress log | `tests/e2e/e2e_progress.log` |

E2E also runs `pytest tests/regression` at the end. Exit code is non-zero if either E2E or regression fails.

Details: [tests/e2e/README.md](../tests/e2e/README.md)

---

## 5. Trigger system restore (manual ops)

Restore is **not** called automatically by `Orchestrator.run()` — invoke it explicitly when needed.

**Requires:** `system_schema` in `config.ini` (e.g. `system_admin`).

```python
import configparser
from handuflow import (
    run,
    load_config,
    create_restore_point,
    list_restore_points,
    get_restore_point_details,
    initiate_restore,
)

cfg = load_config("/path/to/handuflow_dir/config.ini")

# Run pipeline; validated master specs are on the result
result = run(spark, config=cfg)
print(result.status)

# Snapshot current Delta versions (target + staging tables)
rp_id = create_restore_point(
    spark, cfg, created_by="ops@corp"
)
print(rp_id)  # HFRP0001, HFRP0002, ...

# List / inspect (master specs always read from config.ini)
print(list_restore_points(spark, cfg))
print(get_restore_point_details(spark, cfg, rp_id))

# Roll back all tables to that point
request_id = initiate_restore(
    spark, cfg, rp_id, requested_by="ops@corp"
)
```

Full guide: [SYSTEM_RESTORE.md](SYSTEM_RESTORE.md)

---

## 6. Config checklist before any run

Ensure `config.ini` has:

| Key | Example | Required |
|-----|---------|----------|
| `file_hunt_path` | `/path/to/handuflow_dir` | Yes |
| `temp_log_location` | `.../temp` | Yes |
| `system_schema` | `system_admin` | Yes |
| `global_vacuum_hours` | `168` (168–8760) | No (default 168) |
| `[PLATFORM] runtime_mode` | `local` or `unity_catalog` | No (default local) |
| `[FILES] master_spec_name` | `master_specs.xlsx` | Yes |

Validate without running:

```python
from handuflow import load_config, validate_handuflow_config

cfg = load_config("/path/to/handuflow_dir/config.ini", check_paths_exist=True)
validate_handuflow_config(cfg, check_paths_exist=True)
```

---

## 7. Troubleshooting

| Symptom | Check |
|---------|--------|
| `ConfigError HF020/HF021` | Paths in `config.ini`, `master_specs.xlsx` exists |
| `ValidationError HF001–HF013` | Excel columns, feed_specs JSON — [ERROR_CODES.md](ERROR_CODES.md) |
| Spark / Java errors | `java -version`, JAR packages on first run |
| E2E uses wrong config | `tests/e2e/spark_setup.py` → `files_dev/config.ini` |
| Restore fails | `system_schema` DB exists; versions not vacuumed past `global_vacuum_hours` |

---

## Related docs

| Doc | Topic |
|-----|--------|
| [SETUP.md](SETUP.md) | First-time install and folder layout |
| [API.md](API.md) | Public Python API |
| [CONFIG.md](CONFIG.md) | Every config.ini key |
| [tests/README.md](../tests/README.md) | Test layout |
| [SYSTEM_RESTORE.md](SYSTEM_RESTORE.md) | Restore points (HFRP####) |
