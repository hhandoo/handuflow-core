# Setup guide

Step-by-step local setup. For Databricks, use the same layout on Volumes/DBFS and set `runtime_mode=unity_catalog`.

---

## 1. Prerequisites

| Requirement | Notes |
|-------------|--------|
| Python 3.10–3.12 | 3.13 works for dev; Spark integration tests target 3.10–3.12 |
| Java 11+ | Required by Spark |
| Git | Clone the repo |

---

## 2. Install

```bash
git clone https://github.com/hhandoo/handuflow-core.git
cd handuflow-core
python -m venv .venv && source .venv/bin/activate
pip install -e .
```

First Spark run downloads Delta + spark-xml JARs (needs network).

---

## 3. Create directory layout

Pick a root path (`handuflow_dir`). Example: `files_dev` in the repo.

```bash
HANDUFLOW_DIR=/path/to/handuflow_dir

mkdir -p "$HANDUFLOW_DIR"/{handuflow_outbound,handuflow_logs,temp,dmc_temp}
```

### Required folders

| Path | Purpose |
|------|---------|
| `{handuflow_dir}/` | Root (`file_hunt_path`) — holds `config.ini` + `master_specs.xlsx` |
| `{handuflow_dir}/handuflow_outbound/` | Run Excel reports + lineage PNG |
| `{handuflow_dir}/handuflow_logs/` | Archived logs (after each run) |
| `{handuflow_dir}/temp/` | Active logs + scratch during run |
| `{handuflow_dir}/dmc_temp/` | API download parquet scratch |

**Template:** copy `files_dev/` from this repo (includes `config.ini` + `.gitkeep` placeholders).

```text
handuflow_dir/
├── config.ini
├── master_specs.xlsx          ← you provide this
├── handuflow_outbound/
├── handuflow_logs/
├── temp/
└── dmc_temp/
```

---

## 4. Configure `config.ini`

Copy `files_dev/config.ini` into `handuflow_dir/` and set paths.

```ini
[DEFAULT]
file_hunt_path=/path/to/handuflow_dir
outbound_directory_name=handuflow_outbound
log_directory_name=handuflow_logs
temp_log_location=/path/to/handuflow_dir/temp
log_retention_policy_in_days=7
max_concurrent_batches=4
global_vacuum_hours=168
system_schema=system_admin

[PLATFORM]
runtime_mode=local

[DMC_CONFIG]
temp=/path/to/handuflow_dir/dmc_temp

[FILES]
master_spec_name=master_specs.xlsx

[LINEAGE_DIAGRAM]
BOX_WIDTH=4.4
BOX_HEIGHT=2.2
X_GAP=2.0
Y_GAP=2.5
ROOT_GAP=2.0
```

All keys explained: [CONFIG.md](CONFIG.md).

---

## 5. Create `master_specs.xlsx`

| Rule | Value |
|------|--------|
| Location | `{file_hunt_path}/master_specs.xlsx` |
| Sheet name | `master_specs` |
| Active rows | `is_active = True` |

**18 required columns** — see [MASTER_SPECS.md](MASTER_SPECS.md).

Minimal medallion row:

| Column | Example |
|--------|---------|
| `feed_id` | `101` |
| `data_flow_direction` | `BRONZE_TO_SILVER` |
| `load_type` | `INCREMENTAL_CDC` |
| `feed_type` | (display label) |
| `target_unity_catalog` | `local` |
| `target_schema_name` | `silver` |
| `target_table_name` | `country_codes` |
| `feed_specs` | JSON string (see step 6) |
| `parallelism_group_number` | `1` |
| `is_active` | `True` |

---

## 6. Define `feed_specs` (JSON per row)

Paste JSON into the Excel `feed_specs` cell (single line recommended).

| Feed type | Doc | Example file |
|-----------|-----|--------------|
| Medallion (silver/gold) | [FEED_SPECS.md](FEED_SPECS.md) | `documentation/examples/medallion/country_codes.json` |
| Bronze ingest | [FEED_SPECS.md](FEED_SPECS.md#bronze-feeds-source_to_bronze) | `documentation/examples/bronze/simple_get_request.json` |

**Medallion — 9 required keys:** `primary_key`, `composite_key`, `partition_keys`, `vacuum_hours`, `source_table_name`, `selection_query`, `selection_schema`, `standard_checks`, `comprehensive_checks`.

**Optional:** `allow_empty_source`, `allow_unmatched_deletes`.

---

## 7. Create Spark session

HanduFlow does **not** create Spark — pass your own session.

| Need | Local | Databricks |
|------|-------|------------|
| Delta | JAR packages + extensions (below) | Cluster runtime |
| Metastore | `.enableHiveSupport()` | `runtime_mode=unity_catalog` |
| XML ingest | spark-xml JAR | Install on cluster |

**Local:**

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("HanduFlow")
    .enableHiveSupport()
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0,com.databricks:spark-xml_2.12:0.17.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
```

**Databricks:** `spark = SparkSession.builder.getOrCreate()`

---

## 8. Run

```python
from handuflow import run

result = run(spark, config_path="/path/to/handuflow_dir/config.ini")
print(result.status)
```

Or use `Orchestrator` directly — see [API.md](API.md).

**All trigger options** (pipeline, local script, pytest, E2E, restore): [RUN.md](RUN.md)

---

## 9. Verify outputs

| Output | Location |
|--------|----------|
| Excel report | `{file_hunt_path}/handuflow_outbound/results_<run_id>_*.xlsx` |
| Lineage PNG | `{file_hunt_path}/handuflow_outbound/feed_lineage_<run_id>.png` |
| Archived log | `{file_hunt_path}/handuflow_logs/handuflow_log_<run_id>_*.log` |

Report sheets: [DATA_QUALITY.md](DATA_QUALITY.md#run-report-excel).

---

## 10. Run pipeline (what happens)

```text
Validate config + master_specs + feed_specs JSON
  → Bronze extract (SOURCE_TO_BRONZE feeds)
  → Pre-load DQ (gates medallion loads)
  → Medallion loads (FULL / APPEND / CDC / SCD2)
  → Post-load DQ (report only)
  → Excel report + lineage + log archive
```

Load behavior: [MASTER_SPECS.md](MASTER_SPECS.md#load-types).

---

## Docs

| Doc | Topic |
|-----|--------|
| [RUN.md](RUN.md) | How to trigger pipeline, tests, restore |
| [API.md](API.md) | Public Python API |
| [CONFIG.md](CONFIG.md) | config.ini |
| [MASTER_SPECS.md](MASTER_SPECS.md) | Excel columns & load types |
| [FEED_SPECS.md](FEED_SPECS.md) | Feed JSON |
| [DATA_QUALITY.md](DATA_QUALITY.md) | DQ checks |
| [ERROR_CODES.md](ERROR_CODES.md) | HF### codes |
