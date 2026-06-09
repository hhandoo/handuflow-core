# HanduFlow

Spark + Delta Lake orchestration for medallion pipelines: bronze ingest, data quality, CDC, SCD Type 2, lineage, and Excel reporting — driven by `master_specs.xlsx` and JSON feed specs.

Works locally (Hive) and on Databricks Unity Catalog with the same code.

---

## Install

```bash
pip install handuflow
# or from source
pip install -e .
```

Requires **Java 11+**, **Python 3.10+**, and a **Spark session you provide**.

---

## Usage

```python
from pyspark.sql import SparkSession
from handuflow import run

spark = SparkSession.builder.appName("HanduFlow").enableHiveSupport().getOrCreate()
result = run(spark, config_path="/path/to/handuflow_dir/config.ini")

print(result.status)       # COMPLETED | COMPLETED_WITH_ERRORS | ...
print(result.load_results)   # per-feed outcomes
```

Full API: **[documentation/API.md](documentation/API.md)**

```python
from handuflow import (
    Orchestrator,
    RunResult,
    RunStatus,
    load_config,
    CatalogResolver,
    ConfigError,
    SOURCE_TO_BRONZE,
)
```

---

## Setup

1. Create `handuflow_dir/` with `config.ini`, `master_specs.xlsx`, and output folders  
2. Define feeds in Excel + JSON  
3. Run `handuflow.run(spark, config_path=...)`

**Step-by-step:** [documentation/SETUP.md](documentation/SETUP.md)  
**Template:** `files_dev/`

---

## Documentation

| Doc | Contents |
|-----|----------|
| [RUN.md](documentation/RUN.md) | **How to trigger** pipeline, tests, local dev, restore |
| [DEPLOYMENT.md](documentation/DEPLOYMENT.md) | Versioning, PyPI release, CI/CD, prod config |
| [API.md](documentation/API.md) | Public Python API |
| [MODULES.md](documentation/MODULES.md) | Every file & module reference (74 modules) |
| [SETUP.md](documentation/SETUP.md) | Install, folders, config, run |
| [CONFIG.md](documentation/CONFIG.md) | `config.ini` reference |
| [MASTER_SPECS.md](documentation/MASTER_SPECS.md) | Excel columns & load types |
| [FEED_SPECS.md](documentation/FEED_SPECS.md) | Feed JSON keys |
| [DATA_QUALITY.md](documentation/DATA_QUALITY.md) | DQ checks & reports |
| [ERROR_CODES.md](documentation/ERROR_CODES.md) | HF### error codes |
| [SYSTEM_RESTORE.md](documentation/SYSTEM_RESTORE.md) | Delta restore points (HFRP####) |

Examples: `documentation/examples/medallion/`, `documentation/examples/bronze/`

---

## Pipeline

```text
Validate specs → Bronze extract → Pre-load DQ → Medallion loads → Post-load DQ → Excel report + lineage
```

Per-feed failures are isolated; the batch continues. See [DATA_QUALITY.md](documentation/DATA_QUALITY.md).

---

## Databricks

Set `[PLATFORM] runtime_mode=unity_catalog`, store config on Volumes, use real catalog names in master specs. Same `run()` API.

---

## Development

[CONTRIBUTING.md](CONTRIBUTING.md)

```bash
pip install -e ".[dev]"
pytest tests/regression -v
```

Test layout: [tests/README.md](tests/README.md)

---

## License

Apache 2.0 — [LICENSE](LICENSE)
