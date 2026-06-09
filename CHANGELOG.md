# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

---

## [Unreleased]
## [0.0.3] - 2026-06-09

### Added
-

### Changed
-

### Fixed
Anoterh release test

## [0.0.2] - 2026-06-09

### Added
-

### Changed
-

### Fixed
release check

## [0.0.1] - 2026-06-09

### Added
- Initial public release of **HanduFlow** (`handuflow`) from [handuflow-core](https://github.com/hhandoo/handuflow-core).
- Spark + Delta Lake orchestration for medallion pipelines: validate specs, bronze ingest, pre/post-load data quality, medallion loads, Excel reporting, and lineage diagrams.
- Excel-driven configuration via `master_specs.xlsx` and JSON feed specs (`files_dev/` template included).
- Load types: `FULL_LOAD`, `APPEND_LOAD`, `INCREMENTAL_CDC`, `SCD_TYPE_2`, `API_EXTRACTOR`, and `STORAGE_FETCH`.
- Data quality runners with standard and comprehensive checks, gating, and Excel DQ reports.
- Delta Lake system restore points (`HFRP####`) with global restore and audit tables.
- System cleanup: log/outbound retention and Delta vacuum.
- Public Python API: `handuflow.run()`, `Orchestrator`, `RunResult`, `RunStatus`, `CatalogResolver`, `load_config`, and typed errors (`HF###` codes).
- Local (Hive) and Databricks Unity Catalog support via `runtime_mode` in `config.ini`.
- Regression and enterprise E2E test harnesses; CI on push/PR and PyPI publish on version tags.
- Documentation under `documentation/` (setup, config, API, modules, deployment, error codes).

### Changed
- Fresh versioning baseline at `0.0.1` for the `handuflow-core` repository.

### Fixed
- N/A (initial release).

