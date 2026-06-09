# `data_quality/` package

Per-feed data quality: standard column checks, comprehensive SQL checks, Excel reporting.

See also: [DATA_QUALITY.md](../DATA_QUALITY.md) (user guide for check types and feed JSON).

---

## `data_quality/runner/feed_data_quality_runner.py`

| | |
|---|---|
| **Visibility** | Internal |
| **Purpose** | Per-feed DQ orchestration across pipeline stages. |

### Constants

| Name | Description |
|------|-------------|
| `PRE_LOAD_STAGE` | Checks that gate ingest (failure blocks load) |
| `POST_LOAD_STAGE` | Report-only checks after load completes |

### Class: `FeedDataQualityRunner`

| Method | Description |
|--------|-------------|
| `run(feed_row, stage)` | Execute DQ for one feed at given stage |
| `run_post_load_checks(...)` | Post-load comprehensive checks |
| `finalize()` | Collect summary rows for Excel report |

Delegates to `StandardDQExecutor` and `ComprehensiveDQExecutor`. Errors wrapped via `error_handler`.

**Dependencies:** `config.run_logger`, `executors.*`, `exception.error_handler`

---

## `data_quality/executors/standard_dq_executor.py`

| | |
|---|---|
| **Visibility** | Internal |
| **Purpose** | Column-level standard checks from `standard_checks` in feed JSON. |

### Class: `StandardDQExecutor`

| Method | Description |
|--------|-------------|
| `run_check(check_def, df)` | Dispatch by check type |

Check types: null count/rate, value range, allowed values, duplicate detection, primary-key uniqueness. Raises `DataQualityException` (HF073) on threshold breach.

**Dependencies:** `exception.data_quality_exception`

---

## `data_quality/executors/comprehensive_dq_executor.py`

| | |
|---|---|
| **Visibility** | Internal |
| **Purpose** | SQL-based comprehensive checks with PRE_LOAD / POST_LOAD staging. |

### Class: `ComprehensiveDQExecutor`

| Method | Description |
|--------|-------------|
| `run(check_def, spark, ...)` | Execute SQL check against dependency datasets |

Constants: `PRE_LOAD`, `POST_LOAD` — determine whether failure blocks ingest.

**Dependencies:** `exception.data_quality_exception`

---

## `data_quality/model/feed_dq_summary_row.py`

| | |
|---|---|
| **Visibility** | Internal |
| **Purpose** | Dataclass row for DQ Excel summary sheet. |

### `FeedDQSummaryRow`

Fields: `feed_id`, check name, stage, pass/fail, actual vs threshold values, timestamp.

**Dependencies:** None

---

## `data_quality/report/dq_excel_report_writer.py`

| | |
|---|---|
| **Visibility** | Internal |
| **Purpose** | Write combined DQ results to an Excel file. |

### Class: `DQExcelReportWriter`

| Method | Description |
|--------|-------------|
| `write(path, summary_rows, detail_rows)` | Static: create DQ report workbook |

**Dependencies:** None (uses `openpyxl`)
