# `result_generator/` package

Multi-sheet Excel run report generation.

---

## `result_generator/result_generator.py`

| | |
|---|---|
| **Visibility** | Internal |
| **Purpose** | Build Excel run report with readiness, loads, DQ, and dashboard sheets. |

### Class: `ResultGenerator`

| Method | Description |
|--------|-------------|
| `run(validated_specs, load_results, dq_manifest, phase_errors)` | Write report to configured output path |

### Report sheets (internal builders)

| Sheet | Contents |
|-------|----------|
| Readiness | Validation summary, active feed count |
| Load results | Per-feed status, duration, row counts, error codes |
| Data quality | DQ check outcomes by feed |
| Dashboard | Aggregate pass/fail metrics |

Output path comes from `config.ini` (`system_run_report_path` or equivalent via `cfg_get`).

Raises `ResultGenerationException` (HF080) on write failure.

**Dependencies:** `config.config_paths`, `config.run_logger`, `exception.*`, `data_movement_controller.data_class.load_result`

**Called by:** `Orchestrator._finalize_run()`
