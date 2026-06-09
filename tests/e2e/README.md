# HanduFlow Enterprise E2E QA Harness

24×7 enterprise-grade validation against the real HanduFlow implementation with **100% data-integrity reconciliation** on every test.

## Prerequisites

```bash
cd /path/to/handuflow-core
pip install -e ".[spark]"
```

Config: `files_dev/config.ini` (edit `file_hunt_path` and temp paths for your checkout)

## Run

```bash
# Smoke (4 tests)
PYTHONUNBUFFERED=1 .venv/bin/python -m tests.e2e.run_e2e --smoke

# Quick (~144 tests, up to 10k rows)
PYTHONUNBUFFERED=1 .venv/bin/python -m tests.e2e.run_e2e --quick

# Full (177 tests, up to 100k)
PYTHONUNBUFFERED=1 .venv/bin/python -m tests.e2e.run_e2e --full

# Heavy (314 tests, up to 1M)
PYTHONUNBUFFERED=1 .venv/bin/python -m tests.e2e.run_e2e --heavy

# Extreme — enterprise multi-million (386 tests, 1M–10M rows)
PYTHONUNBUFFERED=1 .venv/bin/python -m tests.e2e.run_e2e --extreme

tail -f tests/e2e/e2e_progress.log
```

## Cluster configuration

Spark auto-detects host resources (`/proc/meminfo`, CPU count) and configures:

| Setting | Behaviour |
|---------|-----------|
| `master` | `local[N]` — all CPU cores |
| Memory | 35–45% of available RAM (driver + executor) |
| AQE | Adaptive execution, skew join, coalesce |
| Delta | `optimizeWrite` + `autoCompact` |
| Parallelism | `cores × 2–4` based on mode |

Logged at startup: `Cluster: cores=… mem=… shuffle=…`

## Data integrity (every test)

| Check | Description |
|-------|-------------|
| `row_count` | Source count = target count |
| `except` | Bidirectional EXCEPT ALL |
| `hash` | SHA-256 business fingerprint |
| `key_coverage` | Every key in source ↔ target |
| `cdc_hash` | Production `AuditColumns.row_hash_expr` (CDC only) |
| `enterprise_integrity` | Staging rules, partitions, SCD history |
| `aggregates` / `nulls` / `duplicates` / `schema` | Statistical + structural |

CDC hash validation uses the **same formula as production** (`non_key` columns, `||` separator) — no false positives.

## Multi-million coverage (`--extreme`)

| Category | Tests | Scales |
|----------|-------|--------|
| Multi-Million | 40 | 1M, 2M, 5M, 10M initial + 10% churn |
| Multi-Million DQ | 28 | All 7 DQ profiles @ 1M rows |
| Enterprise 24×7 | 4 | 5M + 5 churn cycles + partition migration + full DQ |
| Large Scale | 24 | 100k – 1M |

Source mutations at ≥10k rows use **Spark-native hash predicates** (no driver `collect()`).

## Load-type suites

- **Full Load** — exact source mirror; `t_full` only staging
- **Incremental CDC** — 50% churn @ 100k, multi-cycle, CDC hash + staging
- **Append Load** — insert-only behaviour, consecutive append rounds
- **SCD Type 2** — version history, partition migration + updates

## System vacuum & restore

Every mode exercises `global_vacuum_hours` cleanup and multi-point Delta restore (`HFRP####`) **per load type**:

| Mode | Vacuum hours | Restore scenarios | Notes |
|------|--------------|-----------------|-------|
| Smoke | 168 | Single restore | Combined with initial 100-row load |
| Quick | 168, 720, 8760 | All six restore chains + combined vacuum/restore | 1k rows per load type |
| Full | 168, 720, 8760 | All six restore chains | + restore after 20% changes |
| Heavy | 168, 720, 8760 | All scenarios | + stale-row vacuum, multi-cycle restore |
| Extreme | 168, 720, 8760 | All scenarios | + 100k enterprise vacuum + triple restore |

Validation after restore compares target to snapshot at the chosen restore point. Vacuum tests optionally inject expired `_x_last_modification_timestamp` rows and assert retention deletion.

## Output

- Excel: `tests/e2e/test_results.xlsx`
- Columns: `#`, `Parent Test Case`, `Test Case`, `source_row_count`, `target_row_count`, `cluster_config`, DQ fields, staging counts

## Isolation

Every test drops all warehouse dirs + catalog entries before running.
