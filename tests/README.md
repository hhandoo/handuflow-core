# Tests

**How to trigger everything** (pipeline + tests + local script): [documentation/RUN.md](../documentation/RUN.md)

## Layout

```text
tests/
├── conftest.py          # Shared pytest fixtures (Spark session)
├── helpers/             # Shared utilities
│   └── spark_isolation.py
├── regression/          # Pytest — unit + integration (CI-friendly)
└── e2e/                 # Long-running Spark E2E harness
    ├── README.md
    └── run_e2e.py
```

Feed JSON examples live under `documentation/examples/` (not duplicated here).

## Regression (pytest)

```bash
pip install -e ".[dev]"

# All regression tests
pytest tests/regression -v

# Integration only (Spark)
pytest tests/regression -m integration -v

# Fast unit subset
pytest tests/regression/test_load_integrity_unit.py -v
```

## E2E (Spark, long-running)

See [e2e/README.md](e2e/README.md).

```bash
PYTHONUNBUFFERED=1 .venv/bin/python -m tests.e2e.run_e2e --smoke
PYTHONUNBUFFERED=1 .venv/bin/python -m tests.e2e.run_e2e --quick
```

Reports: `tests/e2e/test_results.xlsx` (generated, gitignored).

## Manual local orchestrator run

```bash
python scripts/run_local_orchestrator.py
```

Uses `files_dev/config.ini` and seeds `demo.test` for configured feeds.
