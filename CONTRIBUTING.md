# Contributing to HanduFlow

Thank you for helping improve **HanduFlow**. This guide covers local setup, conventions, and how to propose changes.

## Development setup

```bash
git clone https://github.com/hhandoo/handuflow-core.git
cd handuflow-core
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

Requires **Java 11+** and network access on first Spark run (Maven JARs).

## Project layout

```text
src/handuflow/
├── orchestrator/       # Orchestrator.run() pipeline
├── config/             # config.ini helpers, Spark session, catalog naming
├── validation/         # Startup rules for master_specs.xlsx
├── data_movement_controller/
├── data_quality/
├── result_generator/
├── system_cleanup/     # Retention + Delta vacuum
├── system_restore/     # Restore points (HFRP####)
├── system_shared/      # Shared cleanup/restore utilities
└── exception/          # HanduFlow error types
```

Per-module reference: [documentation/MODULES.md](documentation/MODULES.md)

## Configuration for local runs

1. Edit `files_dev/config.ini` so `file_hunt_path` and temp paths match your checkout.
2. Set `[PLATFORM] runtime_mode=local` and `target_unity_catalog=local` in master specs.
3. See [documentation/CONFIG.md](documentation/CONFIG.md) and [documentation/SETUP.md](documentation/SETUP.md).

## Code conventions

- **Naming:** library name is **HanduFlow** / **handuflow** (not SDMF).
- **Modules & packages:** PEP 8 `snake_case` for all `.py` files and directories (e.g. `load_dispatcher.py`, `system_restore/restore.py`). **Classes** remain `PascalCase`; **functions** and **variables** use `snake_case`.
- **Imports:** prefer package public API (`from handuflow import Orchestrator`) or submodule paths (`handuflow.data_movement_controller.load_dispatcher`), not legacy PascalCase module names.
- **Logging:** use `logging.getLogger(__name__)`; user-facing pipeline logs use the `handuflow` logger from `LoggingConfig`.
- **Errors:** subclass `BaseException` in `handuflow.exception`; do not swallow errors in the orchestrator—record in `RunResult.phase_errors`.
- **Spark:** no `collect()` on large datasets in library code; prefer DataFrame APIs.
- **Config:** read paths via `handuflow.config.cfg_get`, not hard-coded section names.
- **Tables:** use `CatalogResolver` for local vs Unity Catalog qualified names.

## Running checks

See [documentation/RUN.md](documentation/RUN.md) for all trigger options.

```bash
pip install -e ".[dev]"

# Unit + integration regression (all load types)
pytest tests/regression -m integration -v

# Integrity helpers only (faster)
pytest tests/regression/test_load_integrity_unit.py -v

# Manual local orchestrator (files_dev; not CI)
python scripts/run_local_orchestrator.py

# E2E harness (long-running)
python -m tests.e2e.run_e2e --smoke
```

## Versioning & release

- **Single source of truth:** `pyproject.toml` → `[project].version` (current published version)
- **Next release:** edit [`RELEASE.toml`](../RELEASE.toml), then `python scripts/release.py prepare`
- **Branch flow:** push only to **`dev`** → PR **`dev` → `main`** → merge triggers **one** deploy run — see [documentation/DEPLOYMENT.md](documentation/DEPLOYMENT.md)

## Pull requests

1. Branch from and push to **`dev`** only — never push directly to **`main`**.
2. Open PRs **`dev` → `main`** when ready to land or release.
2. One logical change per PR.
3. Update README if you add config keys or public API.
4. Add a CHANGELOG entry under `[Unreleased]` when behavior changes (or use `RELEASE.toml` for release notes).
5. For releases, ensure `RELEASE.toml` / `CHANGELOG.md` are prepared before merging.
6. Ensure `Orchestrator.run()` still finalizes logs in `finally`.

## Public API

Stable entry points: [documentation/API.md](documentation/API.md)

```python
from handuflow import run, Orchestrator, RunResult, RunStatus, CatalogResolver
```

Avoid relying on private methods (`_finalize`, `_phase_errors`).

## Questions

Open a [GitHub issue](https://github.com/hhandoo/handuflow-core/issues) for bugs or design discussions.
