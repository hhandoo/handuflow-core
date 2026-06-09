# Deployment & versioning

How HanduFlow is packaged, versioned, and released.

---

## Versioning

| Item | Location |
|------|----------|
| **Canonical version** | `pyproject.toml` → `[project].version` |
| **Runtime `__version__`** | `handuflow.__version__` (from installed metadata or `pyproject.toml`) |
| **Git tags** | `v0.0.1` (must match `pyproject.toml`) |
| **Changelog** | `CHANGELOG.md` ([Keep a Changelog](https://keepachangelog.com/)) |

Semantic versioning: `MAJOR.MINOR.PATCH`

```python
import handuflow
print(handuflow.__version__)
```

---

## Environment templates

| Directory | Use |
|-----------|-----|
| `files_dev/` | Local development (Hive, `runtime_mode=local`) |
| `files_prod/` | Production / Databricks template (`runtime_mode=unity_catalog`) |

Copy the appropriate `config.ini` into your `handuflow_dir` and set paths. See [CONFIG.md](CONFIG.md).

Required folders under `file_hunt_path`:

```text
handuflow_dir/
├── config.ini
├── master_specs.xlsx
├── handuflow_outbound/
├── handuflow_logs/
├── temp/
└── dmc_temp/
```

---

## Install

### From PyPI

```bash
pip install handuflow
pip install "handuflow[spark]"   # optional: local PySpark
```

### From source (development)

```bash
pip install -e ".[dev]"          # recommended
# or
pip install -r requirements-dev.txt
```

### Runtime only (no dev tools)

```bash
pip install -e .
pip install -e ".[spark]"
```

Dependency pins are defined only in **`pyproject.toml`**. The `requirements*.txt` files are convenience wrappers.

---

## Branch & deployment flow

**Never push to `main`.** All commits go to **`dev`**; **`main`** is updated only by merging pull requests from **`dev`**.

```text
dev  ──push──►  (no CI)
  │
  └── PR ──►  main  ──CI──►  merge ──►  PyPI + GitHub Release (if release PR)
```

| Branch / event | Workflow | What runs |
|----------------|----------|-----------|
| Push to **`dev`** | — | No CI (local dev only) |
| PR **`dev` → `main`** | [CI](../.github/workflows/ci.yml) | Tests, version check, package build — must pass before merge |
| Push to **`main`** (merge) | [CI](../.github/workflows/ci.yml) | Same checks on merged code |
| PR **`dev` → `main`** merged (release) | [Release](../.github/workflows/release.yml) | Build → PyPI → GitHub Release + tag |
| Tag **`v*.*.*`** on a **`main`** commit (optional) | [Release](../.github/workflows/release.yml) | Same publish flow |

Develop on **`dev`** (push freely; no CI). Open a pull request into **`main`** — CI runs on the PR and must pass before merge. If the merged PR includes a version bump and `CHANGELOG.md` section (from `release.py prepare`), PyPI publish runs automatically.

**GitHub branch protection (recommended):** require PR reviews + CI checks on **`main`**; disallow direct pushes to **`main`**.

---

## Release process

Releases are driven by **`RELEASE.toml`** at the repo root — the release control file. It always describes the *next* version to publish and its notes.

### 1. Edit the next release

Open [`RELEASE.toml`](../RELEASE.toml) and set:

- **`version`** — must be greater than the current `pyproject.toml` version (e.g. `"0.0.2"` after `0.0.1`)
- **`notes`** — markdown with Keep a Changelog sections (`### Added`, `### Changed`, `### Fixed`)

Validate without writing files:

```bash
python scripts/release.py check
```

### 2. Prepare the release

```bash
python scripts/release.py prepare
# optional: python scripts/release.py prepare --dry-run
```

This updates:

| File | Change |
|------|--------|
| `pyproject.toml` | Sets `[project].version` to `version` from `RELEASE.toml` |
| `CHANGELOG.md` | Inserts a dated `[X.Y.Z]` section (used by CI for GitHub Release notes) |
| `RELEASE.toml` | Resets with a suggested next patch version and empty notes |

### 3. Push to dev and open a PR to main

On **`dev`** only (never push to **`main`**):

```bash
git checkout dev
git add pyproject.toml CHANGELOG.md RELEASE.toml
git commit -m "Release 0.0.2"
git push origin dev
```

Open a pull request **`dev` → `main`**. When CI passes, merge the PR. The [Release](../.github/workflows/release.yml) workflow publishes to PyPI automatically if `CHANGELOG.md` contains a section for the new version.

### 4. Automated publish

Merging a release PR into **`main`** triggers [`.github/workflows/release.yml`](../.github/workflows/release.yml):

1. Validates `CHANGELOG.md` has a section for `pyproject.toml` version
2. Builds wheel + sdist (`python -m build`)
3. Publishes to PyPI via [`pypa/gh-action-pypi-publish`](https://github.com/pypa/gh-action-pypi-publish) (`PYPI_API_TOKEN` secret)
4. Creates GitHub Release (tag `vX.Y.Z`) with changelog notes

Non-release merges (no new changelog section) skip PyPI publish. Optional: push tag `v*.*.*` on a **`main`** commit to re-run publish manually.

Manual dry-run (build only, no upload):

```bash
# GitHub Actions → Release → Run workflow → dry_run: true
```

**PyPI setup:** add repository secret `PYPI_API_TOKEN` (PyPI → Account settings → API tokens). Optional: configure [trusted publishing](https://docs.pypi.org/trusted-publishers/) for the `pypi` environment instead of a token.

### CI (main only)

[`.github/workflows/ci.yml`](../.github/workflows/ci.yml) runs on pull requests into **`main`** and on pushes to **`main`** (after merge). Pushes to **`dev`** do not trigger CI.

- Config + unit tests (no Spark)
- Integration tests (`pytest -m integration`)
- Version consistency check (`pyproject.toml` == `handuflow._version`)
- Package build (`python -m build`, artifact uploaded — not published)

---

## Databricks deployment

1. Copy `files_prod/config.ini` to DBFS/Volumes `handuflow_dir/`
2. Set `file_hunt_path`, `system_schema`, `global_vacuum_hours`
3. Place `master_specs.xlsx` under `file_hunt_path`
4. Install wheel on cluster or `%pip install handuflow`
5. Run:

```python
from pyspark.sql import SparkSession
from handuflow import run

spark = SparkSession.builder.getOrCreate()
result = run(spark, config_path="/dbfs/mnt/handuflow_dir/config.ini")
```

---

## Local deployment

See [RUN.md](RUN.md) and [SETUP.md](SETUP.md).

```bash
pip install -e ".[spark]"
python scripts/run_local_orchestrator.py
```

---

## Related

| Doc | Topic |
|-----|--------|
| [RUN.md](RUN.md) | Trigger pipeline & tests |
| [CONFIG.md](CONFIG.md) | config.ini reference |
| [CONTRIBUTING.md](../CONTRIBUTING.md) | Development workflow |
