"""Package version — single source of truth is pyproject.toml [project].version."""

from __future__ import annotations

from pathlib import Path


def _version_from_pyproject() -> str:
    import tomllib

    root = Path(__file__).resolve().parents[2]
    with (root / "pyproject.toml").open("rb") as fh:
        data = tomllib.load(fh)
    return str(data["project"]["version"])


def _installed_version() -> str | None:
    try:
        from importlib.metadata import PackageNotFoundError, version

        return version("handuflow")
    except PackageNotFoundError:
        return None
    except Exception:
        return None


__version__ = _installed_version() or _version_from_pyproject()
