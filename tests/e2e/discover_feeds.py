"""Discover active feeds from files_dev master_specs."""

from __future__ import annotations

from pathlib import Path

from tests.e2e.spark_setup import MASTER_SPECS_PATH


def discover_configured_feeds(
    path: str | Path | None = None,
) -> list[dict]:
    import pandas as pd

    p = Path(path) if path is not None else MASTER_SPECS_PATH
    if not p.exists():
        return []
    df = pd.read_excel(p, sheet_name="master_specs")
    active = df[df["is_active"] == True]  # noqa: E712
    return active.to_dict(orient="records")
