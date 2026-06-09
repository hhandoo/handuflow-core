"""Collect source/target Delta table names from master specs."""

from __future__ import annotations

import json
import configparser
from typing import TYPE_CHECKING

import pandas as pd

from handuflow.config.catalog_resolver import CatalogResolver

if TYPE_CHECKING:
    pass

TABLE_TYPE_SOURCE = "SOURCE"
TABLE_TYPE_TARGET = "TARGET"


def master_specs_to_dataframe(master_specs: pd.DataFrame | list[dict]) -> pd.DataFrame:
    if isinstance(master_specs, pd.DataFrame):
        return master_specs
    return pd.DataFrame(master_specs)


def collect_master_spec_table_entries(
    master_specs: pd.DataFrame | list[dict],
    config: configparser.ConfigParser,
) -> list[tuple[str, str]]:
    """
    Return (table_name, table_type) pairs for all source and target tables.

    table_type is ``SOURCE`` or ``TARGET``. The same physical table may appear twice.
    """
    df = master_specs_to_dataframe(master_specs)
    entries: list[tuple[str, str]] = []
    if df.empty:
        return entries

    for row in df.to_dict(orient="records"):
        catalog = str(row.get("target_unity_catalog", "") or "").strip()
        schema = str(row.get("target_schema_name", "") or "").strip()
        table = str(row.get("target_table_name", "") or "").strip()
        if schema and table:
            resolver = CatalogResolver(catalog, config=config)
            entries.append(
                (resolver.target_table(schema, table), TABLE_TYPE_TARGET)
            )

        raw_specs = row.get("feed_specs")
        if not raw_specs:
            continue
        try:
            feed_specs = (
                json.loads(raw_specs) if isinstance(raw_specs, str) else raw_specs
            )
        except json.JSONDecodeError:
            continue
        source_table = (feed_specs.get("source_table_name") or "").strip()
        if source_table:
            entries.append((source_table, TABLE_TYPE_SOURCE))
    return entries


def expected_table_set(
    master_specs: pd.DataFrame | list[dict],
    config: configparser.ConfigParser,
) -> set[str]:
    """Unique table names required for a complete restore point."""
    return {name for name, _ in collect_master_spec_table_entries(master_specs, config)}
