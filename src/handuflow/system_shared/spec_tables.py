"""Collect source/target Delta table names from master specs."""

from __future__ import annotations

import json
import os
import configparser
from typing import TYPE_CHECKING

import pandas as pd

from handuflow.config.config_paths import cfg_get

from handuflow.config.catalog_resolver import CatalogResolver
from handuflow.constants import is_ingestion_direction

if TYPE_CHECKING:
    pass

TABLE_TYPE_SOURCE = "SOURCE"
TABLE_TYPE_TARGET = "TARGET"
TABLE_TYPE_STAGING = "STAGING"


def master_specs_to_dataframe(master_specs: pd.DataFrame | list[dict]) -> pd.DataFrame:
    if isinstance(master_specs, pd.DataFrame):
        return master_specs
    return pd.DataFrame(master_specs)


def load_master_specs_from_config(config: configparser.ConfigParser) -> pd.DataFrame:
    """Load active rows from ``master_specs.xlsx`` using paths in ``config.ini``."""
    file_hunt_path = cfg_get(config, "file_hunt_path")
    master_spec_name = cfg_get(
        config, "master_spec_name", "master_specs.xlsx", section="FILES"
    )
    master_specs_path = os.path.join(file_hunt_path, master_spec_name)
    df = pd.read_excel(master_specs_path, sheet_name="master_specs")
    return df[df["is_active"] == True]


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
    """Unique source and target table names (cleanup and legacy callers)."""
    return {name for name, _ in collect_master_spec_table_entries(master_specs, config)}


def collect_cleanup_table_entries(
    master_specs: pd.DataFrame | list[dict],
    config: configparser.ConfigParser,
) -> set[str]:
    """
    All master-spec Delta tables eligible for post-run retention cleanup.

    Includes source, target, and intermediate staging tables.
    """
    names = {
        name for name, _ in collect_master_spec_table_entries(master_specs, config)
    }
    names.update(
        name
        for name, _ in collect_restore_point_table_entries(master_specs, config)
    )
    return names


def _staging_table_names(
    target_table_name: str,
    resolver: CatalogResolver,
) -> list[str]:
    """Per-feed staging Delta tables under the catalog staging schema."""
    staging_schema = resolver.staging_schema()
    return [
        f"{staging_schema}.t_full_{target_table_name}",
        f"{staging_schema}.t_incr_{target_table_name}",
        f"{staging_schema}.t_incr_cdf_changes_{target_table_name}",
    ]


def collect_restore_point_table_entries(
    master_specs: pd.DataFrame | list[dict],
    config: configparser.ConfigParser,
) -> list[tuple[str, str]]:
    """
    Return (table_name, table_type) pairs for restore points.

    ``WITHIN_UNITY_CATALOG`` feeds: source, target, and staging tables exactly as
    configured in master specs (no inferred bronze/silver/gold layers).

    ``INGESTION`` feeds: landing target table only.
    """
    df = master_specs_to_dataframe(master_specs)
    entries: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    if df.empty:
        return entries

    for row in df.to_dict(orient="records"):
        catalog = str(row.get("target_unity_catalog", "") or "").strip()
        schema = str(row.get("target_schema_name", "") or "").strip()
        table = str(row.get("target_table_name", "") or "").strip()
        direction = str(row.get("data_flow_direction", "") or "").strip()
        if not (schema and table):
            continue
        resolver = CatalogResolver(catalog, config=config)
        target = resolver.target_table(schema, table)
        table_types: dict[str, str] = {target: TABLE_TYPE_TARGET}

        if not is_ingestion_direction(direction):
            raw_specs = row.get("feed_specs")
            feed_specs: dict | None = None
            if raw_specs:
                try:
                    feed_specs = (
                        json.loads(raw_specs)
                        if isinstance(raw_specs, str)
                        else raw_specs
                    )
                except json.JSONDecodeError:
                    feed_specs = None
            if feed_specs:
                source_table = (feed_specs.get("source_table_name") or "").strip()
                if source_table:
                    table_types[source_table] = TABLE_TYPE_SOURCE
            for name in _staging_table_names(table, resolver):
                table_types[name] = TABLE_TYPE_STAGING

        for table_name, table_type in table_types.items():
            pair = (table_name, table_type)
            if pair in seen:
                continue
            seen.add(pair)
            entries.append(pair)
    return entries


def expected_restore_table_set(
    master_specs: pd.DataFrame | list[dict],
    config: configparser.ConfigParser,
) -> set[str]:
    """Unity Catalog target tables required for a complete restore point."""
    return {
        name
        for name, table_type in collect_restore_point_table_entries(
            master_specs, config
        )
        if table_type == TABLE_TYPE_TARGET
    }
