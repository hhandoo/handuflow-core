"""Unit tests for restore-point table scope (targets + staging, not sources)."""

from __future__ import annotations

import configparser
import json

import pandas as pd

from handuflow.constants import INGESTION, WITHIN_UNITY_CATALOG
from handuflow.system_shared.spec_tables import (
    TABLE_TYPE_SOURCE,
    TABLE_TYPE_STAGING,
    TABLE_TYPE_TARGET,
    collect_cleanup_table_entries,
    collect_master_spec_table_entries,
    collect_restore_point_table_entries,
    expected_restore_table_set,
    expected_table_set,
)


def _config() -> configparser.ConfigParser:
    cfg = configparser.ConfigParser()
    cfg.read_dict(
        {
            "DEFAULT": {"global_vacuum_hours": "168"},
            "PLATFORM": {"runtime_mode": "local"},
        }
    )
    return cfg


def _master_row(
    *,
    direction: str = WITHIN_UNITY_CATALOG,
) -> dict:
    return {
        "feed_id": 1,
        "target_unity_catalog": "local",
        "target_schema_name": "silver",
        "target_table_name": "t_iso_language_codes",
        "data_flow_direction": direction,
        "feed_specs": json.dumps(
            {
                "source_table_name": "demo.test",
                "selection_schema": {
                    "type": "struct",
                    "fields": [
                        {
                            "name": "id",
                            "type": "long",
                            "nullable": False,
                            "metadata": {},
                        }
                    ],
                },
            }
        ),
    }


def test_restore_scope_within_unity_catalog_includes_source_target_staging():
    cfg = _config()
    specs = pd.DataFrame([_master_row()])

    restore_entries = collect_restore_point_table_entries(specs, cfg)
    by_type = {name: t for name, t in restore_entries}

    assert by_type["silver.t_iso_language_codes"] == TABLE_TYPE_TARGET
    assert by_type["demo.test"] == TABLE_TYPE_SOURCE
    assert by_type["staging.t_full_t_iso_language_codes"] == TABLE_TYPE_STAGING
    assert by_type["staging.t_incr_t_iso_language_codes"] == TABLE_TYPE_STAGING
    assert (
        by_type["staging.t_incr_cdf_changes_t_iso_language_codes"]
        == TABLE_TYPE_STAGING
    )
    assert "bronze.t_iso_language_codes" not in by_type
    assert "gold.t_iso_language_codes" not in by_type

    assert expected_restore_table_set(specs, cfg) == {"silver.t_iso_language_codes"}


def test_restore_scope_ingestion_includes_target_only():
    cfg = _config()
    specs = pd.DataFrame([_master_row(direction=INGESTION)])

    restore_entries = collect_restore_point_table_entries(specs, cfg)
    by_type = {name: t for name, t in restore_entries}

    assert by_type == {"silver.t_iso_language_codes": TABLE_TYPE_TARGET}


def test_cleanup_scope_includes_sources_targets_and_staging():
    cfg = _config()
    specs = pd.DataFrame([_master_row()])

    names = collect_cleanup_table_entries(specs, cfg)

    assert "demo.test" in names
    assert "silver.t_iso_language_codes" in names
    assert "staging.t_full_t_iso_language_codes" in names


def test_cleanup_legacy_entries_still_include_sources():
    cfg = _config()
    specs = pd.DataFrame([_master_row()])

    cleanup_entries = collect_master_spec_table_entries(specs, cfg)
    names = {name for name, _ in cleanup_entries}
    types = {t for _, t in cleanup_entries}

    assert "demo.test" in names
    assert "silver.t_iso_language_codes" in names
    assert TABLE_TYPE_SOURCE in types
    assert expected_table_set(specs, cfg) == {"demo.test", "silver.t_iso_language_codes"}
