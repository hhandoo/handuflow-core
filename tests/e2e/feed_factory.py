"""Build master_spec + feed_specs dicts for LoadDispatcher."""

from __future__ import annotations

import json

from tests.e2e.data_generator import SELECTION_SCHEMA


def build_feed_specs(
    source_table: str,
    *,
    partition_keys: list[str] | None = None,
    allow_unmatched_deletes: bool = True,
) -> dict:
    return {
        "primary_key": "id",
        "composite_key": [],
        "partition_keys": partition_keys or [],
        "vacuum_hours": 168,
        "source_table_name": source_table,
        "selection_query": None,
        "selection_schema": SELECTION_SCHEMA,
        "standard_checks": [],
        "comprehensive_checks": [],
        "allow_unmatched_deletes": allow_unmatched_deletes,
        "allow_empty_source": False,
    }


def build_master_spec(
    feed_id: int,
    load_type: str,
    target_table: str,
    feed_specs: dict,
    *,
    target_schema: str = "qaft_silver",
) -> dict:
    return {
        "feed_id": feed_id,
        "system_name": "QAFT",
        "subsystem_name": "E2E",
        "category": "test",
        "sub_category": load_type,
        "data_flow_direction": "BRONZE_TO_SILVER",
        "residing_layer": "silver",
        "feed_name": f"qaft_{load_type.lower()}_{feed_id}",
        "feed_type": "DELTA_TABLE",
        "feed_specs": json.dumps(feed_specs),
        "load_type": load_type,
        "target_unity_catalog": "local",
        "target_schema_name": target_schema,
        "target_table_name": target_table,
        "suggested_feed_name": f"qaft_{target_table}",
        "parallelism_group_number": 1,
        "parent_feed_id": "",
        "is_active": True,
    }
