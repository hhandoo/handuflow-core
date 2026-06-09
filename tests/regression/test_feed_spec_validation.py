"""Medallion feed_specs key validation (no Spark)."""

from handuflow.validation.validation_rules.enforce_master_specs_structure import (
    validate_medallion_feed_spec_keys,
)

BASE = {
    "primary_key": "id",
    "composite_key": [],
    "partition_keys": [],
    "vacuum_hours": 168,
    "source_table_name": "demo.t",
    "selection_query": None,
    "selection_schema": {"type": "struct", "fields": []},
    "standard_checks": [],
    "comprehensive_checks": [],
}


def test_optional_allow_unmatched_deletes_accepted():
    spec = {**BASE, "allow_unmatched_deletes": False}
    assert validate_medallion_feed_spec_keys(spec) is None


def test_optional_allow_empty_source_accepted():
    spec = {**BASE, "allow_empty_source": True}
    assert validate_medallion_feed_spec_keys(spec) is None


def test_unknown_key_rejected():
    spec = {**BASE, "extra_field": 1}
    err = validate_medallion_feed_spec_keys(spec)
    assert err is not None
    assert "Unknown keys" in err


def test_missing_required_key_rejected():
    spec = {k: v for k, v in BASE.items() if k != "primary_key"}
    err = validate_medallion_feed_spec_keys(spec)
    assert err is not None
    assert "Missing keys" in err
