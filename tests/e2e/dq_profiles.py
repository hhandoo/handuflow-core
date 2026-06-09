"""Data-quality feed_specs profiles for E2E tests."""

from __future__ import annotations

LOAD_TYPE_LABELS = {
    "FULL_LOAD": "Full Load",
    "APPEND_LOAD": "Append Load",
    "INCREMENTAL_CDC": "Incremental CDC",
    "SCD_TYPE_2": "SCD Type 2",
}


def apply_dq_profile(
    feed_specs: dict,
    profile: str,
    *,
    source_table: str,
    ref_schema: str = "qaft_ref",
) -> dict:
    """Attach standard / comprehensive checks for DQ test scenarios."""
    fs = dict(feed_specs)
    if profile in ("none", "", None):
        fs["standard_checks"] = []
        fs["comprehensive_checks"] = []
        return fs

    if profile == "standard_pass":
        fs["standard_checks"] = [
            {
                "check_sequence": ["_check_primary_key"],
                "column_name": "id",
                "threshold": 0,
            },
            {
                "check_sequence": ["_check_nulls"],
                "column_name": "business_key",
                "threshold": 0,
            },
        ]
        fs["comprehensive_checks"] = []
        return fs

    if profile == "standard_fail":
        fs["standard_checks"] = [
            {
                "check_sequence": ["_check_nulls"],
                "column_name": "id",
                "threshold": 0,
            }
        ]
        fs["comprehensive_checks"] = []
        return fs

    if profile == "pre_load_pass":
        fs["standard_checks"] = []
        fs["comprehensive_checks"] = [
            {
                "check_name": "source_row_count_positive",
                "query": (
                    f"SELECT id FROM {source_table} GROUP BY id HAVING COUNT(*) > 1"
                ),
                "severity": "ERROR",
                "threshold": 0,
                "load_stage": "PRE_LOAD",
            }
        ]
        return fs

    if profile == "pre_load_fail":
        fs["standard_checks"] = []
        fs["comprehensive_checks"] = [
            {
                "check_name": "orphan_business_keys",
                "query": (
                    f"SELECT s.id FROM {source_table} s "
                    f"LEFT JOIN {ref_schema}.valid_business_keys r "
                    f"ON s.business_key = r.business_key "
                    f"WHERE r.business_key IS NULL"
                ),
                "severity": "ERROR",
                "threshold": 0,
                "load_stage": "PRE_LOAD",
                "dependency_dataset": [f"{ref_schema}.valid_business_keys"],
            }
        ]
        return fs

    if profile == "post_load_pass":
        fs["standard_checks"] = []
        fs["comprehensive_checks"] = [
            {
                "check_name": "target_has_rows",
                "query": "SELECT 1 AS fail WHERE (SELECT COUNT(*) FROM __TARGET__) = 0",
                "severity": "ERROR",
                "threshold": 0,
                "load_stage": "POST_LOAD",
            }
        ]
        return fs

    if profile == "post_load_fail":
        fs["standard_checks"] = []
        fs["comprehensive_checks"] = [
            {
                "check_name": "target_row_count_impossible",
                "query": (
                    "SELECT 1 AS fail WHERE "
                    "(SELECT COUNT(*) FROM __TARGET__) != 999999999"
                ),
                "severity": "ERROR",
                "threshold": 0,
                "load_stage": "POST_LOAD",
            }
        ]
        return fs

    if profile == "full_dq":
        fs = apply_dq_profile(fs, "standard_pass", source_table=source_table)
        fs["comprehensive_checks"] = [
            {
                "check_name": "no_duplicate_ids",
                "query": (
                    f"SELECT id FROM {source_table} GROUP BY id HAVING COUNT(*) > 1"
                ),
                "threshold": 0,
                "load_stage": "PRE_LOAD",
            },
            {
                "check_name": "target_row_count",
                "query": (
                    "SELECT 1 AS fail WHERE (SELECT COUNT(*) FROM __TARGET__) = 0"
                ),
                "threshold": 0,
                "load_stage": "POST_LOAD",
            },
        ]
        return fs

    raise ValueError(f"Unknown DQ profile: {profile}")
