"""Audit column policy per load type (no Spark)."""

from handuflow.data_movement_controller.audit_columns import (
    AuditColumns,
    SCD_TARGET_COLUMNS,
    STAGING_ONLY_COLUMNS,
    TARGET_ROW_HASH_COLUMN,
    TargetLoadKind,
)

FEED_SPECS = {
    "selection_schema": {
        "type": "struct",
        "fields": [
            {"name": "alpha3_b", "type": "string", "nullable": True, "metadata": {}},
            {"name": "english", "type": "string", "nullable": True, "metadata": {}},
        ],
    }
}


def test_expected_target_full_and_append_are_business_only():
    expected = AuditColumns.expected_target_columns(
        FEED_SPECS, TargetLoadKind.FULL_LOAD
    )
    assert expected == ["alpha3_b", "english"]
    assert TARGET_ROW_HASH_COLUMN not in expected
    assert STAGING_ONLY_COLUMNS.isdisjoint(expected)


def test_expected_target_cdc_includes_row_hash():
    expected = AuditColumns.expected_target_columns(
        FEED_SPECS, TargetLoadKind.INCREMENTAL_CDC
    )
    assert TARGET_ROW_HASH_COLUMN in expected
    assert "_x_operation" not in expected
    assert "_x_load_id" not in expected


def test_expected_target_scd_includes_scd_metadata():
    expected = AuditColumns.expected_target_columns(
        FEED_SPECS, TargetLoadKind.SCD_TYPE_2
    )
    assert SCD_TARGET_COLUMNS.issubset(set(expected))
    assert "_x_operation" not in expected


def test_merge_update_columns_exclude_stream_metadata():
    cols = AuditColumns.merge_update_columns(
        ["alpha3_b", "english", "_x_operation", "_x_row_hash", "_x_load_id"]
    )
    assert cols == ["alpha3_b", "english", "_x_row_hash"]


def test_merge_insert_values_exclude_stream_metadata():
    cols = AuditColumns.merge_insert_values(
        ["alpha3_b", "english", "_x_operation", "_x_row_hash", "_x_load_id"]
    )
    assert cols == ["alpha3_b", "english", "_x_row_hash"]
