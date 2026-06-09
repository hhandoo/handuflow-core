"""CDC column alignment helpers (no Spark)."""

from handuflow.data_movement_controller.load_integrity import (
    STAGING_ONLY_COLUMNS,
    LoadIntegrityVerifier,
)


def test_staging_columns_excluded_from_business_keys():
    feed_specs = {
        "selection_schema": {
            "type": "struct",
            "fields": [
                {
                    "name": "alpha3_b",
                    "type": "string",
                    "nullable": True,
                    "metadata": {},
                },
                {
                    "name": "english",
                    "type": "string",
                    "nullable": True,
                    "metadata": {},
                },
            ],
        }
    }
    cols = LoadIntegrityVerifier.non_key_business_columns(
        feed_specs, ["alpha3_b"]
    )
    assert cols == ["english"]
    assert "_x_load_id" not in cols
    assert STAGING_ONLY_COLUMNS.isdisjoint(cols)
