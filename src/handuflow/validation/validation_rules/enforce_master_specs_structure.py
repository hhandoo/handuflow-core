# inbuilt
import os
import json

# external
import pandas as pd

# internal
from handuflow.constants import ALLOWED_DATA_FLOW_DIRECTIONS, WITHIN_UNITY_CATALOG
from handuflow.validation.validation_rule import ValidationRule
from handuflow.validation.validation_context import ValidationContext

# Medallion feed_specs: required keys (exact set not required).
REQUIRED_MEDALLION_FEED_SPEC_KEYS = frozenset(
    {
        "primary_key",
        "composite_key",
        "partition_keys",
        "vacuum_hours",
        "source_table_name",
        "selection_query",
        "selection_schema",
        "standard_checks",
        "comprehensive_checks",
    }
)

OPTIONAL_MEDALLION_FEED_SPEC_KEYS = frozenset(
    {
        "allow_empty_source",
        "allow_unmatched_deletes",
    }
)

ALLOWED_MEDALLION_FEED_SPEC_KEYS = (
    REQUIRED_MEDALLION_FEED_SPEC_KEYS | OPTIONAL_MEDALLION_FEED_SPEC_KEYS
)


def validate_medallion_feed_spec_keys(feed_specs_dict: dict) -> str | None:
    """Return an error message if keys are invalid, else None."""
    top = set(feed_specs_dict.keys())
    missing = REQUIRED_MEDALLION_FEED_SPEC_KEYS - top
    unknown = top - ALLOWED_MEDALLION_FEED_SPEC_KEYS
    if not missing and not unknown:
        return None
    parts = []
    if missing:
        parts.append(f"Missing keys: {sorted(missing)}")
    if unknown:
        parts.append(f"Unknown keys: {sorted(unknown)}")
    return "Validation failed! " + "; ".join(parts)


class EnforceMasterSpecsStructure(ValidationRule):
    name = "Enforce master spec structure"
    error_code = "HF002"
    def validate(self, context: ValidationContext):
        master_specs_path = os.path.join(context.file_hunt_path, context.master_spec_name)
        if os.path.exists(master_specs_path) == False:
            self.fail(
                message = f"System can't find the Master Specs File at [{context.file_hunt_path}]. Terminating process", 
                original_exception=None
            )
        context.master_specs_dataframe = pd.read_excel(master_specs_path, sheet_name="master_specs")
        context.master_specs_dataframe = context.master_specs_dataframe[context.master_specs_dataframe['is_active'] == True]
        cols = context.master_specs_dataframe.columns.tolist()
        required_columns = [
            "feed_id",
            "system_name",
            "subsystem_name",
            "category",
            "sub_category",
            "data_flow_direction",
            "residing_layer",
            "feed_name",
            "feed_type",
            "feed_specs",
            "load_type",
            "target_unity_catalog",
            "target_schema_name",
            "target_table_name",
            "suggested_feed_name",
            "parallelism_group_number",
            "parent_feed_id",
            "is_active",
        ]
        expected = set(required_columns)
        actual = set(context.master_specs_dataframe.columns)
        missing = expected - actual
        extra = actual - expected
        if missing or extra:
            self.fail(
                message = f"Missing columns: {sorted(missing)} | Extra columns: {sorted(extra)}", 
                original_exception=None
            )
        subset = context.master_specs_dataframe[required_columns].replace(r"^\s*$", pd.NA, regex=True)
        null_mask = subset.isna()
        if null_mask.any().any():
            errors = (
                null_mask
                .stack()
                .reset_index()
            )
            errors.columns = ["row_index", "column", "is_null"]
            errors = errors[errors["is_null"]]
            errors["row_number"] = errors["row_index"] + 2
            error_lines = []
            grouped = (
                errors[["row_number", "column"]]
                .groupby("row_number")["column"]
                .apply(list)
            )
            for row_number, columns in grouped.items():
                cols = ", ".join(columns)
                error_lines.append(f"  • Row {row_number}: {cols}")
            message = (
                "Validation failed: required columns contain null or blank values\n\n"
                "Affected rows and columns:\n"
                + "\n".join(error_lines) + '\n'
            )
            self.fail(
                message=message,
                original_exception=None            
            )
        invalid_directions = context.master_specs_dataframe[
            ~context.master_specs_dataframe["data_flow_direction"].isin(
                ALLOWED_DATA_FLOW_DIRECTIONS
            )
        ]
        if not invalid_directions.empty:
            values = sorted(
                invalid_directions["data_flow_direction"].astype(str).unique().tolist()
            )
            self.fail(
                message=(
                    "Validation failed: data_flow_direction must be one of "
                    f"{sorted(ALLOWED_DATA_FLOW_DIRECTIONS)}; found {values}"
                ),
                original_exception=None,
            )
        else:

            filtered_df = context.master_specs_dataframe[
                context.master_specs_dataframe["data_flow_direction"]
                == WITHIN_UNITY_CATALOG
            ]

            all_feed_specs = filtered_df['feed_specs'].to_list()
            for feed_specs in all_feed_specs:
                feed_specs_dict = json.loads(feed_specs)
                message = validate_medallion_feed_spec_keys(feed_specs_dict)
                if message:
                    self.fail(
                        message=message,
                        original_exception=None,
                    )








