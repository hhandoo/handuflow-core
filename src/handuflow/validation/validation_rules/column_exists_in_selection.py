from handuflow.validation.validation_rule import ValidationRule
from handuflow.validation.validation_context import ValidationContext
class ColumnExistsInSelection(ValidationRule):
    name = "Feed spec column exists in selection check"
    error_code = "HF007"

    def validate(self, context: ValidationContext):
        if context.mdf_feed_specs_array is None:
            self.fail(
                message="JSON has not been parsed yet",
                original_exception=None
            )

        for json_dict in context.mdf_feed_specs_array:
            if json_dict["data_flow_direction"] != "SOURCE_TO_BRONZE":
                data = json_dict["feed_specs_dict"]
                if "source_table_name" not in data:
                    self.fail(
                        f"Missing 'source_table_name' for feed id {json_dict['feed_id']}",
                        original_exception=None
                    )
                if "selection_schema" not in data:
                    self.fail(
                        f"Missing 'selection_schema' for feed id {json_dict['feed_id']}",
                        original_exception=None
                    )
                fields = data["selection_schema"].get("fields")
                if not isinstance(fields, list):
                    self.fail(
                        f"'selection_schema.fields' must be a list for feed id {json_dict['feed_id']}",
                        original_exception=None
                    )
                table_name = data["source_table_name"]
                if context.spark.catalog.tableExists(table_name):
                    try:
                        table_df = context.spark.table(table_name)
                        table_columns = {col.name for col in table_df.schema.fields}
                    except Exception as e:
                        self.fail(
                            message=f"Unable to read schema for table '{table_name}' for feed id {json_dict['feed_id']}",
                            original_exception=e
                        )
                    for idx, field in enumerate(fields):
                        if not isinstance(field, dict) or "name" not in field:
                            self.fail(
                                f"selection_schema.fields[{idx}] must contain 'name' for feed id {json_dict['feed_id']}",
                                original_exception=None
                            )
                        column_name = field["name"]
                        if column_name not in table_columns:
                            self.fail(
                                message=(
                                    f"selection_schema.fields[{idx}].name '{column_name}' "
                                    f"not found in table '{table_name}'"
                                    f" for feed id {json_dict['feed_id']}"
                                ),
                                original_exception=None
                            )
