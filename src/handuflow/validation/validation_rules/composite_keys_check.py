from handuflow.constants import is_within_unity_catalog_direction
from handuflow.validation.validation_rule import ValidationRule
from handuflow.validation.validation_context import ValidationContext
class CompositeKeysCheck(ValidationRule):
    name = "Composite keys check"
    error_code = "HF005"

    def validate(self, context: ValidationContext):

        if context.mdf_feed_specs_array is None:
            self.fail(
                message="JSON list has not been parsed yet",
                original_exception=None
            )
        
        for json_dict in context.mdf_feed_specs_array:

            if is_within_unity_catalog_direction(json_dict["data_flow_direction"]):
                data = json_dict['feed_specs_dict']
                value = data.get("composite_key")
                if "composite_key" not in data:
                    self.fail(
                        message=f"Missing 'composite_key' for feed id {json_dict['feed_id']}",
                        original_exception=None
                    )
                if not isinstance(value, list):
                    self.fail(
                        message=f"'composite_key' must be an list for feed id {json_dict['feed_id']}",
                        original_exception=None
                    )
                table_name = data["source_table_name"]
                if context.spark.catalog.tableExists(table_name):
                    table_columns = context._get_table_columns(data, self.name)
                    for key in value:
                        if not isinstance(key, str):
                            self.fail(
                                message=f"'composite_key' must contain only strings for feed id {json_dict['feed_id']}",
                                original_exception=None
                            )
                        if key not in table_columns:
                            self.fail(
                                message=f"'composite_key' column '{key}' not found in table "
                                        f"'{data['source_table_name']}'  for feed id {json_dict['feed_id']}",
                                original_exception=None
                            )
