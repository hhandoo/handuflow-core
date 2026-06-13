from handuflow.constants import is_within_unity_catalog_direction
from handuflow.validation.validation_rule import ValidationRule
from handuflow.validation.validation_context import ValidationContext
class PrimaryKey(ValidationRule):
    name = "Feed spec primary key check"
    error_code = "HF004"

    def validate(self, context: ValidationContext):
        if context.mdf_feed_specs_array is None:
            self.fail(
                message="JSON list has not been parsed yet",
                original_exception=None
            )
        
        
        for json_dict in context.mdf_feed_specs_array:

            if is_within_unity_catalog_direction(json_dict["data_flow_direction"]):

                feed_specs_dict = json_dict['feed_specs_dict']

                if "primary_key" not in feed_specs_dict:
                    self.fail(
                        message=f"Missing 'primary_key' for feed id {json_dict['feed_id']}",
                        original_exception=None
                    )

                pk = feed_specs_dict["primary_key"]
                if not isinstance(pk, str) or not pk.strip():
                    self.fail(
                        message=f"'primary_key' must be a non-empty string for feed id {json_dict['feed_id']}",
                        original_exception=None
                    )
                
                if context.spark.catalog.tableExists(feed_specs_dict['source_table_name']):

                    table_columns = context._get_table_columns(feed_specs_dict, self.name)
                    if pk not in table_columns:
                        self.fail(
                            message=f"primary_key '{pk}' not found in table '{feed_specs_dict['source_table_name']}' for feed id {json_dict['feed_id']}",
                            original_exception=None
                        )                
        


