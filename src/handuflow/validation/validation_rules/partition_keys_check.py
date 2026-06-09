from handuflow.validation.validation_rule import ValidationRule
from handuflow.validation.validation_context import ValidationContext
class PartitionKeysCheck(ValidationRule):
    name = "Partition keys check"
    error_code = "HF006"

    def validate(self, context: ValidationContext):

        if context.mdf_feed_specs_array is None:
            self.fail(
                message="JSON list has not been parsed yet",
                original_exception=None
            )
        
        for json_dict in context.mdf_feed_specs_array:

            if json_dict['data_flow_direction'] != 'SOURCE_TO_BRONZE':
                
                data = json_dict['feed_specs_dict']
                value = data.get("partition_keys")
                if "partition_keys" not in data:
                    self.fail(
                        message=f"Missing 'partition_keys' for feed id {json_dict['feed_id']}",
                        original_exception=None
                    )
                if not isinstance(value, list):
                    self.fail(
                        message=f"'partition_keys' must be an list for feed id {json_dict['feed_id']}",
                        original_exception=None
                    )

                table_name = data["source_table_name"]
                if context.spark.catalog.tableExists(table_name):
                    table_columns = context._get_table_columns(data, self.name)
                    for key in value:
                        if not isinstance(key, str):
                            self.fail(
                                message=f"'partition_keys' must contain only strings for feed id {json_dict['feed_id']}",
                                original_exception=None
                            )
                        if key not in table_columns:
                            self.fail(
                                message=f"'partition_keys' column '{key}' not found in table "
                                        f"'{data['source_table_name']}'  for feed id {json_dict['feed_id']}",
                                original_exception=None
                            )
