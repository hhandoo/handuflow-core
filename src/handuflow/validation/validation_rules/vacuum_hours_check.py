from handuflow.validation.validation_rule import ValidationRule
from handuflow.validation.validation_context import ValidationContext
class VacuumHoursCheck(ValidationRule):
    name = "Vacuum Hours Check"
    error_code = "HF010"

    def validate(self, context: ValidationContext):

        if context.mdf_feed_specs_array is None:
            self.fail(
                message="JSON list has not been parsed yet",
                original_exception=None
            )
        
        for json_dict in context.mdf_feed_specs_array:

            if json_dict['data_flow_direction'] != 'SOURCE_TO_BRONZE':

                value = json_dict['feed_specs_dict'].get("vacuum_hours")
                if not isinstance(value, int):
                    self.fail(
                        message=f"'vacuum_hours' must be an integer for feed id {json_dict['feed_id']}",
                        original_exception=None
                    )
