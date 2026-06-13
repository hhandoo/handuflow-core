from handuflow.constants import is_within_unity_catalog_direction
from handuflow.validation.validation_rule import ValidationRule
from handuflow.validation.validation_context import ValidationContext
class EnforceStandardChecks(ValidationRule):
    name = "Feed spec standard checks enforcement"
    error_code = "HF008"

    def validate(self, context: ValidationContext):

        if context.mdf_feed_specs_array is None:
            self.fail(
                message="JSON has not been parsed yet",
                original_exception=None
            )

        for json_dict in context.mdf_feed_specs_array:
            if is_within_unity_catalog_direction(json_dict["data_flow_direction"]):
                data = json_dict["feed_specs_dict"]
                if data is None:
                    self.fail(
                        message=f"JSON has not been parsed yet for feed id {json_dict['feed_id']}",
                        original_exception=None
                    )
                if "standard_checks" not in data:
                    self.fail(
                        message=f"Missing 'checks' for feed id {json_dict['feed_id']}",
                        original_exception=None
                    )

                if not isinstance(data["standard_checks"], list):
                    self.fail(
                        message=f"'standard_checks' must be a list for feed id {json_dict['feed_id']}",
                        original_exception=None
                    )
