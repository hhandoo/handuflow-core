from handuflow.constants import is_within_unity_catalog_direction
from handuflow.validation.validation_rule import ValidationRule
from handuflow.validation.validation_context import ValidationContext
class StandardCheckStructureCheck(ValidationRule):
    name = "Feed spec standard checks structure check"
    error_code = "HF009"

    def validate(self, context: ValidationContext):

        if context.mdf_feed_specs_array is None:
            self.fail(
                message="JSON has not been parsed yet",
                original_exception=None
            )
        

        for json_dict in context.mdf_feed_specs_array:
            if is_within_unity_catalog_direction(json_dict["data_flow_direction"]):
                data = json_dict['feed_specs_dict']
                if data is None:
                    self.fail(
                        message=f"JSON has not been parsed yet for feed id {json_dict['feed_id']}",
                        original_exception=None
                    )
                required_fields = ["check_sequence", "column_name", "threshold"]
                for idx, check in enumerate(data["standard_checks"]): 
                    if not isinstance(check, dict):
                        self.fail(
                            message=f"checks[{idx}] must be an object for feed id {json_dict['feed_id']}",
                            original_exception=None
                        )
                    for f in required_fields:
                        if f not in check:
                            self.fail(
                                message=f"checks[{idx}] missing '{f}' for feed id {json_dict['feed_id']}",
                                original_exception=None
                            )
                    if not isinstance(check["check_sequence"], list):
                        self.fail(
                            message=f"checks[{idx}].check_sequence must be a list for feed id {json_dict['feed_id']}",
                            original_exception=None
                        )
                    for seq in check["check_sequence"]:
                        if not isinstance(seq, str):
                            self.fail(
                                message=f"checks[{idx}].check_sequence must contain only strings for feed id {json_dict['feed_id']}",
                                original_exception=None
                            )
                    if not (isinstance(check["column_name"], str) or isinstance(check["column_name"], list)):
                        self.fail(
                            message=f"checks[{idx}].column_name must be a string for feed id {json_dict['feed_id']}",
                            original_exception=None
                        )
                    if not isinstance(check["threshold"], int):
                        self.fail(
                            message=f"checks[{idx}].threshold must be an integer for feed id {json_dict['feed_id']}",
                            original_exception=None
                        )
            