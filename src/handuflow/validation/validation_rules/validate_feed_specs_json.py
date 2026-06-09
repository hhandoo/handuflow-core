import json
from handuflow.validation.validation_rule import ValidationRule
from handuflow.validation.validation_context import ValidationContext


class ValidateFeedSpecsJSON(ValidationRule):
    name = "Feed spec JSON format check"
    error_code = "HF001"

    def validate(self, context: ValidationContext):
        context.mdf_feed_specs_array = context.master_specs_dataframe[
            ["feed_id", "feed_specs", "data_flow_direction"]
        ].to_dict(orient="records")
        try:
            cnt = len(context.mdf_feed_specs_array)
            for i in range(cnt):
                json_str_dict = json.loads(
                    context.mdf_feed_specs_array[i]["feed_specs"]
                )
                context.mdf_feed_specs_array[i]["feed_specs_dict"] = json_str_dict
        except json.JSONDecodeError as e:
            self.fail(
                message="Invalid JSON format, please make sure the JSON string provided is a valid JSON.",
                original_exception=e,
            )
