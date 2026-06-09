import os
from handuflow.validation.validation_rule import ValidationRule
from handuflow.validation.validation_context import ValidationContext


class ValidateMasterSpecs(ValidationRule):
    name = "Master spec check"
    error_code = "HF003"

    def validate(self, context: ValidationContext):
        master_specs = os.path.join(context.file_hunt_path, context.master_spec_name)
        if not os.path.exists(master_specs):
            self.fail(
                message=(
                    f"System can't find the Master Specs File at [{context.file_hunt_path}]. "
                    "Terminating process"
                ),
            )
