from handuflow.validation.validation_rule import ValidationRule
from handuflow.validation.validation_context import ValidationContext
class ComprehensiveChecksDependencyDatasetCheck(ValidationRule):
    name = "Feed spec comprehensive checks dependency dataset check"
    error_code = "HF011"

    def validate(self, context: ValidationContext):

        if context.mdf_feed_specs_array is None:
            self.fail(
                message="JSON has not been parsed yet",
                original_exception=None
            )

        for json_dict in context.mdf_feed_specs_array:
            if json_dict["data_flow_direction"] != "SOURCE_TO_BRONZE":
                data = json_dict["feed_specs_dict"]
                if data is None:
                    self.fail(
                        message=f"JSON has not been parsed yet for feed id {json_dict['feed_id']}",
                        original_exception=None
                    )
                if "comprehensive_checks" not in data:
                    return
                for idx, check in enumerate(data["comprehensive_checks"]):
                    deps = check.get("dependency_dataset", [])
                    if not isinstance(deps, list):
                        self.fail(
                            f"comprehensive_checks[{idx}].dependency_dataset must be a list for feed id {json_dict['feed_id']}",
                            original_exception=None
                        )
                    for dep in deps:
                        if not context.spark.catalog.tableExists(dep):
                            self.fail(
                                message=(
                                    f"comprehensive_checks[{idx}].dependency_dataset "
                                    f"table does not exist: '{dep}'"
                                    f"for feed id {json_dict['feed_id']}"
                                ),
                                original_exception=None
                            )
