# inbuilt
import logging
import pandas as pd
from typing import Sequence

# internal
from handuflow.exception.base_exception import BaseException as HanduFlowError
from handuflow.exception.error_handler import resolve_error_code, wrap_exception
from handuflow.validation.validation_rule import ValidationRule
from handuflow.validation.validation_context import ValidationContext
from handuflow.validation.validation_result import ValidationResult


class Validator:
    def __init__(self, rules: Sequence[ValidationRule], fail_fast: bool = True):
        self.rules = rules
        self.fail_fast = fail_fast
        self.logger = logging.getLogger(__name__)

    def validate(self, context: ValidationContext) -> ValidationResult:
        self.logger.info(
            "System validation starting: %s rule(s), fail_fast=%s",
            len(self.rules),
            self.fail_fast,
        )
        errors = []
        rows = []
        for idx, rule in enumerate(self.rules, start=1):
            try:
                rule.validate(context)
                self.logger.info(f"Rule {idx}: {rule.name} — PASSED")
                rows.append(
                    {
                        "rule_index": idx,
                        "rule_name": rule.name,
                        "status": "PASSED",
                        "error_message": None,
                    }
                )
            except Exception as e:
                if isinstance(e, HanduFlowError):
                    err = e
                else:
                    err = wrap_exception(
                        e,
                        error_code=getattr(rule, "error_code", None),
                    )
                self.logger.error(
                    f"Rule {idx}: {rule.name} — FAILED [{err.error_code}]: {err.short_message()}",
                    exc_info=True,
                )
                errors.append(err)
                rows.append(
                    {
                        "rule_index": idx,
                        "rule_name": rule.name,
                        "status": "FAILED",
                        "error_code": resolve_error_code(err),
                        "error_message": err.short_message(),
                    }
                )
                if self.fail_fast:
                    break
        results_df = pd.DataFrame(rows)
        result = ValidationResult(
            passed=len(errors) == 0,
            passed_rules=len(rows) - len(errors),
            total_rules=len(self.rules),
            errors=errors,
            results_df=results_df,
        )
        self._log_summary(result)
        return result

    def _log_summary(self, result: ValidationResult):
        if result.passed:
            self.logger.info(
                f"SYSTEM VALIDATION SUCCESS: {result.passed_rules}/{result.total_rules} "
                f"INTEGRITY ({result.score:.2f}%)"
            )
        else:
            self.logger.error(
                f"SYSTEM VALIDATION FAILED: {result.passed_rules}/{result.total_rules} "
                f"INTEGRITY ({result.score:.2f}%)"
            )
