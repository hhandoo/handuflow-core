import pandas as pd
from typing import List
from handuflow.exception.validation_error import ValidationError

class ValidationResult:
    def __init__(
        self,
        passed: bool,
        passed_rules: int,
        total_rules: int,
        errors: List[ValidationError],
        results_df: pd.DataFrame
    ):
        self.passed = passed
        self.passed_rules = passed_rules
        self.total_rules = total_rules
        self.errors = errors
        self.results_df = results_df

    @property
    def score(self) -> float:
        return (self.passed_rules / self.total_rules) * 100 if self.total_rules else 0.0
