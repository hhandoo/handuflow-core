from abc import ABC, abstractmethod

from handuflow.exception.validation_error import ValidationError
from handuflow.exception.error_codes import DEFAULT_ERROR_CODE
from handuflow.validation.validation_context import ValidationContext


class ValidationRule(ABC):
    name: str
    error_code: str = DEFAULT_ERROR_CODE

    @abstractmethod
    def validate(self, context: ValidationContext):
        """Raise ValidationError on failure"""
        pass

    def fail(self, message: str, **kwargs) -> None:
        """Raise a ValidationError tagged with this rule's error code."""
        raise ValidationError(
            message=message,
            error_code=kwargs.pop("error_code", self.error_code),
            **kwargs,
        )
