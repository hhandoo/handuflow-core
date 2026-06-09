from handuflow.exception.base_exception import BaseException


class ValidationError(BaseException):
    def __init__(self, message=None, *, error_code: str = "HF013", **kwargs):
        super().__init__(message, error_code=error_code, **kwargs)
