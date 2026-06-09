from handuflow.exception.base_exception import BaseException


class ConfigError(BaseException):
    def __init__(self, message=None, *, error_code: str = "HF020", **kwargs):
        super().__init__(message, error_code=error_code, **kwargs)
