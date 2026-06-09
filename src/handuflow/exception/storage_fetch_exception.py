from handuflow.exception.base_exception import BaseException


class StorageFetchException(BaseException):
    def __init__(self, message=None, *, error_code: str = "HF060", **kwargs):
        super().__init__(message, error_code=error_code, **kwargs)
