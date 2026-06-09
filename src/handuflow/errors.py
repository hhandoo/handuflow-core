"""
HanduFlow errors — stable alias for :mod:`handuflow.exception`.

Use either::

    from handuflow.errors import ConfigError, wrap_exception
    from handuflow import ConfigError, wrap_exception
"""

from handuflow.exception import (
    DEFAULT_ERROR_CODE,
    ERROR_CODES,
    VALIDATION_RULE_CODES,
    BaseException,
    ConfigError,
    DataLoadException,
    DataQualityException,
    ExtractionException,
    ResultGenerationException,
    StorageFetchException,
    SystemError,
    ValidationError,
    exception_message,
    exception_to_record,
    format_error_label,
    get_error_category,
    get_error_description,
    resolve_error_code,
    wrap_exception,
)

__all__ = [
    "DEFAULT_ERROR_CODE",
    "ERROR_CODES",
    "VALIDATION_RULE_CODES",
    "BaseException",
    "ConfigError",
    "ValidationError",
    "DataLoadException",
    "DataQualityException",
    "ExtractionException",
    "StorageFetchException",
    "ResultGenerationException",
    "SystemError",
    "wrap_exception",
    "exception_to_record",
    "exception_message",
    "format_error_label",
    "get_error_description",
    "get_error_category",
    "resolve_error_code",
]
