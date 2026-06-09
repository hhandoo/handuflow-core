"""HanduFlow exception hierarchy and error code registry."""

from handuflow.exception.base_exception import BaseException
from handuflow.exception.config_error import ConfigError
from handuflow.exception.data_load_exception import DataLoadException
from handuflow.exception.data_quality_exception import DataQualityException
from handuflow.exception.extraction_exception import ExtractionException
from handuflow.exception.result_generation_exception import ResultGenerationException
from handuflow.exception.storage_fetch_exception import StorageFetchException
from handuflow.exception.system_error import SystemError
from handuflow.exception.validation_error import ValidationError
from handuflow.exception.error_codes import (
    DEFAULT_ERROR_CODE,
    ERROR_CODES,
    VALIDATION_RULE_CODES,
    format_error_label,
    get_error_category,
    get_error_description,
)
from handuflow.exception.error_handler import (
    exception_message,
    exception_to_record,
    resolve_error_code,
    wrap_exception,
)

__all__ = [
    "BaseException",
    "ConfigError",
    "DataLoadException",
    "DataQualityException",
    "ExtractionException",
    "ResultGenerationException",
    "StorageFetchException",
    "SystemError",
    "ValidationError",
    "DEFAULT_ERROR_CODE",
    "ERROR_CODES",
    "VALIDATION_RULE_CODES",
    "format_error_label",
    "get_error_category",
    "get_error_description",
    "exception_message",
    "exception_to_record",
    "resolve_error_code",
    "wrap_exception",
]
