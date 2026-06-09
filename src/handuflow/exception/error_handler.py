"""
Central error wrapping and structured error records for HanduFlow.
"""

from __future__ import annotations

import traceback
from typing import Any

from handuflow.exception.base_exception import BaseException as HanduFlowError
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
    format_error_label,
    get_error_description,
)


def resolve_error_code(exc: Exception) -> str:
    if isinstance(exc, HanduFlowError) and exc.error_code:
        return exc.error_code
    return DEFAULT_ERROR_CODE


def wrap_exception(
    exc: Exception,
    *,
    error_code: str | None = None,
    feed_id: int | str | None = None,
    context: dict[str, Any] | None = None,
    message: str | None = None,
) -> HanduFlowError:
    """
    Convert any exception into a HanduFlow error. Never re-raises raw exceptions.
    """
    if isinstance(exc, HanduFlowError):
        if feed_id is not None and exc.feed_id is None:
            exc.feed_id = feed_id
        if context:
            exc.context = {**(exc.context or {}), **context}
        if error_code and exc.error_code == DEFAULT_ERROR_CODE:
            exc.error_code = error_code
        return exc

    code = error_code or DEFAULT_ERROR_CODE
    msg = message or str(exc) or exc.__class__.__name__
    merged_context = dict(context or {})
    if feed_id is not None:
        merged_context.setdefault("feed_id", feed_id)

    wrapper_cls = _wrapper_for_code(code)
    return wrapper_cls(
        message=msg,
        error_code=code,
        feed_id=feed_id,
        context=merged_context,
        original_exception=exc,
        log=False,
    )


def exception_to_record(
    exc: Exception,
    *,
    phase: str | None = None,
    feed_id: int | str | None = None,
    error_code: str | None = None,
) -> dict[str, Any]:
    """Structured error dict for RunResult.phase_errors, logs, and APIs."""
    wrapped = wrap_exception(
        exc,
        error_code=error_code,
        feed_id=feed_id,
        context={"phase": phase} if phase else None,
    )
    record = wrapped.to_dict()
    if phase:
        record["phase"] = phase
    return record


def exception_message(exc: Exception | None, *, include_code: bool = True) -> str | None:
    if exc is None:
        return None
    if isinstance(exc, HanduFlowError):
        text = exc.short_message()
    else:
        wrapped = wrap_exception(exc, log=False)
        text = wrapped.short_message()
    if not include_code:
        return text[:8000]
    code = resolve_error_code(exc if isinstance(exc, HanduFlowError) else wrap_exception(exc, log=False))
    label = format_error_label(code)
    body = text[:8000] if text else repr(exc)
    return f"{label} — {body}"[:8000]


def _wrapper_for_code(code: str) -> type[HanduFlowError]:
    try:
        num = int(code[2:])
    except (ValueError, IndexError):
        return SystemError
    if 1 <= num <= 19:
        return ValidationError
    if 20 <= num <= 29:
        return ConfigError
    if 30 <= num <= 49:
        return DataLoadException
    if 50 <= num <= 59:
        return ExtractionException
    if 60 <= num <= 69:
        return StorageFetchException
    if 70 <= num <= 79:
        return DataQualityException
    if 80 <= num <= 89:
        return ResultGenerationException
    return SystemError
