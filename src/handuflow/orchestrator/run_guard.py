# inbuilt
import logging
import time
from collections.abc import Callable
from typing import Any, TypeVar

from handuflow.config.run_logger import log_step
from handuflow.exception.error_codes import DEFAULT_ERROR_CODE
from handuflow.exception.error_handler import exception_to_record, resolve_error_code

T = TypeVar("T")

logger = logging.getLogger(__name__)


def run_phase(
    phase_name: str,
    errors: list[dict],
    fn: Callable[[], T],
    *,
    default: T | None = None,
    reraise: bool = False,
    **context: Any,
) -> T | None:
    """
    Execute a pipeline phase; record failures without stopping the run unless reraise=True.
    """
    log_step(logger, f"phase.{phase_name}", status="START", **context)
    start = time.perf_counter()
    try:
        result = fn()
        elapsed = round(time.perf_counter() - start, 2)
        log_step(
            logger, f"phase.{phase_name}", status="OK", duration_sec=elapsed, **context
        )
        return result
    except Exception as exc:
        elapsed = round(time.perf_counter() - start, 2)
        error_code = resolve_error_code(exc)
        if error_code == DEFAULT_ERROR_CODE:
            error_code = "HF092"
        log_step(
            logger,
            f"phase.{phase_name}",
            status="FAIL",
            duration_sec=elapsed,
            error_code=error_code,
            error_type=type(exc).__name__,
            error=str(exc),
            exc_info=True,
            **context,
        )
        errors.append(
            exception_to_record(exc, phase=phase_name, error_code=error_code)
        )
        if reraise:
            raise
        return default
