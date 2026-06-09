"""Structured step logging for HanduFlow pipeline runs."""

from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from typing import Any, Iterator


def fmt_ctx(**kwargs: Any) -> str:
    parts = []
    for key, value in kwargs.items():
        if value is None:
            continue
        parts.append(f"{key}={value}")
    return " | ".join(parts)


def log_step(
    logger: logging.Logger,
    step: str,
    *,
    status: str,
    exc_info: bool = False,
    **context: Any,
) -> None:
    """Log a pipeline step with uniform ``[STEP]`` prefix."""
    ctx = fmt_ctx(**context)
    msg = f"[STEP] {step} | status={status}"
    if ctx:
        msg = f"{msg} | {ctx}"
    if status in ("FAIL", "ERROR"):
        logger.error(msg, exc_info=exc_info)
    elif status in ("SKIP", "WARN"):
        logger.warning(msg)
    else:
        logger.info(msg)


@contextmanager
def step(
    logger: logging.Logger,
    step_name: str,
    *,
    reraise: bool = True,
    **context: Any,
) -> Iterator[None]:
    """Context manager: log START, OK (with duration), or FAIL on exception."""
    log_step(logger, step_name, status="START", **context)
    start = time.perf_counter()
    try:
        yield
    except Exception as exc:
        elapsed = round(time.perf_counter() - start, 2)
        log_step(
            logger,
            step_name,
            status="FAIL",
            duration_sec=elapsed,
            error_type=type(exc).__name__,
            error=str(exc),
            exc_info=True,
            **context,
        )
        if reraise:
            raise
    else:
        elapsed = round(time.perf_counter() - start, 2)
        log_step(logger, step_name, status="OK", duration_sec=elapsed, **context)


def log_feed_event(
    logger: logging.Logger,
    event: str,
    *,
    feed_id: Any,
    feed_name: str | None = None,
    load_type: str | None = None,
    **extra: Any,
) -> None:
    """Log a feed-scoped event."""
    log_step(
        logger,
        event,
        status=extra.pop("status", "INFO"),
        feed_id=feed_id,
        feed_name=feed_name,
        load_type=load_type,
        **extra,
    )
