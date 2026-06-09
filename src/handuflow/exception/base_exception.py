# inbuilt
import sys
import traceback
import logging

from handuflow.exception.error_codes import (
    DEFAULT_ERROR_CODE,
    format_error_label,
    get_error_description,
)


class BaseException(Exception):
    """
    Unified base exception for all HanduFlow pipeline errors.
    Automatically logs a clean, human-readable error block.
    """

    def __init__(
        self,
        message=None,
        details=None,
        context=None,
        original_exception=None,
        log=True,
        *,
        error_code: str | None = None,
        feed_id: int | str | None = None,
    ):
        self.error_code = error_code or DEFAULT_ERROR_CODE
        self.feed_id = feed_id
        self.message = message or get_error_description(self.error_code)
        super().__init__(self.message)

        self.details = details
        self.context = context or {}
        self.original_exception = original_exception

        exc_type, exc_value, exc_tb = sys.exc_info()
        self.exc_type = exc_type.__name__ if exc_type else None
        self.exc_value = str(exc_value) if exc_value else None
        self.full_traceback = (
            "".join(traceback.format_exception(exc_type, exc_value, exc_tb))
            if exc_type
            else None
        )

        self.logger = logging.getLogger(__name__)

        if log:
            self.logger.error(self.to_pretty_text())

    def short_message(self) -> str:
        """Single-line message for LoadResult and reports."""
        return self.message or get_error_description(self.error_code)

    def __str__(self):
        return f"{format_error_label(self.error_code)} — {self.short_message()}"

    def to_pretty_text(self):
        return f"""
        ==================== HANDUFLOW ERROR ====================

        Error Code:
        {self.error_code}

        Error:
        {format_error_label(self.error_code)}

        Error Type:
        {self.__class__.__name__}

        Message:
        {self.message}

        Feed ID:
        {self.feed_id if self.feed_id is not None else "N/A"}

        -------------------- DETAILS --------------------
        {self._format_block(self.details)}

        -------------------- CONTEXT --------------------
        {self._format_block(self.context)}

        ------------- ORIGINAL EXCEPTION ---------------
        {self._format_block(repr(self.original_exception) if self.original_exception else None)}

        ------------------ STACK TRACE ------------------
        {self._format_block(self.full_traceback)}

        =================================================
        """.strip()

    def to_dict(self):
        """Structured error payload for APIs, MLflow, or persistence."""
        return {
            "error_code": self.error_code,
            "error_label": format_error_label(self.error_code),
            "error_description": get_error_description(self.error_code),
            "error_type": self.__class__.__name__,
            "message": self.message,
            "feed_id": self.feed_id,
            "details": self.details,
            "context": self.context,
            "original_exception": repr(self.original_exception)
            if self.original_exception
            else None,
            "exception_type": self.exc_type,
            "exception_message": self.exc_value,
            "traceback": self.full_traceback,
        }

    @staticmethod
    def _format_block(value):
        if value in (None, "", {}, []):
            return "N/A"
        return value
