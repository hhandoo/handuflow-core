# logging.py
import logging
import sys
from datetime import datetime, UTC

class LoggingPrettyFormatter(logging.Formatter):
    """Color-coded formatter for root logger."""

    COLORS = {
        "INFO": "\033[94m",      # blue
        "WARNING": "\033[93m",   # yellow
        "ERROR": "\033[91m",     # red
        "CRITICAL": "\033[95m",  # magenta
        "ENDC": "\033[0m",
    }

    
    def __init__(self, is_log_verbose: bool = True):
        super().__init__()
        self.is_log_verbose = is_log_verbose


    def format(self, record: logging.LogRecord) -> str:
        ts = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")

        # Disable colors if not a real terminal (Databricks, logs, files)
        if sys.stdout.isatty():
            color = self.COLORS.get(record.levelname, "")
            endc = self.COLORS["ENDC"]
        else:
            color = endc = ""

        message = record.getMessage()
        return f"{color}[{ts}] {f"[{record.name}]" if self.is_log_verbose else ""} [{record.levelname}] - {message}{endc}"