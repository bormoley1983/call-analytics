# -*- coding: utf-8 -*-
"""
Centralised logging configuration for call-analytics.

Environment variables
---------------------
LOG_LEVEL        : Logging level name (default: INFO).
LOG_FORMAT       : Log-line format string (default: structured text).
LOG_FILE         : Path to a rotating log file (optional).
LOG_MAX_BYTES    : Max bytes per log file before rotation (default: 10 MiB).
LOG_BACKUP_COUNT : Number of rotated log files to keep (default: 5).

Call setup_logging() exactly once, at the application entry point, before
any other module is imported (or at least before any log messages are emitted).
"""

import logging
import logging.handlers
import os
import sys

_DEFAULT_FORMAT = "%(asctime)s %(levelname)-8s %(name)s: %(message)s"
_DEFAULT_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def setup_logging() -> None:
    """Configure application-wide logging from environment variables."""
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    fmt = os.getenv("LOG_FORMAT", _DEFAULT_FORMAT)
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]

    log_file = os.getenv("LOG_FILE")
    if log_file:
        max_bytes = int(os.getenv("LOG_MAX_BYTES", str(10 * 1024 * 1024)))  # 10 MiB
        backup_count = int(os.getenv("LOG_BACKUP_COUNT", "5"))
        handlers.append(
            logging.handlers.RotatingFileHandler(
                log_file,
                maxBytes=max_bytes,
                backupCount=backup_count,
                encoding="utf-8",
            )
        )

    # force=True replaces any handlers added by imported libraries (e.g. faster-whisper)
    logging.basicConfig(
        level=level,
        format=fmt,
        datefmt=_DEFAULT_DATE_FORMAT,
        handlers=handlers,
        force=True,
    )
