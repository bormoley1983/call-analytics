"""
Centralised logging configuration for call-analytics.

Provides structured JSON logging, Elasticsearch integration, and context
correlation for production deployments while maintaining human-readable
console output for development.

Environment variables
---------------------
LOG_LEVEL            : Logging level name (default: INFO).
LOG_FORMAT           : ``text`` or ``json`` for console handler (default: text).
LOG_FILE             : Path to a rotating JSON log file (default: logs/app.json).
LOG_MAX_BYTES        : Max bytes per log file before rotation (default: 10 MiB).
LOG_BACKUP_COUNT     : Number of rotated log files to keep (default: 5).
LOG_ES_URL           : Elasticsearch URL for remote logging (optional).
LOG_ES_INDEX         : Elasticsearch index prefix (default: call-analytics).
LOG_ES_LEVEL         : Minimum level sent to Elasticsearch (default: ERROR).
LOG_ES_USERNAME      : Elasticsearch username for authentication (optional).
LOG_ES_PASSWORD      : Elasticsearch password for authentication (optional).
LOG_CORRELATION_ID   : Header name for correlation ID in requests (default: X-Correlation-Id).
ENVIRONMENT          : ``production`` or ``development`` (default: development).

Call setup_logging() exactly once, at the application entry point, before
any other module is imported (or at least before any log messages are emitted).

Examples
--------
Development (human-readable console):

    export LOG_LEVEL=DEBUG
    python src/cli.py

Production (JSON files + Elasticsearch for errors):

    export LOG_FILE=/var/log/call-analytics/app.json
    export LOG_ES_URL=https://es-host:9200
    export LOG_ES_LEVEL=ERROR
    export ENVIRONMENT=production
    uvicorn api.app:app --host 0.0.0.0 --port 8000
"""

from __future__ import annotations

import contextvars
import json
import logging
import logging.handlers
import os
import queue
import sys
import threading
import traceback
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pythonjsonlogger import jsonlogger  # type: ignore[import-not-found]

# Runtime optional imports for structured logging and Elasticsearch
_JSONLOGGER_AVAILABLE = False
try:
    from pythonjsonlogger import jsonlogger  # type: ignore[import-not-found]

    _JSONLOGGER_AVAILABLE = True
except ImportError:
    jsonlogger = None  # type: ignore[assignment]

_ES_AVAILABLE = False
_ElasticsearchClass: type[Any] | None = None
try:
    from elasticsearch import (
        Elasticsearch as _ElasticsearchClass,  # type: ignore[import-not-found]
    )

    _ES_AVAILABLE = True
except ImportError:
    _ElasticsearchClass = None


_DEFAULT_FORMAT = "%(asctime)s %(levelname)-8s %(name)s: %(message)s"
_DEFAULT_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
_LIBRARY_DEFAULT_LEVELS = {
    "urllib3": "WARNING",
    "httpcore": "WARNING",
    "httpx": "WARNING",
    "paramiko": "WARNING",
    "ctranslate2": "WARNING",
    "faster_whisper": "INFO",
}

# Thread-safe correlation ID context variable
_correlation_id: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "correlation_id", default=None
)


class JsonFormatter(jsonlogger.JsonFormatter if jsonlogger else logging.Formatter):  # type: ignore[misc]
    """Structured JSON formatter for machine-readable logs.

    Adds exception details, correlation IDs, and timestamps in ISO format.
    Falls back to standard logging.Formatter if python-json-logger is unavailable.
    """

    def __init__(self) -> None:
        if jsonlogger:
            super().__init__(
                fmt="%(timestamp)s %(level)s %(logger)s %(message)s %(module)s %(function)s %(lineno)d",
                datefmt="%Y-%m-%dT%H:%M:%S",
            )
        else:
            super().__init__()

    def format(self, record: logging.LogRecord) -> str:
        log_data: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # Add exception info if present
        if record.exc_info and record.exc_info[0] is not None:
            log_data["exception"] = {
                "type": record.exc_info[0].__name__,
                "message": str(record.exc_info[1]),
                "traceback": "".join(traceback.format_exception(*record.exc_info)),
            }

        # Add correlation ID if available
        correlation_id = _correlation_id.get()
        if correlation_id:
            log_data["correlation_id"] = correlation_id

        # Add any extra fields from the record
        for key, value in record.__dict__.items():
            if key not in (
                "name",
                "msg",
                "args",
                "created",
                "levelname",
                "levelno",
                "pathname",
                "filename",
                "module",
                "exc_info",
                "exc_text",
                "stack_info",
                "lineno",
                "funcName",
                "thread",
                "threadName",
                "process",
                "processName",
                "message",
                "relativeCreated",
                "correlation_id",  # Already handled above
            ):
                log_data[key] = value

        return json.dumps(log_data, default=str)


class ContextFilter(logging.Filter):
    """Inject correlation IDs and additional context into log records."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.correlation_id = _correlation_id.get()
        return True


class ErrorOnlyFilter(logging.Filter):
    """Only pass ERROR and above to the handler.

    Useful for Elasticsearch to reduce costs by only sending errors.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno >= logging.ERROR


def _level_from_name(name: str, fallback: int) -> int:
    return getattr(logging, name.upper(), fallback)


def _configure_library_levels() -> None:
    default_level_name = os.getenv("LOG_LEVEL_LIBRARIES", "").upper()
    default_level = (
        _level_from_name(default_level_name, logging.NOTSET)
        if default_level_name
        else None
    )

    for logger_name, level_name in _LIBRARY_DEFAULT_LEVELS.items():
        env_name = f"LOG_LEVEL_{logger_name.upper().replace('.', '_')}"
        resolved_name = os.getenv(env_name, default_level_name or level_name)
        resolved_level = (
            default_level
            if default_level is not None and env_name not in os.environ
            else None
        )
        logging.getLogger(logger_name).setLevel(
            resolved_level
            if resolved_level is not None
            else _level_from_name(resolved_name, logging.INFO)
        )


def _create_console_handler() -> logging.Handler:
    """Create a console handler with text or JSON formatting."""
    handler = logging.StreamHandler(sys.stdout)

    log_format = os.getenv("LOG_FORMAT", "text").lower()
    if log_format == "json" and jsonlogger:
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(
            logging.Formatter(_DEFAULT_FORMAT, datefmt=_DEFAULT_DATE_FORMAT)
        )

    # Add context filter
    handler.addFilter(ContextFilter())

    return handler


def _create_file_handler() -> logging.Handler | None:
    """Create a rotating JSON file handler as failsafe logging."""
    log_file = os.getenv("LOG_FILE")

    # Default to logs/app.json if not specified (ensure directory exists)
    if not log_file:
        log_dir = os.path.join(os.getcwd(), "logs")
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, "app.json")

    max_bytes = int(os.getenv("LOG_MAX_BYTES", str(10 * 1024 * 1024)))  # 10 MiB
    backup_count = int(os.getenv("LOG_BACKUP_COUNT", "5"))

    handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )

    # Always use JSON format for file logs
    if jsonlogger:
        handler.setFormatter(JsonFormatter())
    else:
        # Fallback to text format if python-json-logger is not available
        handler.setFormatter(
            logging.Formatter(_DEFAULT_FORMAT, datefmt=_DEFAULT_DATE_FORMAT)
        )

    # Add context filter
    handler.addFilter(ContextFilter())

    return handler


class ElasticsearchHandler(logging.Handler):
    """Custom logging handler that sends logs to Elasticsearch.

    Uses a background thread and queue to avoid blocking the main application.
    Only sends logs at or above the configured level (default: ERROR).
    """

    def __init__(
        self,
        es_url: str,
        index_prefix: str = "call-analytics",
        level: int = logging.ERROR,
        username: str | None = None,
        password: str | None = None,
        queue_size: int = 1024,
    ) -> None:
        super().__init__(level)
        self.es_url = es_url
        self.index_prefix = index_prefix
        self.username = username
        self.password = password
        self.queue_size = queue_size

        # Background queue for async processing
        self._queue: queue.Queue[str] = queue.Queue(maxsize=queue_size)
        self._stop_event = threading.Event()
        self._worker_thread = threading.Thread(target=self._worker, daemon=True)
        self._worker_thread.start()

        # Initialize Elasticsearch client
        self.es: Any = None
        try:
            if _ElasticsearchClass is None:
                print("Warning: elasticsearch package not installed", file=sys.stderr)
                return

            api_key = None
            headers: dict[str, str] = {}

            if username and password:
                headers["Authorization"] = f"Basic {username}:{password}"

            self.es = _ElasticsearchClass(
                [es_url],
                api_key=api_key,
                headers=headers if headers else None,
            )
        except (ValueError, OSError) as e:
            print(f"Warning: Failed to connect to Elasticsearch: {e}", file=sys.stderr)
            self.es = None

    def _worker(self) -> None:
        """Background worker that processes log messages from the queue."""
        while not self._stop_event.is_set():
            try:
                # Wait for item with timeout to check stop event periodically
                try:
                    log_data = self._queue.get(timeout=1.0)
                except queue.Empty:
                    continue

                if self.es is None:
                    continue

                # Parse JSON and send to Elasticsearch
                try:
                    data = json.loads(log_data)
                    timestamp = data.get(
                        "timestamp", datetime.now(timezone.utc).isoformat()
                    )
                    date_part = (
                        timestamp[:10]
                        if len(timestamp) >= 10
                        else datetime.now(timezone.utc).strftime("%Y.%m.%d")
                    )
                    index = f"{self.index_prefix}-{date_part}"

                    self.es.index(
                        index=index,
                        document=data,
                        timeout="5s",
                    )
                except json.JSONDecodeError:
                    # If not JSON, skip (shouldn't happen with our formatter)
                    pass
                except TimeoutError:
                    # ES connection timeout - drop the message to prevent queue buildup
                    pass
                except (OSError, RuntimeError, ValueError, TypeError):
                    # Any other error - drop the message
                    pass
                finally:
                    self._queue.task_done()
            except (OSError, RuntimeError, ValueError, TypeError):
                # Unexpected error in worker - continue processing
                pass

    def emit(self, record: logging.LogRecord) -> None:
        """Add log record to the queue for background processing."""
        try:
            log_data = self.format(record)
            # Non-blocking put - drop message if queue is full
            self._queue.put_nowait(log_data)
        except queue.Full:
            # Queue is full - drop the message to prevent blocking
            pass
        except (OSError, RuntimeError, ValueError, TypeError):
            # Any other error - silently ignore to prevent log crashes
            pass

    def close(self) -> None:
        """Stop the background worker and flush remaining messages."""
        self._stop_event.set()
        self._worker_thread.join(timeout=5.0)
        super().close()


def _create_elasticsearch_handler() -> logging.Handler | None:
    """Create an Elasticsearch handler for remote logging.

    Returns None if LOG_ES_URL is not configured or elastic-transport is unavailable.
    """
    es_url = os.getenv("LOG_ES_URL")
    if not es_url or not _ES_AVAILABLE:
        return None

    try:
        index_prefix = os.getenv("LOG_ES_INDEX", "call-analytics")
        es_level = _level_from_name(os.getenv("LOG_ES_LEVEL", "ERROR"), logging.ERROR)

        es_username = os.getenv("LOG_ES_USERNAME")
        es_password = os.getenv("LOG_ES_PASSWORD")

        handler = ElasticsearchHandler(
            es_url=es_url,
            index_prefix=index_prefix,
            level=es_level,
            username=es_username,
            password=es_password,
        )

        # Use JSON formatter for Elasticsearch
        if _JSONLOGGER_AVAILABLE:
            handler.setFormatter(JsonFormatter())
        else:
            handler.setFormatter(
                logging.Formatter(_DEFAULT_FORMAT, datefmt=_DEFAULT_DATE_FORMAT)
            )

        # Add filters
        handler.addFilter(ContextFilter())
        handler.addFilter(ErrorOnlyFilter())

        return handler

    except (ValueError, OSError, RuntimeError) as e:
        # Log warning but don't fail if ES handler can't be created
        print(f"Warning: Failed to create Elasticsearch handler: {e}", file=sys.stderr)
        return None


def setup_logging() -> None:
    """Configure application-wide logging from environment variables.

    Sets up multiple handlers based on environment:
    - Console handler (text or JSON format)
    - File handler (JSON format, always enabled as failsafe)
    - Elasticsearch handler (if LOG_ES_URL is configured)
    """
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    handlers: list[logging.Handler] = []

    # Always add console handler for local output
    handlers.append(_create_console_handler())

    # Add file handler (failsafe - always enabled)
    file_handler = _create_file_handler()
    if file_handler:
        handlers.append(file_handler)

    # Add Elasticsearch handler if configured
    es_handler = _create_elasticsearch_handler()
    if es_handler:
        handlers.append(es_handler)

    # force=True replaces any handlers added by imported libraries (e.g. faster-whisper)
    logging.basicConfig(
        level=level,
        format=_DEFAULT_FORMAT,  # Default format; individual handlers override
        datefmt=_DEFAULT_DATE_FORMAT,
        handlers=handlers,
        force=True,
    )

    _configure_library_levels()


def set_correlation_id(correlation_id: str | None) -> contextvars.Token[str | None]:
    """Set the correlation ID for the current context.

    Parameters
    ----------
    correlation_id : str or None
        The correlation ID to set for this request/context.

    Returns
    -------
    contextvars.Token
        A token that can be used to restore the previous state.

    Examples
    --------
    >>> token = set_correlation_id("req-123")
    >>> logger.info("This log will have correlation_id=req-123")
    >>> _correlation_id.reset(token)  # Restore previous state
    """
    return _correlation_id.set(correlation_id)


def clear_correlation_id() -> None:
    """Clear the current correlation ID.

    Use this to reset the correlation ID after a request completes.
    """
    _correlation_id.set(None)
