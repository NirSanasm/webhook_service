"""
Structured JSON logging setup.

Every log record includes: timestamp, level, service_name, environment,
trace_id, span_id, plus any context-specific fields added via extra={}.

Sensitive fields (payloads, credentials, tokens) must NEVER be logged.
"""
import logging
import sys
from typing import Any

from pythonjsonlogger.json import JsonFormatter as _JsonFormatter

from app.core.config import get_settings

settings = get_settings()

# Fields to always suppress from logs
_SENSITIVE_FIELDS = frozenset(
    {"password", "token", "secret", "authorization", "api_key", "payload"}
)


class SanitizingFilter(logging.Filter):
    """Remove sensitive fields from log records."""

    def filter(self, record: logging.LogRecord) -> bool:
        for field in _SENSITIVE_FIELDS:
            if hasattr(record, field):
                setattr(record, field, "[REDACTED]")
        return True


class ContextualFormatter(_JsonFormatter):
    """JSON formatter that adds standard service-level fields."""

    def add_fields(
        self,
        log_record: dict[str, Any],
        record: logging.LogRecord,
        message_dict: dict[str, Any],
    ) -> None:
        super().add_fields(log_record, record, message_dict)
        log_record["service_name"] = settings.app_name
        log_record["environment"] = settings.environment

        # Inject OpenTelemetry trace context if available
        try:
            from opentelemetry import trace

            span = trace.get_current_span()
            if span and span.is_recording():
                ctx = span.get_span_context()
                log_record["trace_id"] = format(ctx.trace_id, "032x")
                log_record["span_id"] = format(ctx.span_id, "016x")
        except Exception:
            pass


def configure_logging(service_name: str | None = None) -> None:
    """
    Configure the root logger with structured JSON output.
    Call once at application startup.
    """
    log_level = getattr(logging, settings.log_level.upper(), logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    handler.addFilter(SanitizingFilter())

    if settings.log_format == "json":
        fmt = ContextualFormatter(
            fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )
        handler.setFormatter(fmt)
    else:
        handler.setFormatter(
            logging.Formatter("%(asctime)s | %(levelname)-8s | %(name)s | %(message)s")
        )

    root = logging.getLogger()
    root.setLevel(log_level)
    root.handlers = [handler]

    # Silence noisy third-party loggers
    for noisy in ("aio_pika", "aiormq", "asyncio", "uvicorn.access"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """Return a named logger. Call configure_logging() first."""
    return logging.getLogger(name)

