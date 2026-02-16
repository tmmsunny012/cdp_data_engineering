"""Structured JSON logging with automatic PII redaction for the CDP platform.

Configures *structlog* to emit JSON lines suitable for Google Cloud Logging
ingestion.  Every log event automatically carries a correlation ID, service
name, environment tag, and ISO-8601 timestamp.  A dedicated processor scans
every key/value pair and replaces values that look like PII (email addresses,
phone numbers, person names flagged via an explicit key list) with the literal
string ``[REDACTED]``.
"""

from __future__ import annotations

import logging
import os
import re
import uuid
from contextvars import ContextVar
from typing import Any

import structlog

# --------------------------------------------------------------------------- #
# Correlation-ID context                                                       #
# --------------------------------------------------------------------------- #
_correlation_id_ctx: ContextVar[str | None] = ContextVar(
    "correlation_id",
    default=None,
)


def get_correlation_id() -> str:
    """Return the current correlation ID, creating one if absent."""
    cid = _correlation_id_ctx.get()
    if cid is None:
        cid = uuid.uuid4().hex
        _correlation_id_ctx.set(cid)
    return cid


def set_correlation_id(cid: str) -> None:
    """Explicitly set the correlation ID (e.g. from an incoming header)."""
    _correlation_id_ctx.set(cid)


# --------------------------------------------------------------------------- #
# PII redaction processor                                                      #
# --------------------------------------------------------------------------- #
_EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
_PHONE_RE = re.compile(r"\+?\d[\d\-\s()]{7,}\d")
_PII_KEYS = frozenset(
    {
        "email",
        "email_address",
        "phone",
        "phone_number",
        "mobile",
        "first_name",
        "last_name",
        "full_name",
        "name",
        "student_name",
        "guardian_name",
        "parent_email",
        "personal_email",
    }
)
_REDACTED = "[REDACTED]"


def _redact_pii(
    logger: Any,
    method_name: str,
    event_dict: dict[str, Any],
) -> dict[str, Any]:
    """Structlog processor that replaces PII values with ``[REDACTED]``."""
    for key, value in list(event_dict.items()):
        if not isinstance(value, str):
            continue
        if key.lower() in _PII_KEYS:
            event_dict[key] = _REDACTED
            continue
        if _EMAIL_RE.search(value):
            event_dict[key] = _EMAIL_RE.sub(_REDACTED, value)
        if _PHONE_RE.search(value):
            event_dict[key] = _PHONE_RE.sub(_REDACTED, value)
    return event_dict


# --------------------------------------------------------------------------- #
# Injection processors                                                         #
# --------------------------------------------------------------------------- #
def _inject_context(
    logger: Any,
    method_name: str,
    event_dict: dict[str, Any],
) -> dict[str, Any]:
    """Add correlation_id, service_name, and environment to every event."""
    event_dict.setdefault("correlation_id", get_correlation_id())
    event_dict.setdefault("service", _service_name_ctx.get("unknown"))
    event_dict.setdefault("environment", os.getenv("CDP_ENV", "development"))
    return event_dict


_service_name_ctx: ContextVar[str] = ContextVar("service_name", default="unknown")

# --------------------------------------------------------------------------- #
# Public setup function                                                        #
# --------------------------------------------------------------------------- #


def setup_logging(service_name: str, log_level: str = "INFO") -> structlog.stdlib.BoundLogger:
    """Configure structlog for JSON output with PII redaction.

    Parameters
    ----------
    service_name:
        Logical name of the micro-service (e.g. ``"profile-api"``).
    log_level:
        Standard Python log level string.  Defaults to ``"INFO"``.

    Returns
    -------
    structlog.stdlib.BoundLogger
        A pre-configured logger instance ready for use.
    """
    _service_name_ctx.set(service_name)
    level = getattr(logging, log_level.upper(), logging.INFO)

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            _inject_context,  # type: ignore[arg-type]
            _redact_pii,  # type: ignore[arg-type]
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    logging.basicConfig(format="%(message)s", level=level, force=True)
    logger: structlog.stdlib.BoundLogger = structlog.get_logger(service_name)
    logger.info("logging_initialised", log_level=log_level)
    return logger
