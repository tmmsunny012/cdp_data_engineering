"""FastAPI endpoint for email-marketing platform webhooks.

Handles event callbacks from providers such as Braze, SendGrid, or Mailgun.
Supports open, click, bounce, and unsubscribe events and publishes raw
payloads to the ``cdp.raw.email`` Kafka topic.

Apple Mail Privacy Protection (MPP) is detected by user-agent heuristics;
machine-opened events are flagged so downstream analytics can filter them.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import os
from datetime import UTC, datetime
from typing import Any, Literal

from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel, Field

from src.ingestion.kafka_producer import CDPKafkaProducer

logger = logging.getLogger(__name__)

router = APIRouter(tags=["webhooks"])

WEBHOOK_SECRET: str = os.getenv("EMAIL_WEBHOOK_SECRET", "")
KAFKA_TOPIC = "cdp.raw.email"

VALID_EVENT_TYPES = frozenset(
    {
        "email_opened",
        "email_clicked",
        "email_bounced",
        "email_unsubscribed",
    }
)

# Apple MPP sends opens from known Apple proxy IPs and a specific user-agent.
_APPLE_MPP_UA_FRAGMENT = "Mozilla/5.0"  # simplified; real detection uses IP ranges
_APPLE_MPP_INDICATORS = ("apple", "cfnetwork")

_producer: CDPKafkaProducer | None = None


async def _get_producer() -> CDPKafkaProducer:
    global _producer  # noqa: PLW0603
    if _producer is None:
        _producer = CDPKafkaProducer(client_id="email-webhook")
        await _producer.start()
    return _producer


# ---------------------------------------------------------------------------
# Payload models
# ---------------------------------------------------------------------------


class EmailRawEvent(BaseModel):
    """Canonical shape for a raw email-marketing event."""

    event_type: Literal[
        "email_opened",
        "email_clicked",
        "email_bounced",
        "email_unsubscribed",
    ]
    recipient_email: str
    campaign_id: str | None = None
    link_url: str | None = None
    bounce_type: str | None = None  # hard / soft
    user_agent: str | None = None
    ip_address: str | None = None
    is_machine_open: bool = False
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    raw_payload: dict[str, Any] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _verify_signature(payload_bytes: bytes, signature: str) -> bool:
    """HMAC-SHA256 verification using a shared secret."""
    if not WEBHOOK_SECRET:
        logger.warning("EMAIL_WEBHOOK_SECRET not set -- skipping verification")
        return True
    expected = hmac.new(WEBHOOK_SECRET.encode(), payload_bytes, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)


def _detect_machine_open(user_agent: str | None) -> bool:
    """Heuristic: flag likely Apple Mail Privacy Protection opens."""
    if not user_agent:
        return False
    ua_lower = user_agent.lower()
    return any(ind in ua_lower for ind in _APPLE_MPP_INDICATORS)


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------


@router.post("/webhooks/email", status_code=200)
async def email_webhook(
    request: Request,
    x_webhook_signature: str = Header("", alias="X-Webhook-Signature"),
) -> dict[str, str]:
    """Receive email-platform event callbacks.

    Expects a JSON body with at least ``event_type`` and ``recipient_email``.
    The full provider payload is preserved in ``raw_payload`` for auditing.
    """
    body_bytes = await request.body()
    if not _verify_signature(body_bytes, x_webhook_signature):
        raise HTTPException(status_code=403, detail="Invalid webhook signature")

    payload: dict[str, Any] = await request.json()

    event_type = payload.get("event_type", payload.get("event", ""))
    if event_type not in VALID_EVENT_TYPES:
        raise HTTPException(
            status_code=422,
            detail=f"Unsupported event_type: {event_type}",
        )

    user_agent = payload.get("user_agent") or payload.get("useragent")
    is_machine = event_type == "email_opened" and _detect_machine_open(user_agent)

    event = EmailRawEvent(
        event_type=event_type,
        recipient_email=payload.get("recipient_email", payload.get("email", "")),
        campaign_id=payload.get("campaign_id"),
        link_url=payload.get("url"),
        bounce_type=payload.get("bounce_type"),
        user_agent=user_agent,
        ip_address=payload.get("ip"),
        is_machine_open=is_machine,
        timestamp=datetime.now(UTC),
        raw_payload=payload,
    )

    producer = await _get_producer()
    await producer.send(KAFKA_TOPIC, value=event, key=event.recipient_email)
    logger.info(
        "Published %s for %s (machine_open=%s)",
        event_type,
        event.recipient_email,
        is_machine,
    )

    return {"status": "accepted"}
