"""FastAPI endpoint for Twilio WhatsApp inbound webhooks.

Receives text messages, media messages, and delivery-status callbacks from
Twilio, validates the request signature, and publishes raw events to the
``cdp.raw.whatsapp`` Kafka topic for downstream normalisation.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import os
from datetime import UTC, datetime
from urllib.parse import urlencode

from fastapi import APIRouter, Form, Header, HTTPException, Request
from pydantic import BaseModel, Field

from src.ingestion.kafka_producer import CDPKafkaProducer

logger = logging.getLogger(__name__)

router = APIRouter(tags=["webhooks"])

TWILIO_AUTH_TOKEN: str = os.getenv("TWILIO_AUTH_TOKEN", "")
KAFKA_TOPIC = "cdp.raw.whatsapp"

_producer: CDPKafkaProducer | None = None


async def get_producer() -> CDPKafkaProducer:
    """Lazy-initialise a shared Kafka producer."""
    global _producer  # noqa: PLW0603
    if _producer is None:
        _producer = CDPKafkaProducer(client_id="twilio-webhook")
        await _producer.start()
    return _producer


# ---------------------------------------------------------------------------
# Payload models
# ---------------------------------------------------------------------------


class WhatsAppRawEvent(BaseModel):
    """Schema for the raw Kafka message."""

    from_number: str
    body: str | None = None
    media_urls: list[str] = Field(default_factory=list)
    num_media: int = 0
    message_sid: str | None = None
    message_status: str | None = None
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    event_kind: str = "message"  # message | status


# ---------------------------------------------------------------------------
# Signature validation
# ---------------------------------------------------------------------------


def _validate_twilio_signature(
    url: str,
    params: dict[str, str],
    signature: str,
) -> bool:
    """Reproduce Twilio's HMAC-SHA1 request signing algorithm."""
    if not TWILIO_AUTH_TOKEN:
        logger.warning("TWILIO_AUTH_TOKEN not set -- skipping validation")
        return True
    sorted_params = urlencode(sorted(params.items()))
    data = f"{url}{sorted_params}"
    expected = hmac.new(
        TWILIO_AUTH_TOKEN.encode("utf-8"),
        data.encode("utf-8"),
        hashlib.sha1,
    ).hexdigest()
    return hmac.compare_digest(expected, signature)


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------


@router.post("/webhooks/twilio/whatsapp", status_code=200)
async def twilio_whatsapp_webhook(
    request: Request,
    From: str = Form(""),  # noqa: N803
    Body: str = Form(""),  # noqa: N803
    NumMedia: int = Form(0),  # noqa: N803
    MessageSid: str = Form(""),  # noqa: N803
    MessageStatus: str | None = Form(None),  # noqa: N803
    x_twilio_signature: str = Header("", alias="X-Twilio-Signature"),
) -> dict[str, str]:
    """Receive a Twilio WhatsApp callback.

    Handles three kinds of payload:
    * **Inbound text message** -- ``Body`` is populated.
    * **Inbound media message** -- ``NumMedia > 0``, media URLs present.
    * **Delivery status update** -- ``MessageStatus`` is populated.
    """
    form_data = await request.form()
    params = {k: str(v) for k, v in form_data.items()}

    if not _validate_twilio_signature(str(request.url), params, x_twilio_signature):
        raise HTTPException(status_code=403, detail="Invalid Twilio signature")

    # Collect media URLs (Twilio sends MediaUrl0, MediaUrl1, ...)
    media_urls: list[str] = [
        str(form_data[f"MediaUrl{i}"]) for i in range(NumMedia) if f"MediaUrl{i}" in form_data
    ]

    event_kind = "status" if MessageStatus else "message"
    event = WhatsAppRawEvent(
        from_number=From,
        body=Body or None,
        media_urls=media_urls,
        num_media=NumMedia,
        message_sid=MessageSid,
        message_status=MessageStatus,
        event_kind=event_kind,
    )

    producer = await get_producer()
    await producer.send(KAFKA_TOPIC, value=event, key=From or None)
    logger.info("Published %s event for %s", event_kind, From)

    # Twilio expects a quick 200; any body is ignored.
    return {"status": "ok"}
