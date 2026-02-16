"""Async Kafka consumer for website clickstream events.

Reads raw JSON events from ``cdp.raw.clickstream``, validates them against
the expected schema, normalises them via :class:`FormatNormalizer`, and
re-publishes the canonical :class:`CustomerEvent` to the
``cdp.processed.interactions`` topic.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Optional

from aiokafka import AIOKafkaConsumer
from pydantic import BaseModel, Field, ValidationError

from src.ingestion.format_normalizer import FormatNormalizer
from src.ingestion.kafka_producer import CDPKafkaProducer
from src.storage.models.customer_profile import EventSource

logger = logging.getLogger(__name__)

SOURCE_TOPIC = "cdp.raw.clickstream"
DEST_TOPIC = "cdp.processed.interactions"
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CONSUMER_GROUP = os.getenv("CLICKSTREAM_CONSUMER_GROUP", "cdp-clickstream-cg")


# ---------------------------------------------------------------------------
# Expected raw schema (for validation only)
# ---------------------------------------------------------------------------

class RawClickstreamEvent(BaseModel):
    """Minimal schema every raw clickstream JSON must satisfy."""

    session_id: str
    page_url: str
    event_type: str = "page_view"
    user_agent: Optional[str] = None
    utm_params: dict[str, str] = Field(default_factory=dict)
    referrer: Optional[str] = None
    timestamp: Optional[str] = None
    user_id: Optional[str] = None


# ---------------------------------------------------------------------------
# Consumer
# ---------------------------------------------------------------------------

class ClickstreamConsumer:
    """Consume, validate, normalise, and re-publish clickstream events.

    Args:
        producer: Shared :class:`CDPKafkaProducer` for the output topic.
        normalizer: Shared :class:`FormatNormalizer` instance.
    """

    def __init__(
        self,
        producer: CDPKafkaProducer,
        normalizer: Optional[FormatNormalizer] = None,
    ) -> None:
        self._producer = producer
        self._normalizer = normalizer or FormatNormalizer()
        self._consumer: Optional[AIOKafkaConsumer] = None

    async def start(self) -> None:
        """Create and start the underlying Kafka consumer."""
        self._consumer = AIOKafkaConsumer(
            SOURCE_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id=CONSUMER_GROUP,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
        )
        await self._consumer.start()
        logger.info("ClickstreamConsumer started on topic=%s", SOURCE_TOPIC)

    async def stop(self) -> None:
        """Gracefully stop the consumer."""
        if self._consumer:
            await self._consumer.stop()
            logger.info("ClickstreamConsumer stopped")

    async def run(self) -> None:
        """Main consume loop -- runs until cancelled.

        Invalid messages are logged and skipped (dead-letter handling is
        deferred to a later iteration).
        """
        if self._consumer is None:
            await self.start()
        assert self._consumer is not None

        async for msg in self._consumer:
            try:
                raw: dict[str, Any] = msg.value
                validated = RawClickstreamEvent(**raw)

                customer_event = self._normalizer.normalize_json(
                    raw_data=raw, source=EventSource.WEBSITE
                )

                # Enrich normalized_data with parsed clickstream fields.
                customer_event.normalized_data.update({
                    "session_id": validated.session_id,
                    "page_url": validated.page_url,
                    "event_type": validated.event_type,
                    "utm_params": validated.utm_params,
                    "referrer": validated.referrer,
                })
                customer_event.student_id = validated.user_id

                await self._producer.send(
                    DEST_TOPIC,
                    value=customer_event,
                    key=validated.session_id,
                )
            except ValidationError as exc:
                logger.warning(
                    "Invalid clickstream event (offset=%d): %s",
                    msg.offset,
                    exc.error_count(),
                )
            except Exception:
                logger.exception(
                    "Unexpected error processing clickstream event (offset=%d)",
                    msg.offset,
                )
