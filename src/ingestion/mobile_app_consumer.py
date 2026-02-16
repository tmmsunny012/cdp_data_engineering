"""Async Kafka consumer for mobile-app events.

Reads raw events from ``cdp.raw.mobile_app``, handles mobile-specific
event types (lesson completions, quiz attempts, push interactions, etc.),
extracts device identifiers for cross-device identity resolution, and
publishes normalised :class:`CustomerEvent` objects to
``cdp.processed.interactions``.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

from aiokafka import AIOKafkaConsumer
from pydantic import BaseModel, Field, ValidationError

from src.ingestion.format_normalizer import FormatNormalizer
from src.ingestion.kafka_producer import CDPKafkaProducer
from src.storage.models.customer_profile import EventSource, Identifier, IdentifierType

logger = logging.getLogger(__name__)

SOURCE_TOPIC = "cdp.raw.mobile_app"
DEST_TOPIC = "cdp.processed.interactions"
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CONSUMER_GROUP = os.getenv("MOBILE_CONSUMER_GROUP", "cdp-mobile-app-cg")

MOBILE_EVENT_TYPES = frozenset(
    {
        "app_opened",
        "lesson_completed",
        "quiz_taken",
        "push_clicked",
        "course_downloaded",
        "study_session_started",
        "study_session_ended",
        "notification_received",
    }
)


# ---------------------------------------------------------------------------
# Raw event schema
# ---------------------------------------------------------------------------


class RawMobileAppEvent(BaseModel):
    """Expected shape of a raw mobile-app event."""

    event_type: str
    device_id: str
    advertising_id: str | None = None
    firebase_token: str | None = None
    user_id: str | None = None
    app_version: str | None = None
    os_name: str | None = None
    os_version: str | None = None
    timestamp: str | None = None
    properties: dict[str, Any] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Consumer
# ---------------------------------------------------------------------------


class MobileAppConsumer:
    """Consume, validate, and normalise mobile-app events.

    Device identifiers (``device_id``, ``advertising_id``, ``firebase_token``)
    are extracted and emitted as :class:`Identifier` objects so the
    identity-resolution layer can link them to a unified profile.
    """

    def __init__(
        self,
        producer: CDPKafkaProducer,
        normalizer: FormatNormalizer | None = None,
    ) -> None:
        self._producer = producer
        self._normalizer = normalizer or FormatNormalizer()
        self._consumer: AIOKafkaConsumer | None = None

    async def start(self) -> None:
        self._consumer = AIOKafkaConsumer(
            SOURCE_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id=CONSUMER_GROUP,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
        )
        await self._consumer.start()
        logger.info("MobileAppConsumer started on topic=%s", SOURCE_TOPIC)

    async def stop(self) -> None:
        if self._consumer:
            await self._consumer.stop()
            logger.info("MobileAppConsumer stopped")

    # -- identifier extraction ------------------------------------------------

    @staticmethod
    def _extract_identifiers(event: RawMobileAppEvent) -> list[Identifier]:
        """Build a list of device-level identifiers for identity resolution."""
        ids: list[Identifier] = [
            Identifier(type=IdentifierType.DEVICE_ID, value=event.device_id),
        ]
        if event.advertising_id:
            ids.append(Identifier(type=IdentifierType.DEVICE_ID, value=event.advertising_id))
        return ids

    # -- main loop ------------------------------------------------------------

    async def run(self) -> None:
        """Consume loop -- runs until the task is cancelled."""
        if self._consumer is None:
            await self.start()
        assert self._consumer is not None

        async for msg in self._consumer:
            try:
                raw: dict[str, Any] = msg.value
                validated = RawMobileAppEvent(**raw)

                if validated.event_type not in MOBILE_EVENT_TYPES:
                    logger.debug("Skipping unknown mobile event_type=%s", validated.event_type)
                    continue

                customer_event = self._normalizer.normalize_json(
                    raw_data=raw, source=EventSource.APP
                )
                customer_event.event_type = f"mobile.{validated.event_type}"
                customer_event.student_id = validated.user_id

                # Attach device identifiers and mobile-specific metadata.
                identifiers = self._extract_identifiers(validated)
                customer_event.normalized_data.update(
                    {
                        "device_id": validated.device_id,
                        "advertising_id": validated.advertising_id,
                        "firebase_token": validated.firebase_token,
                        "app_version": validated.app_version,
                        "os": f"{validated.os_name or ''} {validated.os_version or ''}".strip(),
                        "identifiers": [i.model_dump() for i in identifiers],
                        **validated.properties,
                    }
                )

                await self._producer.send(
                    DEST_TOPIC,
                    value=customer_event,
                    key=validated.device_id,
                )
            except ValidationError as exc:
                logger.warning(
                    "Invalid mobile event (offset=%d): %s",
                    msg.offset,
                    exc.error_count(),
                )
            except Exception:
                logger.exception("Error processing mobile event (offset=%d)", msg.offset)
