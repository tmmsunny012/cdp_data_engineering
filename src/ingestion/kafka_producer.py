"""Generic async Kafka producer for the CDP ingestion layer.

Wraps aiokafka with retry logic, Prometheus metrics, and Pydantic-native
JSON serialization so every pipeline can publish events with a single call.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os

from aiokafka import AIOKafkaProducer
from prometheus_client import Counter
from pydantic import BaseModel

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Prometheus metrics
# ---------------------------------------------------------------------------

EVENTS_PRODUCED = Counter(
    "cdp_events_produced_total",
    "Total events successfully published to Kafka",
    ["topic"],
)
PRODUCE_ERRORS = Counter(
    "cdp_produce_errors_total",
    "Total failed publish attempts",
    ["topic"],
)

# ---------------------------------------------------------------------------
# Configuration (environment-driven)
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_SECURITY = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
KAFKA_SASL_MECHANISM = os.getenv("KAFKA_SASL_MECHANISM", "PLAIN")
KAFKA_SASL_USER = os.getenv("KAFKA_SASL_USERNAME", "")
KAFKA_SASL_PASS = os.getenv("KAFKA_SASL_PASSWORD", "")
MAX_RETRIES = int(os.getenv("KAFKA_PRODUCER_MAX_RETRIES", "5"))
BASE_BACKOFF_S = float(os.getenv("KAFKA_PRODUCER_BACKOFF_S", "0.5"))


class CDPKafkaProducer:
    """Async Kafka producer with exponential-backoff retry.

    Usage::

        producer = CDPKafkaProducer()
        await producer.start()
        await producer.send("cdp.raw.clickstream", key="sess-123", value=event)
        await producer.stop()
    """

    def __init__(self, client_id: str = "cdp-producer") -> None:
        kwargs: dict[str, object] = {
            "bootstrap_servers": KAFKA_BOOTSTRAP,
            "client_id": client_id,
            "security_protocol": KAFKA_SECURITY,
            "value_serializer": self._serialize,
            "key_serializer": self._key_serialize,
        }
        if KAFKA_SECURITY != "PLAINTEXT":
            kwargs["sasl_mechanism"] = KAFKA_SASL_MECHANISM
            kwargs["sasl_plain_username"] = KAFKA_SASL_USER
            kwargs["sasl_plain_password"] = KAFKA_SASL_PASS
        self._producer = AIOKafkaProducer(**kwargs)

    # -- serialisation helpers ------------------------------------------------

    @staticmethod
    def _serialize(value: object) -> bytes:
        if isinstance(value, BaseModel):
            return value.model_dump_json().encode("utf-8")
        return json.dumps(value, default=str).encode("utf-8")

    @staticmethod
    def _key_serialize(key: str | None) -> bytes | None:
        return key.encode("utf-8") if key else None

    # -- lifecycle ------------------------------------------------------------

    async def start(self) -> None:
        """Start the underlying aiokafka producer."""
        await self._producer.start()
        logger.info("CDPKafkaProducer started (bootstrap=%s)", KAFKA_BOOTSTRAP)

    async def stop(self) -> None:
        """Flush pending messages and close the producer."""
        await self._producer.stop()
        logger.info("CDPKafkaProducer stopped")

    # -- publish with retry ---------------------------------------------------

    async def send(
        self,
        topic: str,
        value: BaseModel | dict[str, object],
        key: str | None = None,
    ) -> None:
        """Publish a message with exponential-backoff retry.

        Args:
            topic: Kafka topic name, e.g. ``cdp.raw.whatsapp``.
            value: Payload -- a Pydantic model or plain dict.
            key: Optional partition key for ordering guarantees.

        Raises:
            RuntimeError: After *MAX_RETRIES* consecutive failures.
        """
        last_exc: Exception | None = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                await self._producer.send_and_wait(topic, value=value, key=key)
                EVENTS_PRODUCED.labels(topic=topic).inc()
                return
            except Exception as exc:
                last_exc = exc
                PRODUCE_ERRORS.labels(topic=topic).inc()
                backoff = BASE_BACKOFF_S * (2 ** (attempt - 1))
                logger.warning(
                    "Kafka send failed (attempt %d/%d, topic=%s): %s — retrying in %.1fs",
                    attempt,
                    MAX_RETRIES,
                    topic,
                    exc,
                    backoff,
                )
                await asyncio.sleep(backoff)

        raise RuntimeError(
            f"Failed to publish to {topic} after {MAX_RETRIES} attempts"
        ) from last_exc
