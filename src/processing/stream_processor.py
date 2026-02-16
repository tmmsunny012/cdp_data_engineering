"""Async Kafka stream processor for CDP event pipeline.

Consumes from cdp.processed.interactions, applies quality checks,
resolves identities, builds profiles, and stages for BigQuery.
"""

from __future__ import annotations

import asyncio
import logging
import signal
import time
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from prometheus_client import Counter, Histogram
from pydantic import BaseModel, Field

from src.processing.identity_resolution import IdentityResolver
from src.processing.profile_builder import ProfileBuilder

logger = logging.getLogger(__name__)

# ── Prometheus metrics ──────────────────────────────────────────────
EVENTS_PROCESSED = Counter("cdp_events_processed_total", "Total events processed", ["source"])
PROCESSING_LATENCY = Histogram(
    "cdp_processing_latency_seconds", "End-to-end event processing latency"
)
DLQ_COUNT = Counter("cdp_dlq_total", "Events sent to dead-letter queue", ["reason"])

# ── Lightweight config ──────────────────────────────────────────────
VALID_SOURCES = frozenset({"website", "app", "crm", "email", "whatsapp"})

INPUT_TOPIC = "cdp.processed.interactions"
BQ_STAGING_TOPIC = "cdp.bigquery.staging"
DLQ_TOPIC = "cdp.dlq"


class ProcessorConfig(BaseModel):
    kafka_brokers: str = "localhost:9092"
    consumer_group: str = "cdp-stream-processor"
    batch_size: int = Field(default=50, ge=1, le=500)
    max_concurrency: int = Field(default=10, ge=1, le=100)


class StreamProcessor:
    """Consumes interaction events, enriches them, and forwards downstream."""

    def __init__(
        self,
        config: ProcessorConfig,
        identity_resolver: IdentityResolver,
        profile_builder: ProfileBuilder,
    ) -> None:
        self._cfg = config
        self._resolver = identity_resolver
        self._builder = profile_builder
        self._consumer: AIOKafkaConsumer | None = None
        self._producer: AIOKafkaProducer | None = None
        self._shutdown_event = asyncio.Event()
        self._semaphore = asyncio.Semaphore(config.max_concurrency)

    # ── lifecycle ────────────────────────────────────────────────────
    async def start(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self._request_shutdown)

        self._consumer = AIOKafkaConsumer(
            INPUT_TOPIC,
            bootstrap_servers=self._cfg.kafka_brokers,
            group_id=self._cfg.consumer_group,
            enable_auto_commit=False,
            max_poll_records=self._cfg.batch_size,
        )
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._cfg.kafka_brokers,
        )
        await self._consumer.start()
        await self._producer.start()
        logger.info("StreamProcessor started — consuming %s", INPUT_TOPIC)
        try:
            await self._consume_loop()
        finally:
            await self._consumer.stop()
            await self._producer.stop()
            logger.info("StreamProcessor shut down gracefully")

    def _request_shutdown(self) -> None:
        logger.info("Shutdown signal received")
        self._shutdown_event.set()

    # ── main loop ────────────────────────────────────────────────────
    async def _consume_loop(self) -> None:
        assert self._consumer is not None
        while not self._shutdown_event.is_set():
            batch = await self._consumer.getmany(timeout_ms=1000, max_records=self._cfg.batch_size)
            tasks: list[asyncio.Task[None]] = []
            for _tp, messages in batch.items():
                for msg in messages:
                    tasks.append(asyncio.create_task(self._process_one(msg)))
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
                await self._consumer.commit()

    async def _process_one(self, msg: Any) -> None:
        async with self._semaphore:
            start = time.monotonic()
            try:
                event: dict[str, Any] = msg.value
                source = event.get("source", "unknown")
                if source not in VALID_SOURCES:
                    raise ValueError(f"Unknown event source: {source}")

                profile_id = await self._resolver.resolve(event)
                profile = await self._builder.update_profile(profile_id, event)

                assert self._producer is not None
                await self._producer.send_and_wait(
                    BQ_STAGING_TOPIC,
                    value={"profile_id": profile_id, "event": event, "profile_snapshot": profile},
                )
                EVENTS_PROCESSED.labels(source=source).inc()
            except Exception as exc:
                logger.exception("Event processing failed — routing to DLQ")
                await self._send_to_dlq(msg, str(exc))
            finally:
                PROCESSING_LATENCY.observe(time.monotonic() - start)

    async def _send_to_dlq(self, original_msg: Any, reason: str) -> None:
        assert self._producer is not None
        await self._producer.send_and_wait(
            DLQ_TOPIC,
            value={"original": original_msg.value, "error": reason},
        )
        DLQ_COUNT.labels(reason=reason[:120]).inc()
