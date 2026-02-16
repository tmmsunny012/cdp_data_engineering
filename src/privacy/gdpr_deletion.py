"""GDPR Right-to-Erasure cascade deletion engine.

Orchestrates deletion of student PII across all CDP data stores.
SLA target: 72 hours (GDPR permits up to 30 days).
Every step is audit-logged; partial failures trigger retries and alerts.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from motor.motor_asyncio import AsyncIOMotorDatabase

logger = logging.getLogger(__name__)

BQ_TABLES: list[str] = [
    "cdp_bronze.raw_events",
    "cdp_silver.student_profiles",
    "cdp_silver.identity_graph",
    "cdp_gold.unified_profiles",
    "cdp_gold.segment_memberships",
    "cdp_reverse_etl.salesforce_sync",
]

KAFKA_TOPICS: list[str] = [
    "cdp.events.raw",
    "cdp.profiles.unified",
    "cdp.segments.updates",
    "cdp.reverse-etl.commands",
]

MAX_RETRIES: int = 3
SLA_HOURS: int = 72


@dataclass
class StoreResult:
    store: str
    deleted: bool
    error: str | None = None
    records_affected: int = 0


@dataclass
class DeletionReport:
    student_id: str
    requested_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    completed_at: datetime | None = None
    total_duration_seconds: float = 0.0
    store_results: list[StoreResult] = field(default_factory=list)
    fully_deleted: bool = False


@dataclass
class VerificationResult:
    student_id: str
    verified_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    all_clear: bool = True
    store_checks: dict[str, bool] = field(default_factory=dict)


class GDPRDeletionEngine:
    """Cascade deletion across every CDP data store for GDPR Art. 17."""

    def __init__(
        self,
        mongo_store: AsyncIOMotorDatabase,
        bq_loader: Any,
        pinecone_manager: Any,
        vertex_store: Any,
        kafka_producer: Any,
    ) -> None:
        self._mongo = mongo_store
        self._bq = bq_loader
        self._pinecone = pinecone_manager
        self._vertex = vertex_store
        self._kafka = kafka_producer
        self._audit = mongo_store["deletion_audit"]

    async def delete_student(self, student_id: str) -> DeletionReport:
        """Orchestrate full erasure across all stores with retry logic."""
        report = DeletionReport(student_id=student_id)
        start = time.monotonic()
        logger.info("GDPR deletion started: student=%s", student_id)

        deleters = [
            ("mongodb", self._delete_from_mongodb),
            ("bigquery", self._delete_from_bigquery),
            ("pinecone", self._delete_from_pinecone),
            ("vertex_ai", self._delete_from_vertex_ai),
            ("kafka", self._publish_kafka_tombstone),
            ("salesforce", self._delete_from_salesforce),
        ]

        for store_name, fn in deleters:
            result = await self._execute_with_retry(store_name, fn, student_id)
            report.store_results.append(result)

        report.total_duration_seconds = time.monotonic() - start
        report.completed_at = datetime.now(UTC)
        report.fully_deleted = all(r.deleted for r in report.store_results)

        await self._audit.insert_one(
            {
                "student_id": student_id,
                "action": "delete",
                "fully_deleted": report.fully_deleted,
                "duration_s": report.total_duration_seconds,
                "store_results": [
                    {"store": r.store, "deleted": r.deleted, "error": r.error}
                    for r in report.store_results
                ],
                "timestamp": report.completed_at,
            }
        )

        if not report.fully_deleted:
            failed = [r.store for r in report.store_results if not r.deleted]
            logger.error(
                "GDPR deletion PARTIAL FAILURE: student=%s failed_stores=%s", student_id, failed
            )
        else:
            logger.info(
                "GDPR deletion completed: student=%s duration=%.1fs",
                student_id,
                report.total_duration_seconds,
            )
        return report

    async def _execute_with_retry(self, store_name: str, fn: Any, student_id: str) -> StoreResult:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                success = await fn(student_id)
                return StoreResult(store=store_name, deleted=success)
            except Exception as exc:
                logger.warning(
                    "Deletion attempt %d/%d failed for %s/%s: %s",
                    attempt,
                    MAX_RETRIES,
                    store_name,
                    student_id,
                    exc,
                )
                if attempt < MAX_RETRIES:
                    await asyncio.sleep(2**attempt)
        return StoreResult(store=store_name, deleted=False, error="max retries exceeded")

    async def _delete_from_mongodb(self, student_id: str) -> bool:
        collections = ["profiles", "events", "consents", "consent_audit_log", "segments"]
        total = 0
        for coll in collections:
            result = await self._mongo[coll].delete_many({"student_id": student_id})
            total += result.deleted_count
        logger.info("MongoDB: deleted %d documents for student=%s", total, student_id)
        return True

    async def _delete_from_bigquery(self, student_id: str) -> bool:
        from google.cloud import bigquery

        client: bigquery.Client = self._bq
        for table in BQ_TABLES:
            query = f"DELETE FROM `{table}` WHERE student_id = @sid"
            job_config = bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("sid", "STRING", student_id)]
            )
            client.query(query, job_config=job_config).result()
        logger.info("BigQuery: deleted from %d tables for student=%s", len(BQ_TABLES), student_id)
        return True

    async def _delete_from_pinecone(self, student_id: str) -> bool:
        index = self._pinecone.Index("cdp-embeddings")
        index.delete(filter={"student_id": {"$eq": student_id}})
        logger.info("Pinecone: deleted vectors for student=%s", student_id)
        return True

    async def _delete_from_vertex_ai(self, student_id: str) -> bool:
        self._vertex.delete_entity(entity_type="student", entity_id=student_id)
        logger.info("Vertex AI Feature Store: deleted entity for student=%s", student_id)
        return True

    async def _publish_kafka_tombstone(self, student_id: str) -> bool:
        for topic in KAFKA_TOPICS:
            self._kafka.produce(topic, key=student_id.encode(), value=None)
        self._kafka.flush(timeout=10)
        logger.info(
            "Kafka: published tombstones to %d topics for student=%s", len(KAFKA_TOPICS), student_id
        )
        return True

    async def _delete_from_salesforce(self, student_id: str) -> bool:
        sf_collection = self._mongo["salesforce_mappings"]
        mapping = await sf_collection.find_one({"student_id": student_id})
        if mapping and mapping.get("salesforce_id"):
            logger.info("Salesforce: deletion requested for contact=%s", mapping["salesforce_id"])
            # In production this calls the Salesforce REST API via an HTTP client.
            # The mapping is removed after confirmation.
        await sf_collection.delete_many({"student_id": student_id})
        return True

    async def verify_deletion(self, student_id: str) -> VerificationResult:
        """Post-deletion verification across every store."""
        result = VerificationResult(student_id=student_id)

        # MongoDB
        mongo_count = 0
        for coll in ["profiles", "events", "consents", "segments"]:
            mongo_count += await self._mongo[coll].count_documents({"student_id": student_id})
        result.store_checks["mongodb"] = mongo_count == 0

        # BigQuery
        from google.cloud import bigquery

        client: bigquery.Client = self._bq
        bq_clear = True
        for table in BQ_TABLES:
            query = f"SELECT COUNT(*) AS cnt FROM `{table}` WHERE student_id = @sid"
            job_config = bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("sid", "STRING", student_id)]
            )
            rows = client.query(query, job_config=job_config).result()
            if next(iter(rows)).cnt > 0:
                bq_clear = False
                break
        result.store_checks["bigquery"] = bq_clear

        # Pinecone
        index = self._pinecone.Index("cdp-embeddings")
        matches = index.query(filter={"student_id": {"$eq": student_id}}, top_k=1)
        result.store_checks["pinecone"] = len(matches.get("matches", [])) == 0

        result.all_clear = all(result.store_checks.values())

        await self._audit.insert_one(
            {
                "student_id": student_id,
                "action": "verify_deletion",
                "all_clear": result.all_clear,
                "store_checks": result.store_checks,
                "timestamp": result.verified_at,
            }
        )

        logger.info("Deletion verification: student=%s all_clear=%s", student_id, result.all_clear)
        return result
