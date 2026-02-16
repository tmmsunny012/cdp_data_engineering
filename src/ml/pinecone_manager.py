"""Pinecone vector database manager for CDP student embeddings."""

from __future__ import annotations

import logging
import time
from typing import Any

from pinecone import Pinecone, ServerlessSpec
from pinecone.exceptions import PineconeApiException

logger = logging.getLogger(__name__)

_BATCH_SIZE: int = 100  # Pinecone recommended upsert batch limit
_MAX_RETRIES: int = 3
_RETRY_BACKOFF_S: float = 1.5


class PineconeManager:
    """Manages a Pinecone index storing student profile embeddings."""

    def __init__(self, api_key: str, index_name: str, dimension: int = 768) -> None:
        self.dimension = dimension
        self.index_name = index_name

        self._pc = Pinecone(api_key=api_key)

        if index_name not in [idx.name for idx in self._pc.list_indexes()]:
            self._pc.create_index(
                name=index_name,
                dimension=dimension,
                metric="cosine",
                spec=ServerlessSpec(cloud="gcp", region="us-central1"),
            )
            logger.info("Created Pinecone index %s (dim=%d)", index_name, dimension)

        self._index = self._pc.Index(index_name)
        logger.info("Connected to Pinecone index %s", index_name)

    # ------------------------------------------------------------------
    # Upsert
    # ------------------------------------------------------------------

    def upsert_embeddings(self, vectors: list[dict[str, Any]]) -> int:
        """Batch upsert vectors with metadata.

        Each dict in *vectors* must contain:
        - ``id``: unique vector ID
        - ``values``: list[float] of length *self.dimension*
        - ``metadata``: dict with student_id, segment (list), enrollment_status, last_updated
        Returns the total number of upserted vectors.
        """
        upserted = 0
        for start in range(0, len(vectors), _BATCH_SIZE):
            batch = vectors[start : start + _BATCH_SIZE]
            self._upsert_with_retry(batch)
            upserted += len(batch)
            logger.debug("Upserted batch %d-%d", start, start + len(batch))
        logger.info("Upserted %d vectors into %s", upserted, self.index_name)
        return upserted

    def _upsert_with_retry(self, batch: list[dict[str, Any]]) -> None:
        """Upsert a single batch with exponential back-off on timeout."""
        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                self._index.upsert(vectors=batch)
                return
            except PineconeApiException as exc:
                if attempt == _MAX_RETRIES:
                    logger.error("Upsert failed after %d retries: %s", _MAX_RETRIES, exc)
                    raise
                wait = _RETRY_BACKOFF_S * (2 ** (attempt - 1))
                logger.warning("Pinecone timeout (attempt %d/%d), retrying in %.1fs", attempt, _MAX_RETRIES, wait)
                time.sleep(wait)

    # ------------------------------------------------------------------
    # Similarity search
    # ------------------------------------------------------------------

    def search_similar(
        self,
        query_vector: list[float],
        top_k: int = 5,
        filter: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Query the index for nearest neighbours with optional metadata filtering."""
        results = self._index.query(
            vector=query_vector,
            top_k=top_k,
            filter=filter,
            include_metadata=True,
        )
        return [
            {"id": m.id, "score": m.score, "metadata": m.metadata}
            for m in results.matches
        ]

    # ------------------------------------------------------------------
    # GDPR deletion
    # ------------------------------------------------------------------

    def delete_by_student(self, student_id: str) -> None:
        """Delete all vectors associated with a student (GDPR compliance)."""
        self._index.delete(filter={"student_id": {"$eq": student_id}})
        logger.info("GDPR: deleted vectors for student %s from %s", student_id, self.index_name)

    # ------------------------------------------------------------------
    # Index stats
    # ------------------------------------------------------------------

    def get_index_stats(self) -> dict[str, Any]:
        """Return vector count, dimension, and index fullness."""
        stats = self._index.describe_index_stats()
        return {
            "total_vector_count": stats.total_vector_count,
            "dimension": stats.dimension,
            "index_fullness": stats.index_fullness,
            "namespaces": {ns: ns_stats.vector_count for ns, ns_stats in stats.namespaces.items()},
        }
