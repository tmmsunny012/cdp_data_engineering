"""Vertex AI embedding generation for CDP student and course content."""

from __future__ import annotations

import logging
import time
from functools import lru_cache
from typing import Any

import vertexai
from vertexai.language_models import TextEmbeddingInput, TextEmbeddingModel

logger = logging.getLogger(__name__)

_BATCH_LIMIT: int = 250  # Vertex AI max texts per embedding request
_RATE_LIMIT_RPM: int = 600  # Vertex AI quota: 600 requests / minute
_MIN_INTERVAL_S: float = 60.0 / _RATE_LIMIT_RPM  # ~0.1s between calls


class EmbeddingGenerator:
    """Generates text embeddings via Vertex AI for the CDP platform."""

    def __init__(
        self,
        project_id: str,
        location: str,
        model_name: str = "textembedding-gecko@003",
    ) -> None:
        self.project_id = project_id
        self.location = location
        self.model_name = model_name

        vertexai.init(project=project_id, location=location)
        self._model = TextEmbeddingModel.from_pretrained(model_name)
        self._last_request_ts: float = 0.0
        logger.info("EmbeddingGenerator initialised with model %s", model_name)

    # ------------------------------------------------------------------
    # Rate-limiting helper
    # ------------------------------------------------------------------

    def _throttle(self) -> None:
        """Ensure we respect the Vertex AI requests-per-minute quota."""
        elapsed = time.monotonic() - self._last_request_ts
        if elapsed < _MIN_INTERVAL_S:
            time.sleep(_MIN_INTERVAL_S - elapsed)
        self._last_request_ts = time.monotonic()

    # ------------------------------------------------------------------
    # Core embedding methods
    # ------------------------------------------------------------------

    def generate_text_embedding(self, text: str) -> list[float]:
        """Embed a single text string and return a 768-dim vector."""
        self._throttle()
        inputs = [TextEmbeddingInput(text=text, task_type="RETRIEVAL_DOCUMENT")]
        embeddings = self._model.get_embeddings(inputs)
        return embeddings[0].values

    def generate_batch_embeddings(self, texts: list[str]) -> list[list[float]]:
        """Embed up to 250 texts in a single request.

        Texts exceeding *_BATCH_LIMIT* are split into sequential sub-batches.
        """
        all_vectors: list[list[float]] = []
        for start in range(0, len(texts), _BATCH_LIMIT):
            batch = texts[start : start + _BATCH_LIMIT]
            self._throttle()
            inputs = [TextEmbeddingInput(text=t, task_type="RETRIEVAL_DOCUMENT") for t in batch]
            embeddings = self._model.get_embeddings(inputs)
            all_vectors.extend(e.values for e in embeddings)
            logger.debug("Embedded batch %d-%d (%d texts)", start, start + len(batch), len(batch))
        return all_vectors

    # ------------------------------------------------------------------
    # Domain-specific embedding helpers
    # ------------------------------------------------------------------

    def generate_student_profile_embedding(self, profile: dict[str, Any]) -> list[float]:
        """Convert a student profile dict to a text summary, then embed it.

        Expected keys: courses, engagement_score, funnel_stage, last_actions.
        """
        summary = (
            f"Student interested in {', '.join(profile.get('courses', []))}. "
            f"Engagement: {profile.get('engagement_score', 'unknown')}. "
            f"Stage: {profile.get('funnel_stage', 'unknown')}. "
            f"Recent activity: {', '.join(profile.get('last_actions', []))}"
        )
        return self.generate_text_embedding(summary)

    def generate_course_embedding(self, course_description: str) -> list[float]:
        """Embed a course description for similarity matching (LRU-cached)."""
        return self._cached_course_embedding(course_description)

    @lru_cache(maxsize=512)
    def _cached_course_embedding(self, course_description: str) -> list[float]:
        """Internal LRU-cached wrapper for course description embeddings."""
        logger.debug("Cache miss for course embedding, calling Vertex AI")
        self._throttle()
        inputs = [TextEmbeddingInput(text=course_description, task_type="RETRIEVAL_DOCUMENT")]
        embeddings = self._model.get_embeddings(inputs)
        return embeddings[0].values

    def generate_whatsapp_embedding(self, message: str) -> list[float]:
        """Embed a WhatsApp message for similar-inquiry lookup."""
        self._throttle()
        inputs = [TextEmbeddingInput(text=message, task_type="RETRIEVAL_QUERY")]
        embeddings = self._model.get_embeddings(inputs)
        return embeddings[0].values
