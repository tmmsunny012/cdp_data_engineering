"""Vertex AI Feature Store manager for student profile features."""

from __future__ import annotations

import logging
from typing import Any

from google.cloud import aiplatform
from google.cloud.aiplatform import Featurestore

logger = logging.getLogger(__name__)

# Canonical feature definitions for the student_profile entity type.
STUDENT_PROFILE_FEATURES: list[dict[str, str]] = [
    {"id": "days_since_last_interaction", "value_type": "INT64"},
    {"id": "total_whatsapp_messages_30d", "value_type": "INT64"},
    {"id": "course_page_views_by_category", "value_type": "STRING"},
    {"id": "enrollment_funnel_stage", "value_type": "STRING"},
    {"id": "sentiment_score_rolling_avg", "value_type": "DOUBLE"},
    {"id": "assignment_completion_rate", "value_type": "DOUBLE"},
]


class VertexFeatureStore:
    """Manages a Vertex AI Feature Store for CDP feature serving."""

    def __init__(self, project_id: str, location: str, feature_store_id: str) -> None:
        self.project_id = project_id
        self.location = location
        self.feature_store_id = feature_store_id

        aiplatform.init(project=project_id, location=location)
        self._fs = Featurestore(featurestore_name=feature_store_id)
        logger.info("Connected to Feature Store %s in %s/%s", feature_store_id, project_id, location)

    # ------------------------------------------------------------------
    # Entity type management
    # ------------------------------------------------------------------

    def create_entity_type(self, name: str, features: list[dict[str, str]]) -> Any:
        """Create an entity type with the given feature definitions.

        Each item in *features* must contain ``id`` and ``value_type`` keys
        matching the Vertex AI Feature Store API schema.
        """
        entity_type = self._fs.create_entity_type(
            entity_type_id=name,
            description=f"CDP entity type: {name}",
        )
        for feat in features:
            entity_type.create_feature(
                feature_id=feat["id"],
                value_type=feat["value_type"],
                description=f"Feature {feat['id']} for {name}",
            )
            logger.info("Created feature %s on entity type %s", feat["id"], name)
        return entity_type

    # ------------------------------------------------------------------
    # Batch ingestion (BigQuery Gold -> Feature Store)
    # ------------------------------------------------------------------

    def batch_ingest_from_bigquery(
        self, entity_type: str, bq_table: str, entity_id_field: str
    ) -> None:
        """Batch-import features from a BigQuery Gold table."""
        et = self._fs.get_entity_type(entity_type_id=entity_type)
        et.ingest_from_bq(
            feature_ids=[f["id"] for f in STUDENT_PROFILE_FEATURES],
            feature_time="event_timestamp",
            bq_source_uri=f"bq://{bq_table}",
            entity_id_field=entity_id_field,
        )
        logger.info("Batch ingestion from %s into %s completed", bq_table, entity_type)

    # ------------------------------------------------------------------
    # Real-time streaming write (Kafka -> Feature Store)
    # ------------------------------------------------------------------

    def stream_write(self, entity_type: str, entity_id: str, features: dict[str, Any]) -> None:
        """Write a single feature row in real time (e.g. from a Kafka consumer)."""
        et = self._fs.get_entity_type(entity_type_id=entity_type)
        et.write_feature_values(
            payloads=[{"entity_id": entity_id, **features}],
        )
        logger.debug("Streamed features for entity %s/%s", entity_type, entity_id)

    # ------------------------------------------------------------------
    # Online serving (low-latency reads for ML inference)
    # ------------------------------------------------------------------

    def online_read(
        self, entity_type: str, entity_ids: list[str], feature_ids: list[str]
    ) -> list[dict[str, Any]]:
        """Read features online for one or more entities."""
        et = self._fs.get_entity_type(entity_type_id=entity_type)
        response = et.read(entity_ids=entity_ids, feature_ids=feature_ids)
        rows: list[dict[str, Any]] = response.to_dict(orient="records")
        logger.debug("Online read returned %d rows for %s", len(rows), entity_type)
        return rows

    # ------------------------------------------------------------------
    # GDPR deletion
    # ------------------------------------------------------------------

    def delete_entity(self, entity_type: str, entity_id: str) -> None:
        """Delete all feature values for a single entity (GDPR compliance)."""
        et = self._fs.get_entity_type(entity_type_id=entity_type)
        et.delete_feature_values(entity_ids=[entity_id])
        logger.info("GDPR: deleted entity %s from %s", entity_id, entity_type)
