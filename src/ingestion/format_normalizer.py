"""Central format normalisation module (KR3 solver).

Every raw event -- regardless of whether it arrives as JSON, a CSV row, or
unstructured WhatsApp text -- is converted into a canonical
:class:`CustomerEvent` by this module.

Responsibilities:
* Timezone normalisation (everything to UTC).
* Field-name mapping (source-specific names -> unified schema).
* Type coercion (strings to datetimes, ints, etc.).
* Basic NLP for WhatsApp text: intent detection and entity extraction.
"""

from __future__ import annotations

import logging
import re
import uuid
from datetime import UTC, datetime
from typing import Any

from src.storage.models.customer_profile import CustomerEvent, EventSource

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Timezone helpers
# ---------------------------------------------------------------------------

_COMMON_TZ_OFFSETS: dict[str, str] = {
    "CET": "+01:00",
    "CEST": "+02:00",
    "EST": "-05:00",
    "PST": "-08:00",
    "IST": "+05:30",
}


def _parse_timestamp(raw: Any) -> datetime:
    """Best-effort timestamp parsing; always returns UTC."""
    if isinstance(raw, datetime):
        if raw.tzinfo is None:
            return raw.replace(tzinfo=UTC)
        return raw.astimezone(UTC)
    if isinstance(raw, (int, float)):
        return datetime.fromtimestamp(raw, tz=UTC)
    if isinstance(raw, str):
        # Replace common named offsets before parsing.
        cleaned = raw.strip()
        for abbr, offset in _COMMON_TZ_OFFSETS.items():
            cleaned = cleaned.replace(abbr, offset)
        try:
            dt = datetime.fromisoformat(cleaned)
        except ValueError:
            logger.warning("Unparseable timestamp '%s'; defaulting to now()", raw)
            return datetime.now(UTC)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)
    return datetime.now(UTC)


# ---------------------------------------------------------------------------
# WhatsApp NLP helpers (lightweight, rule-based)
# ---------------------------------------------------------------------------

_INTENT_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("enrollment_inquiry", re.compile(r"\b(enroll|admission|apply|register)\b", re.I)),
    ("program_inquiry", re.compile(r"\b(program|course|degree|master|bachelor)\b", re.I)),
    ("fee_inquiry", re.compile(r"\b(fee|cost|price|tuition|payment)\b", re.I)),
    ("support_request", re.compile(r"\b(help|support|problem|issue|error)\b", re.I)),
    ("schedule_inquiry", re.compile(r"\b(schedule|deadline|start date|when)\b", re.I)),
]

_ENTITY_PATTERNS: dict[str, re.Pattern[str]] = {
    "email": re.compile(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+"),
    "phone": re.compile(r"\+?\d[\d\s\-()]{7,15}"),
    "program_name": re.compile(r"\b(?:B\.?Sc|M\.?Sc|MBA|B\.?A|M\.?A)\b\.?\s*\w*", re.I),
}


def _detect_intent(text: str) -> str:
    """Return the first matching intent or 'general_message'."""
    for intent, pattern in _INTENT_PATTERNS:
        if pattern.search(text):
            return intent
    return "general_message"


def _extract_entities(text: str) -> dict[str, list[str]]:
    """Extract known entity types from unstructured text."""
    entities: dict[str, list[str]] = {}
    for entity_type, pattern in _ENTITY_PATTERNS.items():
        matches = pattern.findall(text)
        if matches:
            entities[entity_type] = [m.strip() for m in matches]
    return entities


# ---------------------------------------------------------------------------
# Normalizer
# ---------------------------------------------------------------------------


class FormatNormalizer:
    """Stateless converter: raw data in any format -> :class:`CustomerEvent`."""

    def normalize_json(
        self,
        raw_data: dict[str, Any],
        source: EventSource,
    ) -> CustomerEvent:
        """Normalise a raw JSON payload into a :class:`CustomerEvent`.

        Args:
            raw_data: The raw JSON dict as received from the source.
            source: Originating system (website, app, crm, ...).

        Returns:
            A validated :class:`CustomerEvent`.
        """
        ts_raw = raw_data.get("timestamp") or raw_data.get("event_time")
        timestamp = _parse_timestamp(ts_raw) if ts_raw else datetime.now(UTC)

        event_type = raw_data.get("event_type") or raw_data.get("event") or "unknown"
        student_id = raw_data.get("user_id") or raw_data.get("student_id") or raw_data.get("Id")

        return CustomerEvent(
            event_id=raw_data.get("event_id", str(uuid.uuid4())),
            event_type=event_type,
            source=source,
            timestamp=timestamp,
            student_id=student_id,
            raw_data=raw_data,
            normalized_data=self._coerce_types(raw_data),
        )

    def normalize_csv_row(
        self,
        row: dict[str, Any],
        schema_map: dict[str, str],
    ) -> CustomerEvent:
        """Normalise a single CSV row using a field-name mapping.

        Args:
            row: Column-name -> value dict for a single CSV row.
            schema_map: Maps CSV column names to unified field names.

        Returns:
            A validated :class:`CustomerEvent`.
        """
        mapped: dict[str, Any] = {}
        for csv_col, cdp_field in schema_map.items():
            if csv_col in row:
                mapped[cdp_field] = row[csv_col]

        ts_raw = mapped.get("timestamp") or mapped.get("event_time")
        timestamp = _parse_timestamp(ts_raw) if ts_raw else datetime.now(UTC)

        return CustomerEvent(
            event_type=mapped.get("event_type", "csv_import"),
            source=EventSource.CRM,
            timestamp=timestamp,
            student_id=mapped.get("student_id") or mapped.get("salesforce_id"),
            raw_data=row,
            normalized_data=mapped,
        )

    def normalize_whatsapp_text(
        self,
        message_body: str,
        metadata: dict[str, Any],
    ) -> CustomerEvent:
        """Normalise a WhatsApp text message with basic NLP.

        Intent detection and entity extraction are rule-based (no ML model
        dependency) to keep the ingestion path fast and deterministic.

        Args:
            message_body: Raw text content of the WhatsApp message.
            metadata: Envelope data (from_number, message_sid, ...).

        Returns:
            A :class:`CustomerEvent` with intent and entities in
            ``normalized_data``.
        """
        intent = _detect_intent(message_body)
        entities = _extract_entities(message_body)

        return CustomerEvent(
            event_type=f"whatsapp.{intent}",
            source=EventSource.WHATSAPP,
            timestamp=_parse_timestamp(metadata.get("timestamp")),
            student_id=metadata.get("student_id"),
            raw_data={"body": message_body, **metadata},
            normalized_data={
                "intent": intent,
                "entities": entities,
                "from_number": metadata.get("from_number"),
                "message_sid": metadata.get("message_sid"),
                "body_length": len(message_body),
            },
        )

    # -- internal helpers -----------------------------------------------------

    @staticmethod
    def _coerce_types(data: dict[str, Any]) -> dict[str, Any]:
        """Best-effort type coercion for common fields."""
        coerced: dict[str, Any] = {}
        for key, value in data.items():
            if value is None:
                coerced[key] = value
            elif key.endswith("_at") or key == "timestamp":
                coerced[key] = str(_parse_timestamp(value))
            elif isinstance(value, str) and value.isdigit():
                coerced[key] = int(value)
            else:
                coerced[key] = value
        return coerced
