"""Unified customer profile and event models for the EdTech CDP.

Defines Pydantic v2 models that serve as the canonical schema across all
ingestion pipelines, storage layers, and downstream consumers.  The student
journey modelled here is Inquiry -> Application -> Enrollment -> Active -> Alumni.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field, field_validator

# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------


class EnrollmentStatus(StrEnum):
    """Lifecycle stages of a student."""

    ANONYMOUS = "anonymous"
    INQUIRY = "inquiry"
    APPLICATION = "application"
    ENROLLMENT = "enrollment"
    ACTIVE = "active"
    ALUMNI = "alumni"
    CHURNED = "churned"


class IdentifierType(StrEnum):
    """Supported cross-system identifier types."""

    EMAIL = "email"
    PHONE = "phone"
    DEVICE_ID = "device_id"
    SESSION_ID = "session_id"
    SALESFORCE_ID = "salesforce_id"


class EventSource(StrEnum):
    """Originating system for a customer event."""

    WEBSITE = "website"
    APP = "app"
    CRM = "crm"
    EMAIL = "email"
    WHATSAPP = "whatsapp"


# ---------------------------------------------------------------------------
# Sub-models
# ---------------------------------------------------------------------------


class Identifier(BaseModel):
    """A single identifier used to resolve a student across systems."""

    type: IdentifierType
    value: str = Field(..., min_length=1, max_length=512)


class PersonalInfo(BaseModel):
    """PII fields -- all optional to support anonymous/partial profiles."""

    name: str | None = None
    email: str | None = None
    phone: str | None = None


class ChannelConsent(BaseModel):
    """GDPR/ePrivacy consent record for a single communication channel."""

    consented: bool
    timestamp: datetime
    legal_basis: str = Field(..., description="E.g. 'legitimate_interest', 'explicit_consent'")
    version: str = Field(..., description="Consent policy version, e.g. '2.1'")


class InteractionSummary(BaseModel):
    """Aggregated interaction counters for quick profile reads."""

    total_events: int = 0
    last_interaction_at: datetime | None = None
    top_channels: dict[str, int] = Field(default_factory=dict)


class ProfileScores(BaseModel):
    """ML-derived scores attached to every profile."""

    engagement: float = Field(0.0, ge=0.0, le=1.0)
    churn_risk: float = Field(0.0, ge=0.0, le=1.0)
    enrollment_probability: float = Field(0.0, ge=0.0, le=1.0)


class SourceMetadata(BaseModel):
    """Provenance tracking for data lineage."""

    system: str
    ingested_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    pipeline_version: str = "1.0.0"


# ---------------------------------------------------------------------------
# Core models
# ---------------------------------------------------------------------------


class CustomerEvent(BaseModel):
    """Normalized event emitted by every ingestion pipeline.

    Regardless of the original source format (JSON, CSV, WhatsApp text),
    every event is converted into this canonical shape before downstream
    processing.
    """

    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str
    source: EventSource
    timestamp: datetime
    student_id: str | None = None
    raw_data: dict[str, Any] = Field(default_factory=dict)
    normalized_data: dict[str, Any] = Field(default_factory=dict)

    @field_validator("timestamp", mode="before")
    @classmethod
    def _ensure_utc(cls, v: Any) -> datetime:
        """Coerce naive datetimes to UTC."""
        if isinstance(v, datetime) and v.tzinfo is None:
            return v.replace(tzinfo=UTC)
        return v  # type: ignore[return-value]


class CustomerProfile(BaseModel):
    """Unified 360-degree student profile.

    This is the golden record assembled by the identity-resolution and
    profile-unification layers.  It aggregates data from Salesforce,
    website clickstream, mobile app, email, and WhatsApp channels.
    """

    profile_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    identifiers: list[Identifier] = Field(default_factory=list)
    personal_info: PersonalInfo = Field(default_factory=PersonalInfo)
    enrollment_status: EnrollmentStatus = EnrollmentStatus.ANONYMOUS
    segments: list[str] = Field(default_factory=list)
    channel_consent: dict[str, ChannelConsent] = Field(default_factory=dict)
    interaction_summary: InteractionSummary = Field(default_factory=InteractionSummary)
    scores: ProfileScores = Field(default_factory=ProfileScores)
    source_metadata: list[SourceMetadata] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
