"""GDPR consent management for the CDP platform.

Manages per-channel consent for students with full audit trail,
consent versioning, and most-restrictive-wins merge semantics.
All mutations are logged to satisfy GDPR Art. 7 accountability requirements.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

CHANNELS: list[str] = ["email", "whatsapp", "push", "sms", "analytics", "profiling"]


class ConsentSource(str, Enum):
    STUDENT_PORTAL = "student_portal"
    API = "api"
    IMPORT = "import"


class ChannelConsentEntry(BaseModel):
    """Consent state for a single communication channel."""
    channel: str
    consented: bool = False
    legal_basis: str = "consent"
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    terms_version: str = "v1.0"


class ChannelConsent(BaseModel):
    """Aggregate consent state across all channels for a student."""
    student_id: str
    channels: dict[str, ChannelConsentEntry] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    last_modified: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class ConsentManager:
    """Manages GDPR-compliant consent records for students."""

    CURRENT_TERMS_VERSION: str = "v2.1"

    def __init__(self, mongo_client: AsyncIOMotorClient) -> None:
        db = mongo_client.get_default_database()
        self._consents = db["consents"]
        self._audit_log = db["consent_audit_log"]
        logger.info("ConsentManager initialised")

    async def get_consent(self, student_id: str) -> ChannelConsent:
        """Return the full per-channel consent record for a student."""
        doc = await self._consents.find_one({"student_id": student_id})
        if doc is None:
            return ChannelConsent(student_id=student_id)
        doc.pop("_id", None)
        return ChannelConsent(**doc)

    async def update_consent(
        self,
        student_id: str,
        channel: str,
        consented: bool,
        legal_basis: str,
        source: ConsentSource = ConsentSource.API,
    ) -> None:
        """Update consent for a specific channel and log the change."""
        if channel not in CHANNELS:
            raise ValueError(f"Unknown channel: {channel}. Must be one of {CHANNELS}")

        now = datetime.now(timezone.utc)
        existing = await self.get_consent(student_id)
        old_entry = existing.channels.get(channel)
        old_value = old_entry.consented if old_entry else None

        new_entry = ChannelConsentEntry(
            channel=channel,
            consented=consented,
            legal_basis=legal_basis,
            updated_at=now,
            terms_version=self.CURRENT_TERMS_VERSION,
        )

        await self._consents.update_one(
            {"student_id": student_id},
            {
                "$set": {
                    f"channels.{channel}": new_entry.model_dump(),
                    "last_modified": now,
                },
                "$setOnInsert": {"student_id": student_id, "created_at": now},
            },
            upsert=True,
        )

        audit_entry: dict[str, Any] = {
            "student_id": student_id,
            "channel": channel,
            "old_value": old_value,
            "new_value": consented,
            "legal_basis": legal_basis,
            "terms_version": self.CURRENT_TERMS_VERSION,
            "source": source.value,
            "timestamp": now,
        }
        await self._audit_log.insert_one(audit_entry)
        logger.info("Consent updated: student=%s channel=%s consented=%s", student_id, channel, consented)

    async def check_consent(self, student_id: str, channel: str) -> bool:
        """Quick boolean check -- suitable for pre-action gating."""
        doc = await self._consents.find_one(
            {"student_id": student_id}, {f"channels.{channel}.consented": 1}
        )
        if doc is None:
            return False
        entry = (doc.get("channels") or {}).get(channel)
        return bool(entry and entry.get("consented"))

    async def merge_consent(self, primary_id: str, secondary_id: str) -> None:
        """Merge consent records using MOST RESTRICTIVE rule (GDPR-safe)."""
        primary = await self.get_consent(primary_id)
        secondary = await self.get_consent(secondary_id)
        now = datetime.now(timezone.utc)

        for channel in CHANNELS:
            p_entry = primary.channels.get(channel)
            s_entry = secondary.channels.get(channel)
            p_consented = p_entry.consented if p_entry else False
            s_consented = s_entry.consented if s_entry else False
            # Most restrictive: consent only if BOTH profiles consented
            merged = p_consented and s_consented
            if p_entry is None or p_entry.consented != merged:
                await self.update_consent(
                    primary_id, channel, merged, "legitimate_interest", ConsentSource.API
                )

        await self._consents.delete_one({"student_id": secondary_id})
        logger.info("Consent merged: primary=%s secondary=%s (deleted)", primary_id, secondary_id)

    async def get_consent_audit_log(self, student_id: str) -> list[dict[str, Any]]:
        """Return the complete audit trail of consent changes for a student."""
        cursor = self._audit_log.find(
            {"student_id": student_id}
        ).sort("timestamp", 1)
        results: list[dict[str, Any]] = []
        async for doc in cursor:
            doc.pop("_id", None)
            results.append(doc)
        return results

    async def bulk_check(self, student_ids: list[str], channel: str) -> dict[str, bool]:
        """Batch consent check for bulk operations (e.g. campaign sends)."""
        cursor = self._consents.find(
            {"student_id": {"$in": student_ids}},
            {"student_id": 1, f"channels.{channel}.consented": 1},
        )
        result: dict[str, bool] = {sid: False for sid in student_ids}
        async for doc in cursor:
            entry = (doc.get("channels") or {}).get(channel)
            result[doc["student_id"]] = bool(entry and entry.get("consented"))
        return result
