"""Unified Profile Assembly for CDP.

Applies source-of-truth rules, recalculates engagement scores and segments,
and persists updates to MongoDB with optimistic-locking (version field).
"""

from __future__ import annotations

import logging
import math
from datetime import UTC, datetime
from typing import Any

from motor.motor_asyncio import AsyncIOMotorDatabase

logger = logging.getLogger(__name__)

# Source-of-truth precedence: which source owns which section.
CONTACT_INFO_AUTHORITY = "crm"
BEHAVIOUR_AUTHORITIES = frozenset({"website", "app"})

# Engagement scoring weights
RECENCY_WEIGHT: float = 0.55
FREQUENCY_WEIGHT: float = 0.45
RECENCY_HALF_LIFE_DAYS: float = 14.0

# Segment thresholds
SEGMENT_THRESHOLDS: dict[str, tuple[float, float]] = {
    "highly_engaged": (70.0, 100.0),
    "moderately_engaged": (40.0, 70.0),
    "at_risk": (15.0, 40.0),
    "dormant": (0.0, 15.0),
}


class OptimisticLockError(Exception):
    """Raised when the profile was modified concurrently."""


class ProfileBuilder:
    """Assembles and maintains unified customer profiles."""

    def __init__(self, db: AsyncIOMotorDatabase, max_retries: int = 3) -> None:  # type: ignore[type-arg]
        self._profiles = db["profiles"]
        self._max_retries = max_retries

    # ── public entry point ───────────────────────────────────────────
    async def update_profile(self, profile_id: str, event: dict[str, Any]) -> dict[str, Any]:
        for attempt in range(1, self._max_retries + 1):
            profile = await self._profiles.find_one({"profile_id": profile_id})
            if profile is None:
                raise ValueError(f"Profile {profile_id} not found")

            version = profile.get("version", 0)
            profile = self._apply_contact_info(profile, event)
            profile = self._update_interaction_summary(profile, event)
            profile = self._update_scores(profile, event)
            profile = self._update_segments(profile)
            profile = self._merge_identifiers(profile, event.get("identifiers", []))
            profile["version"] = version + 1
            profile["updated_at"] = datetime.now(UTC).isoformat()

            result = await self._profiles.replace_one(
                {"profile_id": profile_id, "version": version},
                profile,
            )
            if result.modified_count == 1:
                logger.debug("Profile %s updated (v%d)", profile_id, version + 1)
                return profile
            logger.warning(
                "Optimistic lock conflict on %s (attempt %d/%d)",
                profile_id,
                attempt,
                self._max_retries,
            )
        raise OptimisticLockError(
            f"Failed to update profile {profile_id} after {self._max_retries} retries"
        )

    # ── contact info (CRM is authority) ──────────────────────────────
    @staticmethod
    def _apply_contact_info(profile: dict[str, Any], event: dict[str, Any]) -> dict[str, Any]:
        source = event.get("source", "")
        if source == CONTACT_INFO_AUTHORITY and event.get("personal_info"):
            profile.setdefault("personal_info", {}).update(event["personal_info"])
        # Consent: always apply most restrictive
        incoming_consent = event.get("consent", {})
        existing_consent = profile.get("consent", {})
        for key in incoming_consent:
            existing_consent[key] = existing_consent.get(key, True) and incoming_consent[key]
        profile["consent"] = existing_consent
        return profile

    # ── interaction summary ──────────────────────────────────────────
    @staticmethod
    def _update_interaction_summary(
        profile: dict[str, Any], event: dict[str, Any]
    ) -> dict[str, Any]:
        summary = profile.setdefault(
            "interaction_summary",
            {
                "total_interactions": 0,
                "per_source_count": {},
                "last_interaction_at": None,
            },
        )
        summary["total_interactions"] += 1
        source = event.get("source", "unknown")
        summary["per_source_count"][source] = summary["per_source_count"].get(source, 0) + 1
        summary["last_interaction_at"] = event.get("timestamp") or datetime.now(UTC).isoformat()
        return profile

    # ── engagement score (recency + frequency) ───────────────────────
    @staticmethod
    def _update_scores(profile: dict[str, Any], event: dict[str, Any]) -> dict[str, Any]:
        summary = profile.get("interaction_summary", {})
        last_str = summary.get("last_interaction_at")
        if last_str:
            last_dt = datetime.fromisoformat(last_str)
            days_ago = max((datetime.now(UTC) - last_dt).total_seconds() / 86400, 0)
            recency = 100.0 * math.exp(-0.693 * days_ago / RECENCY_HALF_LIFE_DAYS)
        else:
            recency = 0.0

        total = summary.get("total_interactions", 0)
        frequency = min(100.0, total * 2.5)

        score = RECENCY_WEIGHT * recency + FREQUENCY_WEIGHT * frequency
        profile["engagement_score"] = round(score, 2)
        return profile

    # ── segment membership ───────────────────────────────────────────
    @staticmethod
    def _update_segments(profile: dict[str, Any]) -> dict[str, Any]:
        score = profile.get("engagement_score", 0.0)
        segments: list[str] = []
        for name, (low, high) in SEGMENT_THRESHOLDS.items():
            if low <= score < high:
                segments.append(name)
        profile["segments"] = segments
        return profile

    # ── identifier merge (dedup) ─────────────────────────────────────
    @staticmethod
    def _merge_identifiers(
        profile: dict[str, Any], new_identifiers: list[dict[str, str]]
    ) -> dict[str, Any]:
        existing: dict[str, str] = profile.get("identifiers", {})
        for ident in new_identifiers:
            id_type = ident.get("type", "")
            id_value = ident.get("value", "")
            if id_type and id_value and id_type not in existing:
                existing[id_type] = id_value
        profile["identifiers"] = existing
        return profile
