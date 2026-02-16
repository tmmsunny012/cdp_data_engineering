"""Identity Resolution Engine for CDP.

Links incoming events to unified customer profiles using deterministic
(exact identifier match) and probabilistic (fuzzy name + partial ID overlap)
strategies.  Merge decisions are audit-logged.
"""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from difflib import SequenceMatcher
from typing import Any, Optional

from motor.motor_asyncio import AsyncIOMotorDatabase

logger = logging.getLogger(__name__)

CONFIDENCE_AUTO_MERGE: float = 0.85
DETERMINISTIC_FIELDS = ("email", "phone", "device_id", "session_id", "salesforce_id")


class IdentityResolver:
    """Resolves an inbound event to a single profile_id."""

    def __init__(self, db: AsyncIOMotorDatabase) -> None:
        self._profiles = db["profiles"]
        self._audit_log = db["identity_audit_log"]

    # ── public entry point ───────────────────────────────────────────
    async def resolve(self, event: dict[str, Any]) -> str:
        identifiers: list[dict[str, str]] = event.get("identifiers", [])
        personal_info: dict[str, Any] = event.get("personal_info", {})

        profile_id = await self._deterministic_match(identifiers)
        if profile_id:
            logger.debug("Deterministic match: %s", profile_id)
            return profile_id

        prob_result = await self._probabilistic_match(personal_info, identifiers)
        if prob_result:
            matched_id, confidence = prob_result
            if confidence >= CONFIDENCE_AUTO_MERGE:
                logger.info(
                    "Auto-merge probabilistic match %s (conf=%.2f)",
                    matched_id, confidence,
                )
                return matched_id
            await self._flag_for_review(event, matched_id, confidence)
            logger.warning(
                "Low-confidence match %s (conf=%.2f) — flagged for review",
                matched_id, confidence,
            )

        return await self._create_profile(event)

    # ── deterministic (exact) ────────────────────────────────────────
    async def _deterministic_match(
        self, identifiers: list[dict[str, str]]
    ) -> Optional[str]:
        for ident in identifiers:
            field = ident.get("type", "")
            value = ident.get("value", "")
            if field in DETERMINISTIC_FIELDS and value:
                doc = await self._profiles.find_one(
                    {f"identifiers.{field}": value},
                    projection={"profile_id": 1},
                )
                if doc:
                    return str(doc["profile_id"])
        return None

    # ── probabilistic (fuzzy) ────────────────────────────────────────
    async def _probabilistic_match(
        self,
        personal_info: dict[str, Any],
        identifiers: list[dict[str, str]],
    ) -> Optional[tuple[str, float]]:
        id_values = {i["value"] for i in identifiers if i.get("value")}
        if not personal_info.get("name") or not id_values:
            return None

        candidates = self._profiles.find(
            {"identifiers": {"$elemMatch": {"value": {"$in": list(id_values)}}}}
        )
        best: tuple[str, float] | None = None
        async for candidate in candidates:
            cand_name = candidate.get("personal_info", {}).get("name", "")
            name_score = SequenceMatcher(
                None,
                personal_info["name"].lower(),
                cand_name.lower(),
            ).ratio()
            cand_ids = {
                v for entry in candidate.get("identifiers_list", [])
                if (v := entry.get("value"))
            }
            overlap = len(id_values & cand_ids) / max(len(id_values | cand_ids), 1)
            confidence = 0.6 * name_score + 0.4 * overlap
            if best is None or confidence > best[1]:
                best = (str(candidate["profile_id"]), confidence)
        return best

    # ── merge ────────────────────────────────────────────────────────
    async def _merge_profiles(
        self, primary_id: str, secondary_id: str
    ) -> None:
        secondary = await self._profiles.find_one({"profile_id": secondary_id})
        if secondary is None:
            return
        primary = await self._profiles.find_one({"profile_id": primary_id})
        if primary is None:
            return

        merged_ids = {
            *primary.get("identifiers_list", []),
            *secondary.get("identifiers_list", []),
        }
        # Consent: most restrictive wins
        pri_consent = primary.get("consent", {})
        sec_consent = secondary.get("consent", {})
        merged_consent = {
            k: pri_consent.get(k, False) and sec_consent.get(k, False)
            for k in {*pri_consent, *sec_consent}
        }
        await self._profiles.update_one(
            {"profile_id": primary_id},
            {"$set": {"identifiers_list": list(merged_ids), "consent": merged_consent}},
        )
        await self._profiles.delete_one({"profile_id": secondary_id})
        await self._audit_log.insert_one({
            "action": "merge",
            "primary_id": primary_id,
            "secondary_id": secondary_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
        logger.info("Merged profile %s into %s", secondary_id, primary_id)

    # ── create ───────────────────────────────────────────────────────
    async def _create_profile(self, event: dict[str, Any]) -> str:
        profile_id = str(uuid.uuid4())
        doc = {
            "profile_id": profile_id,
            "personal_info": event.get("personal_info", {}),
            "identifiers": {
                i["type"]: i["value"] for i in event.get("identifiers", [])
            },
            "identifiers_list": event.get("identifiers", []),
            "consent": event.get("consent", {}),
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        await self._profiles.insert_one(doc)
        await self._audit_log.insert_one({
            "action": "create",
            "profile_id": profile_id,
            "timestamp": doc["created_at"],
        })
        logger.info("Created new profile %s", profile_id)
        return profile_id

    # ── review flag ──────────────────────────────────────────────────
    async def _flag_for_review(
        self, event: dict[str, Any], candidate_id: str, confidence: float
    ) -> None:
        await self._audit_log.insert_one({
            "action": "review_flag",
            "candidate_id": candidate_id,
            "confidence": confidence,
            "event_snapshot": event,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
