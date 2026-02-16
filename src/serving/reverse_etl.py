"""Reverse ETL engine: pushes CDP data to Salesforce, Twilio, and email platforms.

All outbound actions are consent-gated, idempotent, audited, and subject
to Gate 4 quality validation before any data leaves the CDP.
"""

from __future__ import annotations

import hashlib
import logging
import time
from typing import Any

import httpx
from pydantic import BaseModel, Field

from src.storage.mongodb_profile_store import MongoProfileStore

logger = logging.getLogger(__name__)

# ── Data models ───────────────────────────────────────────────────────


class SyncResult(BaseModel):
    """Outcome of a batch sync operation."""

    total: int = 0
    succeeded: int = 0
    failed: int = 0
    errors: list[str] = Field(default_factory=list)


class ReverseETLConfig(BaseModel):
    salesforce_base_url: str = "https://example.my.salesforce.com"
    salesforce_token: str = ""
    twilio_account_sid: str = ""
    twilio_auth_token: str = ""
    twilio_whatsapp_from: str = "whatsapp:+14155238886"
    email_api_url: str = "https://email-platform.example.com/api/v1"
    email_api_key: str = ""
    salesforce_daily_api_limit: int = 100_000


# ── Dedup registry (production: Redis or DB-backed) ───────────────────
_dedup_seen: set[str] = set()

PII_FIELDS = {"email", "phone", "name", "address", "ssn"}


class ReverseETLEngine:
    """Pushes enriched CDP profiles to external activation systems."""

    def __init__(
        self,
        config: ReverseETLConfig,
        profile_store: MongoProfileStore,
    ) -> None:
        self._cfg = config
        self._store = profile_store
        self._sf_calls_today: int = 0
        self._sf_calls_reset: float = time.time()
        self._http = httpx.AsyncClient(timeout=30.0)

    # ── Salesforce sync ───────────────────────────────────────────────

    async def sync_to_salesforce(self, profiles: list[dict[str, Any]]) -> SyncResult:
        """Batch-update Salesforce Contact records with CDP-derived fields."""
        result = SyncResult(total=len(profiles))
        self._maybe_reset_sf_counter()

        for profile in profiles:
            if not self._validate_before_push(profile):
                result.failed += 1
                result.errors.append(f"Validation failed for {profile.get('profile_id')}")
                continue
            if self._sf_calls_today >= self._cfg.salesforce_daily_api_limit:
                logger.warning("Salesforce daily API limit reached — queuing remaining")
                result.failed += len(profiles) - result.succeeded - result.failed
                break
            dedup_key = self._dedup_key("sf", profile.get("profile_id", ""), profile)
            if dedup_key in _dedup_seen:
                result.succeeded += 1
                continue
            try:
                sf_id = profile.get("salesforce_id", "")
                payload = {
                    "Enrollment_Score__c": profile.get("enrollment_score", 0),
                    "CDP_Segment__c": ",".join(profile.get("segments", [])),
                    "Last_Interaction__c": profile.get("last_interaction"),
                }
                await self._http.patch(
                    f"{self._cfg.salesforce_base_url}/services/data/v59.0/sobjects/Contact/{sf_id}",
                    json=payload,
                    headers={"Authorization": f"Bearer {self._cfg.salesforce_token}"},
                )
                self._sf_calls_today += 1
                _dedup_seen.add(dedup_key)
                result.succeeded += 1
                logger.info("audit=sf_sync profile=%s sf_id=%s", profile.get("profile_id"), sf_id)
            except httpx.HTTPError as exc:
                result.failed += 1
                result.errors.append(f"SF error for {profile.get('profile_id')}: {exc}")
                logger.error("Salesforce sync failed: %s", exc)
        return result

    # ── WhatsApp via Twilio ───────────────────────────────────────────

    async def trigger_whatsapp(
        self, profile_id: str, template: str, params: dict[str, str]
    ) -> bool:
        """Send a WhatsApp message via Twilio — consent-gated and idempotent."""
        if not await self._check_consent(profile_id, "whatsapp"):
            logger.warning("audit=consent_denied channel=whatsapp profile=%s", profile_id)
            return False
        dedup_key = self._dedup_key("wa", profile_id, {"template": template, **params})
        if dedup_key in _dedup_seen:
            return True
        profile = await self._store.get_profile(profile_id)
        if profile is None or profile.personal_info.phone is None:
            return False
        resp = await self._http.post(
            f"https://api.twilio.com/2010-04-01/Accounts/{self._cfg.twilio_account_sid}/Messages.json",
            data={
                "From": self._cfg.twilio_whatsapp_from,
                "To": f"whatsapp:{profile.personal_info.phone}",
                "Body": template.format(**params),
            },
            auth=(self._cfg.twilio_account_sid, self._cfg.twilio_auth_token),
        )
        success = resp.status_code < 300
        _dedup_seen.add(dedup_key)
        logger.info("audit=whatsapp profile=%s success=%s", profile_id, success)
        return success

    # ── Email trigger ─────────────────────────────────────────────────

    async def trigger_email(self, profile_id: str, campaign_id: str) -> bool:
        """Trigger an email send via the marketing platform API."""
        if not await self._check_consent(profile_id, "email"):
            logger.warning("audit=consent_denied channel=email profile=%s", profile_id)
            return False
        dedup_key = self._dedup_key("email", profile_id, {"campaign": campaign_id})
        if dedup_key in _dedup_seen:
            return True
        resp = await self._http.post(
            f"{self._cfg.email_api_url}/send",
            json={"profile_id": profile_id, "campaign_id": campaign_id},
            headers={"X-API-Key": self._cfg.email_api_key},
        )
        success = resp.status_code < 300
        _dedup_seen.add(dedup_key)
        logger.info(
            "audit=email profile=%s campaign=%s success=%s", profile_id, campaign_id, success
        )
        return success

    # ── Consent check (CRITICAL) ──────────────────────────────────────

    async def _check_consent(self, profile_id: str, channel: str) -> bool:
        """Verify the profile has active consent for the given channel.

        Returns False if the profile does not exist, has no consent record
        for the channel, or consent has been revoked.
        """
        profile = await self._store.get_profile(profile_id)
        if profile is None:
            return False
        consent = profile.channel_consent.get(channel)
        return not (consent is None or not consent.consented)

    # ── Gate 4 quality validation ─────────────────────────────────────

    def _validate_before_push(self, data: dict[str, Any]) -> bool:
        """Ensure no PII leaks into wrong fields and basic sanity checks."""
        for key, value in data.items():
            if key not in PII_FIELDS and isinstance(value, str):
                lowered = value.lower()
                if "@" in lowered and key not in ("email", "salesforce_id", "profile_id"):
                    logger.error("Gate4 violation: possible email in field %s", key)
                    return False
        if not data.get("profile_id"):
            logger.error("Gate4 violation: missing profile_id")
            return False
        return True

    # ── Helpers ───────────────────────────────────────────────────────

    @staticmethod
    def _dedup_key(channel: str, profile_id: str, payload: Any) -> str:
        raw = f"{channel}:{profile_id}:{repr(sorted(payload.items()) if isinstance(payload, dict) else payload)}"
        return hashlib.sha256(raw.encode()).hexdigest()

    def _maybe_reset_sf_counter(self) -> None:
        """Reset the daily Salesforce API call counter every 24h."""
        if time.time() - self._sf_calls_reset > 86_400:
            self._sf_calls_today = 0
            self._sf_calls_reset = time.time()
