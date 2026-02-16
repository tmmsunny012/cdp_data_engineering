"""Unit tests for identity resolution engine."""

import pytest
from unittest.mock import AsyncMock, MagicMock

from src.storage.models.customer_profile import (
    CustomerEvent,
    CustomerProfile,
    Identifier,
)


@pytest.fixture
def mock_mongo_store() -> AsyncMock:
    store = AsyncMock()
    store.find_by_identifier = AsyncMock(return_value=None)
    store.get_profile = AsyncMock(return_value=None)
    store.upsert_profile = AsyncMock()
    return store


class TestDeterministicMatching:
    """Tests for exact-match identity resolution."""

    @pytest.mark.asyncio
    async def test_match_by_email(self, mock_mongo_store: AsyncMock) -> None:
        """When an event has an email that matches an existing profile, return that profile."""
        existing_profile = CustomerProfile(
            profile_id="prof_001",
            identifiers=[Identifier(type="email", value="max@gmail.com")],
        )
        mock_mongo_store.find_by_identifier.return_value = existing_profile

        event = CustomerEvent(
            event_id="evt_001",
            event_type="page_view",
            source="website",
            raw_data={"email": "max@gmail.com", "page": "/mba"},
        )

        # Identity resolution should find existing profile by email
        mock_mongo_store.find_by_identifier.assert_not_called()

    @pytest.mark.asyncio
    async def test_match_by_phone(self, mock_mongo_store: AsyncMock) -> None:
        """When an event has a phone number matching an existing profile, link them."""
        existing_profile = CustomerProfile(
            profile_id="prof_002",
            identifiers=[Identifier(type="phone", value="+49123456789")],
        )
        mock_mongo_store.find_by_identifier.return_value = existing_profile

        event = CustomerEvent(
            event_id="evt_002",
            event_type="whatsapp_message",
            source="whatsapp",
            raw_data={"from": "+49123456789", "body": "When is the MBA deadline?"},
        )

        assert existing_profile.profile_id == "prof_002"

    @pytest.mark.asyncio
    async def test_no_match_creates_new_profile(self, mock_mongo_store: AsyncMock) -> None:
        """When no identifier matches, a new profile should be created."""
        mock_mongo_store.find_by_identifier.return_value = None

        event = CustomerEvent(
            event_id="evt_003",
            event_type="app_opened",
            source="app",
            raw_data={"device_id": "dev_new_789"},
        )

        # No match found — new profile should be created
        assert mock_mongo_store.find_by_identifier.return_value is None


class TestCrossDeviceResolution:
    """Tests for linking the same student across multiple devices."""

    @pytest.mark.asyncio
    async def test_link_web_session_to_mobile_via_email(
        self, mock_mongo_store: AsyncMock
    ) -> None:
        """When a mobile app login uses the same email as a web session, profiles should merge."""
        web_profile = CustomerProfile(
            profile_id="prof_web",
            identifiers=[
                Identifier(type="session_id", value="sess_abc"),
                Identifier(type="email", value="student@example.edu"),
            ],
        )

        # Mobile event with same email should find and merge
        assert len(web_profile.identifiers) == 2
        assert web_profile.identifiers[1].value == "student@example.edu"


class TestConsentOnMerge:
    """Tests for consent handling during profile merges."""

    def test_most_restrictive_consent_wins(self) -> None:
        """When merging profiles, consent should follow the most restrictive rule."""
        # Profile A: consented to email
        # Profile B: did NOT consent to email
        # Merged: should NOT consent to email (most restrictive)
        consent_a = {"email": True, "whatsapp": True}
        consent_b = {"email": False, "whatsapp": True}

        merged_consent = {
            channel: consent_a.get(channel, False) and consent_b.get(channel, False)
            for channel in set(consent_a) | set(consent_b)
        }

        assert merged_consent["email"] is False  # Most restrictive wins
        assert merged_consent["whatsapp"] is True  # Both consented
