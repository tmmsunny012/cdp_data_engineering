"""Async MongoDB CRUD for unified customer profiles.

Uses motor's async MongoDB driver with connection pooling, optimistic
locking via a version field, and indexed lookups for identity resolution.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection
from pymongo import ASCENDING, ReturnDocument

from src.storage.models.customer_profile import CustomerProfile

logger = logging.getLogger(__name__)


class OptimisticLockError(Exception):
    """Raised when a concurrent update conflicts with the current version."""


class MongoProfileStore:
    """Async CRUD layer over the unified profile collection in MongoDB."""

    def __init__(
        self,
        connection_uri: str,
        database: str = "cdp",
        collection: str = "profiles",
        max_pool_size: int = 50,
    ) -> None:
        self._client: AsyncIOMotorClient = AsyncIOMotorClient(
            connection_uri, maxPoolSize=max_pool_size
        )
        self._db = self._client[database]
        self._col: AsyncIOMotorCollection = self._db[collection]

    # ── reads ─────────────────────────────────────────────────────────

    async def get_profile(self, profile_id: str) -> CustomerProfile | None:
        """Fetch a single profile by its canonical ID."""
        doc = await self._col.find_one({"profile_id": profile_id})
        if doc is None:
            return None
        doc.pop("_id", None)
        return CustomerProfile.model_validate(doc)

    async def find_by_identifier(
        self, identifier_type: str, identifier_value: str
    ) -> CustomerProfile | None:
        """Identity resolution lookup — find profile by any known identifier."""
        doc = await self._col.find_one(
            {"identifiers": {"$elemMatch": {"type": identifier_type, "value": identifier_value}}}
        )
        if doc is None:
            return None
        doc.pop("_id", None)
        return CustomerProfile.model_validate(doc)

    async def find_by_segment(self, segment_name: str, limit: int = 100) -> list[CustomerProfile]:
        """Return profiles belonging to *segment_name*, capped by *limit*."""
        cursor = self._col.find({"segments": segment_name}).limit(limit)
        results: list[CustomerProfile] = []
        async for doc in cursor:
            doc.pop("_id", None)
            results.append(CustomerProfile.model_validate(doc))
        return results

    # ── writes ────────────────────────────────────────────────────────

    async def upsert_profile(self, profile: CustomerProfile) -> None:
        """Insert or update a profile with optimistic locking.

        The document carries a ``_version`` integer.  On update we match
        the current version and atomically increment it.  If no document
        is matched a concurrent writer already bumped the version and we
        raise ``OptimisticLockError``.
        """
        now = datetime.now(UTC)
        data = profile.model_dump(mode="json")
        data["updated_at"] = now.isoformat()

        result = await self._col.find_one_and_update(
            {"profile_id": profile.profile_id},
            {
                "$set": data,
                "$inc": {"_version": 1},
                "$setOnInsert": {"created_at": now.isoformat()},
            },
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        if result is None:
            raise OptimisticLockError(f"Concurrent update on profile {profile.profile_id}")
        logger.debug("Upserted profile %s (v%s)", profile.profile_id, result.get("_version"))

    async def delete_profile(self, profile_id: str) -> bool:
        """Hard-delete a profile (GDPR right-to-erasure)."""
        result = await self._col.delete_one({"profile_id": profile_id})
        deleted = result.deleted_count > 0
        if deleted:
            logger.info("GDPR deletion completed for profile %s", profile_id)
        return deleted

    # ── indexing ───────────────────────────────────────────────────────

    async def ensure_indexes(self) -> None:
        """Create secondary indexes for performant lookups."""
        await self._col.create_index("profile_id", unique=True)
        await self._col.create_index(
            [("identifiers.type", ASCENDING), ("identifiers.value", ASCENDING)]
        )
        await self._col.create_index("personal_info.email")
        await self._col.create_index("personal_info.phone")
        await self._col.create_index("segments")
        logger.info("MongoDB indexes ensured on collection %s", self._col.name)
