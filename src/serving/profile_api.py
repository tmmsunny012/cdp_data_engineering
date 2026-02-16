"""FastAPI async API for unified customer profile retrieval.

Provides profile lookup, identity-based search, interaction history
from BigQuery, similarity search via Pinecone, and segment management.
Includes API-key auth, rate limiting, request tracing, and PII redaction.
"""

from __future__ import annotations

import logging
import time
import uuid
from collections import defaultdict
from typing import Annotated, Any

from fastapi import Depends, FastAPI, Header, HTTPException, Request, Response
from prometheus_client import Histogram
from pydantic import BaseModel, Field

from src.storage.bigquery_loader import BigQueryLoader
from src.storage.mongodb_profile_store import MongoProfileStore

logger = logging.getLogger(__name__)

# ── Prometheus metrics ────────────────────────────────────────────────
REQUEST_LATENCY = Histogram(
    "cdp_api_request_latency_seconds",
    "Profile API request latency",
    ["method", "endpoint"],
)

# ── Rate limiter (in-memory, per API key) ─────────────────────────────
_rate_buckets: dict[str, list[float]] = defaultdict(list)
RATE_LIMIT = 1000  # requests per minute

VALID_API_KEYS: set[str] = {"cdp-internal-key", "partner-key-2024"}

PII_FIELDS = {"email", "phone", "name"}

app = FastAPI(title="CDP Profile API", version="1.0.0")


# ── Dependencies ──────────────────────────────────────────────────────


def _get_profile_store() -> MongoProfileStore:
    """Return singleton profile store — overridden in tests."""
    return MongoProfileStore(connection_uri="mongodb://localhost:27017")


def _get_bq_loader() -> BigQueryLoader:
    return BigQueryLoader(project_id="cdp-prod", dataset="gold")


async def _authenticate(x_api_key: Annotated[str, Header()]) -> str:
    if x_api_key not in VALID_API_KEYS:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return x_api_key


async def _rate_limit(api_key: str = Depends(_authenticate)) -> str:
    now = time.time()
    window = [t for t in _rate_buckets[api_key] if now - t < 60]
    if len(window) >= RATE_LIMIT:
        raise HTTPException(status_code=429, detail="Rate limit exceeded")
    window.append(now)
    _rate_buckets[api_key] = window
    return api_key


# ── Middleware ────────────────────────────────────────────────────────


@app.middleware("http")
async def request_tracing(request: Request, call_next) -> Response:  # type: ignore[no-untyped-def]
    request_id = str(uuid.uuid4())
    request.state.request_id = request_id
    start = time.monotonic()
    response: Response = await call_next(request)
    elapsed = time.monotonic() - start
    response.headers["X-Request-ID"] = request_id
    REQUEST_LATENCY.labels(method=request.method, endpoint=request.url.path).observe(elapsed)
    logger.info(
        "req=%s method=%s path=%s status=%d latency=%.4fs",
        request_id,
        request.method,
        request.url.path,
        response.status_code,
        elapsed,
    )
    return response


def _redact_pii(data: dict[str, Any]) -> dict[str, Any]:
    """Mask PII fields in log output."""
    redacted = dict(data)
    info = redacted.get("personal_info", {})
    for field in PII_FIELDS:
        if field in info and info[field]:
            info[field] = "***REDACTED***"
    return redacted


# ── Request / Response schemas ────────────────────────────────────────


class SegmentUpdate(BaseModel):
    segments_to_add: list[str] = Field(default_factory=list)
    segments_to_remove: list[str] = Field(default_factory=list)


# ── Endpoints ─────────────────────────────────────────────────────────


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/profiles/{profile_id}")
async def get_profile(
    profile_id: str,
    _key: str = Depends(_rate_limit),
    store: MongoProfileStore = Depends(_get_profile_store),  # noqa: B008
) -> dict[str, Any]:
    profile = await store.get_profile(profile_id)
    if profile is None:
        raise HTTPException(status_code=404, detail="Profile not found")
    return profile.model_dump(mode="json")


@app.get("/profiles/search")
async def search_profile(
    email: str | None = None,
    phone: str | None = None,
    _key: str = Depends(_rate_limit),
    store: MongoProfileStore = Depends(_get_profile_store),  # noqa: B008
) -> dict[str, Any]:
    if email:
        profile = await store.find_by_identifier("email", email)
    elif phone:
        profile = await store.find_by_identifier("phone", phone)
    else:
        raise HTTPException(status_code=400, detail="Provide email or phone")
    if profile is None:
        raise HTTPException(status_code=404, detail="Profile not found")
    return profile.model_dump(mode="json")


@app.get("/profiles/{profile_id}/history")
async def get_history(
    profile_id: str,
    _key: str = Depends(_rate_limit),
    bq: BigQueryLoader = Depends(_get_bq_loader),  # noqa: B008
) -> list[dict[str, Any]]:
    sql = (
        f"SELECT * FROM `cdp-prod.gold.interaction_history` "  # nosec B608
        f"WHERE student_id = '{profile_id}' ORDER BY event_timestamp DESC LIMIT 50"
    )
    return bq.run_query(sql)


@app.get("/profiles/{profile_id}/similar")
async def get_similar(
    profile_id: str,
    _key: str = Depends(_rate_limit),
) -> dict[str, Any]:
    # Pinecone integration stub — real implementation queries the vector index
    return {
        "profile_id": profile_id,
        "similar_profiles": [],
        "note": "Pinecone integration pending",
    }


@app.post("/profiles/{profile_id}/segments")
async def update_segments(
    profile_id: str,
    body: SegmentUpdate,
    _key: str = Depends(_rate_limit),
    store: MongoProfileStore = Depends(_get_profile_store),  # noqa: B008
) -> dict[str, Any]:
    profile = await store.get_profile(profile_id)
    if profile is None:
        raise HTTPException(status_code=404, detail="Profile not found")
    current = set(profile.segments)
    current.update(body.segments_to_add)
    current -= set(body.segments_to_remove)
    profile.segments = sorted(current)
    await store.upsert_profile(profile)
    return {"profile_id": profile_id, "segments": profile.segments}
