"""Prometheus metrics definitions and FastAPI middleware for the CDP platform.

Exposes application-level counters, histograms, and gauges that map to every
critical stage of the CDP pipeline — from Kafka ingestion through identity
resolution, BigQuery processing, and real-time profile serving.  A lightweight
FastAPI middleware automatically instruments HTTP request duration and status
codes for any service that imports it.
"""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable

from fastapi import FastAPI, Request, Response
from prometheus_client import (
    REGISTRY,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response as StarletteResponse

# --------------------------------------------------------------------------- #
# Counters                                                                     #
# --------------------------------------------------------------------------- #
events_ingested_total = Counter(
    "events_ingested_total",
    "Total raw events ingested into the CDP pipeline.",
    labelnames=["source", "event_type"],
)

events_processed_total = Counter(
    "events_processed_total",
    "Total events that completed a given pipeline stage.",
    labelnames=["source", "pipeline_stage"],
)

dlq_messages_total = Counter(
    "dlq_messages_total",
    "Events routed to the Dead Letter Queue.",
    labelnames=["source", "error_type"],
)

identity_resolution_matches = Counter(
    "identity_resolution_matches",
    "Identity resolution outcomes by match strategy.",
    labelnames=["match_type"],
)

bigquery_bytes_processed = Counter(
    "bigquery_bytes_processed",
    "Cumulative bytes scanned/processed in BigQuery.",
)

# --------------------------------------------------------------------------- #
# Histograms                                                                   #
# --------------------------------------------------------------------------- #
processing_latency_seconds = Histogram(
    "processing_latency_seconds",
    "End-to-end latency for a pipeline stage.",
    labelnames=["pipeline_stage"],
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)

profile_api_request_duration_seconds = Histogram(
    "profile_api_request_duration_seconds",
    "HTTP request duration for the Profile API.",
    labelnames=["endpoint", "method"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5),
)

# --------------------------------------------------------------------------- #
# Gauges                                                                       #
# --------------------------------------------------------------------------- #
kafka_consumer_lag = Gauge(
    "kafka_consumer_lag",
    "Current consumer lag per topic/group.",
    labelnames=["topic", "consumer_group"],
)

active_profiles_total = Gauge(
    "active_profiles_total",
    "Number of unified profiles considered active (event in last 90 days).",
)

data_freshness_seconds = Gauge(
    "data_freshness_seconds",
    "Seconds since the most recent record for a given source.",
    labelnames=["source"],
)

# --------------------------------------------------------------------------- #
# FastAPI Prometheus middleware                                                 #
# --------------------------------------------------------------------------- #
_http_requests_total = Counter(
    "http_requests_total",
    "Total HTTP requests handled.",
    labelnames=["method", "endpoint", "status_code"],
)

_http_request_duration_seconds = Histogram(
    "http_request_duration_seconds",
    "HTTP request duration in seconds.",
    labelnames=["method", "endpoint"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)


class PrometheusMiddleware(BaseHTTPMiddleware):
    """Starlette middleware that records request count and latency."""

    async def dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        method = request.method
        path = request.url.path
        start = time.perf_counter()
        response: Response = await call_next(request)
        elapsed = time.perf_counter() - start

        _http_requests_total.labels(
            method=method,
            endpoint=path,
            status_code=response.status_code,
        ).inc()
        _http_request_duration_seconds.labels(
            method=method,
            endpoint=path,
        ).observe(elapsed)
        return response


# --------------------------------------------------------------------------- #
# Metrics app factory                                                          #
# --------------------------------------------------------------------------- #
def get_metrics_app() -> FastAPI:
    """Return a minimal FastAPI application that serves ``/metrics``.

    Intended to be mounted as a sub-application or run on a dedicated
    internal port so Prometheus can scrape it without exposing it to
    public traffic.
    """
    app = FastAPI(title="CDP Metrics", docs_url=None, redoc_url=None)

    @app.get("/metrics", include_in_schema=False)
    async def metrics_endpoint() -> StarletteResponse:
        body = generate_latest(REGISTRY)
        return StarletteResponse(
            content=body,
            media_type="text/plain; version=0.0.4; charset=utf-8",
        )

    return app
