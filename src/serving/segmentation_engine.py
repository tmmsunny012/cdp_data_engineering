"""Real-time segment evaluation engine for the EdTech CDP.

Evaluates customer profiles against configurable rules to compute segment
membership.  Publishes segment-change events to Kafka so downstream systems
(Salesforce, marketing automation) can react in near-real-time.
"""
from __future__ import annotations

import json
import logging
import operator
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from aiokafka import AIOKafkaProducer
from pydantic import BaseModel, Field

from src.storage.models.customer_profile import CustomerProfile

logger = logging.getLogger(__name__)

SEGMENT_CHANGES_TOPIC = "cdp.segment.changes"

# ── Operator map ──────────────────────────────────────────────────────
_OPS: dict[str, Any] = {
    ">=": operator.ge,
    "<=": operator.le,
    ">": operator.gt,
    "<": operator.lt,
    "==": operator.eq,
    "!=": operator.ne,
}


class SegmentRule(BaseModel):
    """A single predicate that can be chained with ``and_condition``."""
    field: str
    operator: str
    value: Any
    and_condition: Optional[SegmentRule] = Field(default=None, alias="and")

    model_config = {"populate_by_name": True}


class SegmentDefinition(BaseModel):
    name: str
    rule: SegmentRule


class SegmentationEngine:
    """Evaluates profiles against segment rules and publishes changes."""

    def __init__(self, kafka_brokers: str = "localhost:9092") -> None:
        self._rules: list[SegmentDefinition] = []
        self._producer: Optional[AIOKafkaProducer] = None
        self._kafka_brokers = kafka_brokers
        self._load_rules()

    # ── rule management ───────────────────────────────────────────────

    def _load_rules(self) -> None:
        """Load built-in segment definitions."""
        builtins: list[dict[str, Any]] = [
            {
                "name": "high_intent_prospect",
                "rule": {
                    "field": "interaction_summary.total_events",
                    "operator": ">=",
                    "value": 3,
                    "and": {
                        "field": "enrollment_status",
                        "operator": "==",
                        "value": "inquiry",
                    },
                },
            },
            {
                "name": "at_risk_student",
                "rule": {
                    "field": "days_since_last_login",
                    "operator": ">=",
                    "value": 14,
                    "and": {
                        "field": "enrollment_status",
                        "operator": "==",
                        "value": "active",
                    },
                },
            },
            {
                "name": "engaged_learner",
                "rule": {
                    "field": "interaction_summary.total_events",
                    "operator": ">=",
                    "value": 5,
                },
            },
            {
                "name": "mba_interested",
                "rule": {
                    "field": "viewed_mba_page",
                    "operator": "==",
                    "value": True,
                    "and": {
                        "field": "downloaded_brochure",
                        "operator": "==",
                        "value": True,
                    },
                },
            },
        ]
        for item in builtins:
            self._rules.append(SegmentDefinition.model_validate(item))
        logger.info("Loaded %d built-in segment rules", len(self._rules))

    def add_rule(self, segment_name: str, conditions: dict[str, Any]) -> None:
        """Dynamically register a new segment rule at runtime."""
        defn = SegmentDefinition(
            name=segment_name,
            rule=SegmentRule.model_validate(conditions),
        )
        self._rules.append(defn)
        logger.info("Added segment rule: %s", segment_name)

    # ── evaluation ────────────────────────────────────────────────────

    def _resolve_field(self, profile: CustomerProfile, field: str) -> Any:
        """Dot-notation field resolver against a profile object."""
        obj: Any = profile
        for part in field.split("."):
            if isinstance(obj, dict):
                obj = obj.get(part)
            else:
                obj = getattr(obj, part, None)
            if obj is None:
                return None
        return obj

    def _evaluate_rule(self, profile: CustomerProfile, rule: SegmentRule) -> bool:
        """Recursively evaluate a rule (with optional AND chain)."""
        actual = self._resolve_field(profile, rule.field)
        if actual is None:
            return False
        op_fn = _OPS.get(rule.operator)
        if op_fn is None:
            logger.warning("Unknown operator %s in segment rule", rule.operator)
            return False
        try:
            result = op_fn(actual, rule.value)
        except TypeError:
            return False
        if not result:
            return False
        if rule.and_condition is not None:
            return self._evaluate_rule(profile, rule.and_condition)
        return True

    async def evaluate(self, profile: CustomerProfile) -> list[str]:
        """Return segment names the profile qualifies for.

        Publishes a Kafka event whenever segment membership changes.
        """
        matched: list[str] = []
        for defn in self._rules:
            if self._evaluate_rule(profile, defn.rule):
                matched.append(defn.name)

        previous = set(profile.segments)
        current = set(matched)
        added = current - previous
        removed = previous - current

        if added or removed:
            await self._publish_change(profile.profile_id, added, removed)

        return matched

    async def _publish_change(
        self,
        profile_id: str,
        added: set[str],
        removed: set[str],
    ) -> None:
        """Publish segment change event to Kafka."""
        if self._producer is None:
            self._producer = AIOKafkaProducer(
                bootstrap_servers=self._kafka_brokers,
                value_serializer=lambda v: json.dumps(v).encode(),
            )
            await self._producer.start()
        event = {
            "profile_id": profile_id,
            "segments_added": sorted(added),
            "segments_removed": sorted(removed),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        await self._producer.send_and_wait(SEGMENT_CHANGES_TOPIC, value=event)
        logger.info("Segment change published for %s: +%s -%s", profile_id, added, removed)
