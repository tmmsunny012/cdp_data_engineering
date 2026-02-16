"""Data quality validation gates for the CDP pipeline.

Four-gate validation system ensuring data integrity from ingestion through
reverse ETL. All gates produce structured ValidationResult objects for
monitoring and alerting.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import TYPE_CHECKING

import great_expectations as gx
from great_expectations.dataset import PandasDataset

if TYPE_CHECKING:
    import pandas as pd

logger = logging.getLogger(__name__)

ALLOWED_SOURCES: set[str] = {
    "moodle",
    "canvas",
    "hubspot",
    "salesforce",
    "website",
    "app",
    "manual_import",
}
EMAIL_REGEX = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"


class Severity(StrEnum):
    CRITICAL = "critical"
    WARNING = "warning"
    INFO = "info"


@dataclass
class ValidationResult:
    """Structured output from a data quality gate."""

    gate: str
    passed: bool
    failed_expectations: list[str] = field(default_factory=list)
    severity: Severity = Severity.INFO
    evaluated_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    record_count: int = 0
    metadata: dict[str, object] = field(default_factory=dict)


class CDPDataQualityChecker:
    """Four-gate data quality validation for the CDP pipeline."""

    def __init__(self) -> None:
        self._context = gx.get_context()
        logger.info("CDPDataQualityChecker initialised with GE context")

    def validate_gate1_ingestion(self, df: pd.DataFrame) -> ValidationResult:
        """Gate 1 -- raw event validation at ingestion time."""
        ds = PandasDataset(df)
        failures: list[str] = []

        if not ds.expect_column_to_exist("event_id").success:
            failures.append("event_id column missing")
        if not ds.expect_column_values_to_not_be_null("event_id").success:
            failures.append("event_id contains nulls")
        if not ds.expect_column_values_to_match_strftime_format(
            "timestamp", "%Y-%m-%dT%H:%M:%S"
        ).success:
            failures.append("timestamp not ISO8601")
        if not ds.expect_column_values_to_be_in_set("source", list(ALLOWED_SOURCES)).success:
            failures.append("source contains disallowed values")

        result = ValidationResult(
            gate="gate1_ingestion",
            passed=len(failures) == 0,
            failed_expectations=failures,
            severity=Severity.CRITICAL if failures else Severity.INFO,
            record_count=len(df),
        )
        logger.info("Gate 1 result: passed=%s failures=%d", result.passed, len(failures))
        return result

    def validate_gate2_bronze_to_silver(self, df: pd.DataFrame) -> ValidationResult:
        """Gate 2 -- cleaned data quality between bronze and silver layers."""
        ds = PandasDataset(df)
        failures: list[str] = []

        if not ds.expect_column_values_to_not_be_null("student_id").success:
            failures.append("student_id has null values")
        if not ds.expect_column_values_to_be_unique("event_id").success:
            failures.append("event_id is not unique")
        if not ds.expect_column_values_to_match_regex("email", EMAIL_REGEX).success:
            failures.append("email format invalid")
        if not ds.expect_column_values_to_be_of_type("student_id", "str").success:
            failures.append("student_id type is not str")

        result = ValidationResult(
            gate="gate2_bronze_to_silver",
            passed=len(failures) == 0,
            failed_expectations=failures,
            severity=Severity.CRITICAL if failures else Severity.INFO,
            record_count=len(df),
        )
        logger.info("Gate 2 result: passed=%s failures=%d", result.passed, len(failures))
        return result

    def validate_gate3_silver_to_gold(self, df: pd.DataFrame) -> ValidationResult:
        """Gate 3 -- profile quality validation before gold layer promotion."""
        ds = PandasDataset(df)
        failures: list[str] = []

        if not ds.expect_column_min_to_be_between(
            "profile_completeness_score", min_value=0.6
        ).success:
            failures.append("profile_completeness_score below 0.6 threshold")
        if not ds.expect_column_min_to_be_between("identity_confidence", min_value=0.85).success:
            failures.append("identity_confidence below 0.85 threshold")
        if not ds.expect_column_values_to_not_be_null("primary_profile_id").success:
            failures.append("orphan records detected (null primary_profile_id)")

        result = ValidationResult(
            gate="gate3_silver_to_gold",
            passed=len(failures) == 0,
            failed_expectations=failures,
            severity=Severity.WARNING if failures else Severity.INFO,
            record_count=len(df),
        )
        logger.info("Gate 3 result: passed=%s failures=%d", result.passed, len(failures))
        return result

    def validate_gate4_reverse_etl(self, df: pd.DataFrame, prev_count: int) -> ValidationResult:
        """Gate 4 -- reverse ETL safety checks before writing to destinations."""
        pii_fields = {"email", "phone", "first_name", "last_name", "date_of_birth"}
        failures: list[str] = []
        current_count = len(df)

        lower = prev_count * 0.8
        upper = prev_count * 1.2
        if not (lower <= current_count <= upper):
            failures.append(f"row count {current_count} outside +-20% of previous {prev_count}")

        ds = PandasDataset(df)
        if not ds.expect_column_values_to_not_be_null("salesforce_contact_id").success:
            failures.append("salesforce_contact_id contains nulls")

        marketing_cols = [c for c in df.columns if c.startswith("marketing_")]
        pii_in_marketing = [c for c in marketing_cols if c.replace("marketing_", "") in pii_fields]
        if pii_in_marketing:
            failures.append(f"PII detected in marketing fields: {pii_in_marketing}")

        result = ValidationResult(
            gate="gate4_reverse_etl",
            passed=len(failures) == 0,
            failed_expectations=failures,
            severity=Severity.CRITICAL if failures else Severity.INFO,
            record_count=current_count,
            metadata={"prev_count": prev_count},
        )
        logger.info("Gate 4 result: passed=%s failures=%d", result.passed, len(failures))
        return result

    def data_freshness_check(self, table: str, max_age_hours: int = 24) -> bool:
        """Verify the latest record in *table* is within the acceptable age window."""
        # Delegates to BigQuery metadata; implemented as a timestamp comparison.
        from google.cloud import bigquery

        client = bigquery.Client()
        query = f"SELECT MAX(_ingested_at) AS latest FROM `{table}`"
        rows = client.query(query).result()
        latest: datetime | None = next(iter(rows)).latest
        if latest is None:
            logger.warning("Table %s has no records", table)
            return False
        age = (datetime.now(UTC) - latest.replace(tzinfo=UTC)).total_seconds()
        fresh = age <= max_age_hours * 3600
        logger.info("Freshness check %s: age=%.1fh fresh=%s", table, age / 3600, fresh)
        return fresh

    def cross_source_agreement(
        self, profile_id: str, fields: list[str], threshold: float = 0.9
    ) -> bool:
        """Check field-level agreement across sources for a unified profile."""
        from google.cloud import bigquery

        client = bigquery.Client()
        field_list = ", ".join(fields)
        query = (
            f"SELECT source, {field_list} "
            f"FROM `cdp_silver.profile_attributes` "
            f"WHERE profile_id = @pid"
        )
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("pid", "STRING", profile_id)]
        )
        rows = list(client.query(query, job_config=job_config).result())
        if len(rows) < 2:
            return True

        agreements = 0
        comparisons = 0
        for f in fields:
            values = [getattr(r, f) for r in rows]
            for i in range(len(values)):
                for j in range(i + 1, len(values)):
                    comparisons += 1
                    if values[i] == values[j]:
                        agreements += 1

        ratio = agreements / comparisons if comparisons else 1.0
        passed = ratio >= threshold
        logger.info(
            "Cross-source agreement for %s: %.2f (threshold %.2f)", profile_id, ratio, threshold
        )
        return passed
