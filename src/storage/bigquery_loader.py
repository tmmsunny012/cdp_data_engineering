"""BigQuery streaming and batch loader for the CDP data warehouse.

Provides streaming inserts for real-time events, GCS-based batch loads for
large datasets, MERGE upserts for Silver-to-Gold promotion, GDPR deletion,
and ad-hoc analytics queries with cost tracking.
"""
from __future__ import annotations

import logging
import time
from typing import Any, Optional, Sequence

from google.api_core.exceptions import GoogleAPICallError, ServiceUnavailable
from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig, QueryJobConfig, SourceFormat
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)


class BigQueryLoader:
    """Unified BigQuery loader with streaming, batch, merge, and GDPR ops."""

    def __init__(self, project_id: str, dataset: str) -> None:
        self._project = project_id
        self._dataset = dataset
        self._client = bigquery.Client(project=project_id)

    def _table_ref(self, table: str) -> str:
        return f"{self._project}.{self._dataset}.{table}"

    # ── streaming inserts ─────────────────────────────────────────────

    @retry(
        retry=retry_if_exception_type(ServiceUnavailable),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=8),
    )
    async def stream_insert(self, table: str, rows: list[dict[str, Any]]) -> None:
        """Insert rows via the BigQuery streaming API (real-time path).

        Retries automatically on transient ``ServiceUnavailable`` errors.
        """
        ref = self._table_ref(table)
        errors = self._client.insert_rows_json(ref, rows)
        if errors:
            failed_indexes = [e["index"] for e in errors]
            logger.error(
                "Streaming insert errors on %s — failed row indexes: %s",
                ref,
                failed_indexes,
            )
            raise GoogleAPICallError(f"Streaming insert failures on {ref}: {errors}")
        logger.debug("Streamed %d rows into %s", len(rows), ref)

    # ── batch load from GCS ───────────────────────────────────────────

    def batch_load_from_gcs(
        self,
        table: str,
        gcs_uri: str,
        schema: list[bigquery.SchemaField],
        source_format: SourceFormat = SourceFormat.CSV,
    ) -> None:
        """Load data from a GCS URI into a BQ table (batch path).

        Supports CSV and Parquet.  Partitions by ``event_date`` and clusters
        by ``source``, ``student_id`` when the destination table supports it.
        """
        job_config = LoadJobConfig(
            schema=schema,
            source_format=source_format,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            time_partitioning=bigquery.TimePartitioning(field="event_date"),
            clustering_fields=["source", "student_id"],
            skip_leading_rows=1 if source_format == SourceFormat.CSV else 0,
        )
        ref = self._table_ref(table)
        load_job = self._client.load_table_from_uri(gcs_uri, ref, job_config=job_config)
        load_job.result()  # blocks until complete
        logger.info(
            "Batch load complete: %s → %s (%d rows)",
            gcs_uri, ref, load_job.output_rows or 0,
        )

    # ── merge upsert (Silver → Gold) ─────────────────────────────────

    def merge_upsert(
        self,
        target_table: str,
        staging_table: str,
        merge_keys: list[str],
    ) -> None:
        """Execute a MERGE statement to upsert staging rows into the target."""
        target = self._table_ref(target_table)
        staging = self._table_ref(staging_table)
        on_clause = " AND ".join(f"T.{k} = S.{k}" for k in merge_keys)
        sql = f"""
        MERGE `{target}` T
        USING `{staging}` S
        ON {on_clause}
        WHEN MATCHED THEN
            UPDATE SET T.updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
            INSERT ROW
        """
        self.run_query(sql)
        logger.info("MERGE complete: %s → %s", staging, target)

    # ── GDPR deletion ─────────────────────────────────────────────────

    def delete_student_data(self, student_id: str, tables: list[str]) -> None:
        """Delete all rows for *student_id* across the specified BQ tables."""
        for tbl in tables:
            ref = self._table_ref(tbl)
            sql = f"DELETE FROM `{ref}` WHERE student_id = @sid"
            job_config = QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("sid", "STRING", student_id)
                ]
            )
            job = self._client.query(sql, job_config=job_config)
            job.result()
            logger.info(
                "GDPR delete on %s for student %s — %d rows removed",
                ref, student_id, job.num_dml_affected_rows or 0,
            )

    # ── ad-hoc query ──────────────────────────────────────────────────

    def run_query(self, sql: str) -> list[dict[str, Any]]:
        """Execute an arbitrary SQL query and return rows as dicts.

        Logs the bytes billed for cost tracking.
        """
        start = time.monotonic()
        job = self._client.query(sql)
        rows = [dict(row) for row in job.result()]
        elapsed = time.monotonic() - start
        bytes_billed = job.total_bytes_billed or 0
        logger.info(
            "Query completed in %.2fs — %d rows, %.2f MB billed",
            elapsed, len(rows), bytes_billed / 1_048_576,
        )
        return rows
