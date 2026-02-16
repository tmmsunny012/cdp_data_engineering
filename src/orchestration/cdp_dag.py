"""CDP Batch Pipeline DAG — EdTech Customer Data Platform.

Orchestrates hourly transform and daily ML pipelines across the full
CDP stack: GCS ingestion -> BigQuery Bronze/Silver/Gold -> MongoDB,
Vertex AI Feature Store, Pinecone, and Salesforce reverse-ETL.
"""

from __future__ import annotations

import datetime
from typing import Any

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.slack.notifications.slack import send_slack_notification
from airflow.utils.trigger_rule import TriggerRule

_SLACK_CONN_ID = "slack_cdp_alerts"
_PAGERDUTY_CONN_ID = "pagerduty_cdp"
_GCS_BUCKET = Variable.get("cdp_gcs_raw_bucket", default_var="cdp-raw-prod")
_BQ_PROJECT = Variable.get("cdp_bq_project", default_var="cdp-prod")
_DBT_PROJECT_DIR = "/opt/airflow/dbt/cdp"

default_args: dict[str, Any] = {
    "owner": "cdp-platform-team",
    "retries": 2,
    "retry_delay": datetime.timedelta(minutes=5),
    "execution_timeout": datetime.timedelta(minutes=30),
    "on_failure_callback": send_slack_notification(
        slack_conn_id=_SLACK_CONN_ID,
        text="CDP task {{ ti.task_id }} failed in {{ ti.dag_id }} — run {{ run_id }}",
    ),
}


@dag(
    dag_id="cdp_batch_pipeline",
    schedule="@hourly",
    start_date=datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC),
    catchup=False,
    default_args=default_args,
    tags=["cdp", "batch", "production"],
    sla_miss_callback=send_slack_notification(
        slack_conn_id=_SLACK_CONN_ID,
        text="SLA BREACH: cdp_batch_pipeline exceeded 2-hour window — paging on-call.",
    ),
    dagrun_timeout=datetime.timedelta(hours=2),
    max_active_runs=1,
    doc_md=__doc__,
)
def cdp_batch_pipeline() -> None:
    """Entry-point DAG definition using TaskFlow API."""

    check_new_csv_files = GCSObjectExistenceSensor(
        task_id="check_new_csv_files",
        bucket=_GCS_BUCKET,
        object="salesforce/exports/{{ ds_nodash }}/*.csv",
        google_cloud_conn_id="google_cloud_default",
        timeout=600,
        poke_interval=60,
        mode="reschedule",
    )

    @task(task_id="ingest_csv")
    def ingest_csv(**context: Any) -> dict[str, int]:
        """Parse Salesforce CSV exports, validate schema, load to BigQuery Bronze."""
        from src.ingestion.csv_loader import load_csv_to_bronze

        ds: str = context["ds_nodash"]
        result = load_csv_to_bronze(
            gcs_uri=f"gs://{_GCS_BUCKET}/salesforce/exports/{ds}/*.csv",
            destination=f"{_BQ_PROJECT}.bronze.salesforce_raw",
        )
        return {"rows_loaded": result.rows_loaded, "files_processed": result.files}

    @task(task_id="bronze_to_silver")
    def bronze_to_silver() -> str:
        """Run dbt models that clean and normalise Bronze into Silver."""
        import subprocess

        result = subprocess.run(
            ["dbt", "run", "--select", "tag:silver", "--project-dir", _DBT_PROJECT_DIR],
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout

    @task(task_id="quality_gate_2")
    def quality_gate_silver() -> bool:
        """Great Expectations checkpoint against Silver tables."""
        from src.quality.expectations_runner import run_checkpoint

        return run_checkpoint(checkpoint_name="silver_validation")

    @task(task_id="silver_to_gold")
    def silver_to_gold() -> str:
        """Run dbt models that enrich and aggregate Silver into Gold."""
        import subprocess

        result = subprocess.run(
            ["dbt", "run", "--select", "tag:gold", "--project-dir", _DBT_PROJECT_DIR],
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout

    @task(task_id="quality_gate_3")
    def quality_gate_gold() -> bool:
        """Great Expectations checkpoint against Gold tables."""
        from src.quality.expectations_runner import run_checkpoint

        return run_checkpoint(checkpoint_name="gold_validation")

    @task(task_id="update_mongodb_profiles")
    def update_mongodb_profiles() -> int:
        """Sync Gold unified profiles to MongoDB for real-time serving."""
        from src.storage.mongo_sync import sync_profiles

        return sync_profiles(bq_table=f"{_BQ_PROJECT}.gold.unified_profiles")

    @task(task_id="compute_features")
    def compute_features() -> int:
        """Batch-ingest computed features into Vertex AI Feature Store."""
        from src.ml.feature_store import batch_ingest_features

        return batch_ingest_features(source_table=f"{_BQ_PROJECT}.gold.student_features")

    @task(task_id="generate_embeddings")
    def generate_embeddings() -> int:
        """Generate embeddings via Vertex AI and upsert to Pinecone."""
        from src.ml.embeddings import generate_and_upsert

        return generate_and_upsert(
            source_table=f"{_BQ_PROJECT}.gold.interaction_texts",
            pinecone_index="cdp-interactions",
        )

    @task(task_id="reverse_etl_salesforce")
    def reverse_etl_salesforce() -> int:
        """Push enriched Gold profiles back to Salesforce (consent-gated)."""
        from src.ingestion.salesforce_reverse_etl import sync_to_salesforce

        return sync_to_salesforce(
            source_table=f"{_BQ_PROJECT}.gold.unified_profiles",
            consent_table=f"{_BQ_PROJECT}.gold.consent_flags",
        )

    @task(task_id="data_freshness_check", trigger_rule=TriggerRule.ALL_DONE)
    def data_freshness_check() -> dict[str, bool]:
        """Verify all upstream sources have data fresher than their SLA."""
        from src.quality.freshness import check_all_sources

        return check_all_sources(project=_BQ_PROJECT)

    # ---- dependency wiring ----
    csv_data = ingest_csv()
    check_new_csv_files >> csv_data

    silver = bronze_to_silver()
    csv_data >> silver

    qg2 = quality_gate_silver()
    silver >> qg2

    gold = silver_to_gold()
    qg2 >> gold

    qg3 = quality_gate_gold()
    gold >> qg3

    # Fan-out from Gold quality gate
    mongo = update_mongodb_profiles()
    features = compute_features()
    embeddings = generate_embeddings()
    reverse_etl = reverse_etl_salesforce()

    qg3 >> [mongo, features, embeddings, reverse_etl]

    freshness = data_freshness_check()
    [mongo, features, embeddings, reverse_etl] >> freshness


cdp_batch_pipeline()
