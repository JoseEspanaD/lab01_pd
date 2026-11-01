"""Ingest and validate simulated deliveries at an explicit UTC cutoff."""

from airflow.sdk import dag, get_current_context, task


@dag(
    dag_id="ingest_deliveries",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["laboratorio", "ingestion"],
)
def ingest_deliveries():
    @task
    def validate_inbox():
        import os
        from pathlib import Path

        from inventory.operations.ingestion import validate_and_record

        conf = get_current_context()["dag_run"].conf or {}
        report = validate_and_record(
            Path(os.environ["INVENTORY_INBOX_DIR"]),
            conf.get("cutoff_at"),
            Path(os.environ["INVENTORY_PROCESSED_DIR"]),
        )
        return {
            "cutoff_at": report["cutoff_at"],
            "selected_deliveries": len(report["selected_deliveries"]),
            "record_counts": report["record_counts"],
        }

    validate_inbox()


ingest_deliveries()
