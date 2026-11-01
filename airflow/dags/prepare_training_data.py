"""Persist point-in-time training rows after ingest_deliveries has reported the cut."""

from airflow.sdk import dag, get_current_context, task


@dag(
    dag_id="prepare_training_data",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["laboratorio", "preparation"],
)
def prepare_training_data():
    @task
    def prepare():
        import os
        from pathlib import Path

        from inventory.operations.preparation import prepare_and_record

        conf = get_current_context()["dag_run"].conf or {}
        return prepare_and_record(
            Path(os.environ["INVENTORY_INBOX_DIR"]),
            Path(os.environ["INVENTORY_PROCESSED_DIR"]),
            conf.get("start_date"), conf.get("end_date"), conf.get("cutoff_at"),
        )

    prepare()


prepare_training_data()
