"""Manually monitor a stored forecast publication at an explicit UTC cut."""

from airflow.sdk import dag, get_current_context, task


@dag(
    dag_id="monitor_published_forecasts",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["laboratorio", "monitoring"],
)
def monitor_published_forecasts():
    @task
    def evaluate():
        import os
        from pathlib import Path

        from inventory.operations.monitoring import monitor_and_record

        conf = get_current_context()["dag_run"].conf or {}
        return monitor_and_record(
            Path(os.environ["INVENTORY_INBOX_DIR"]),
            Path(os.environ["INVENTORY_PUBLISHED_DIR"]),
            conf.get("publication_id"), conf.get("evaluation_at"),
        )

    evaluate()


monitor_published_forecasts()
