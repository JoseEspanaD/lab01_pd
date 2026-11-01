"""Manually publish daily forecasts and gross weekly replenishment by cut."""

from airflow.sdk import dag, get_current_context, task


@dag(
    dag_id="weekly_forecast_orders",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["laboratorio", "forecasting", "ordering"],
)
def weekly_forecast_orders():
    @task
    def publish():
        import os
        from pathlib import Path

        from inventory.operations.weekly_forecast import publish_weekly_plan

        return publish_weekly_plan(
            Path(os.environ["INVENTORY_INBOX_DIR"]),
            Path(os.environ["INVENTORY_PROCESSED_DIR"]),
            Path(os.environ["INVENTORY_PUBLISHED_DIR"]),
            Path(os.environ["INVENTORY_COSTS_PATH"]),
            get_current_context()["dag_run"].conf,
        )

    publish()


weekly_forecast_orders()
