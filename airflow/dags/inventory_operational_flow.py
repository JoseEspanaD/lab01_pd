"""Manual Airflow 3 chain for one point-in-time forecast or monitoring refresh."""

from datetime import timedelta

from airflow.sdk import dag, get_current_context, task


@dag(
    dag_id="inventory_operational_flow",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    is_paused_upon_creation=False,
    tags=["laboratorio", "operational"],
)
def inventory_operational_flow():
    @task
    def validate():
        from inventory.operations.workflow import validate_run

        return validate_run(get_current_context()["dag_run"].conf)

    @task
    def produce(plan):
        import os
        from pathlib import Path

        from inventory.operations.production import produce_for_date

        production = plan["production"]
        if production is None:
            return {"deliveries": []}
        return produce_for_date(
            production["observation_date"], plan["cutoff_at"],
            Path(os.environ["INVENTORY_RAW_DIR"]),
            Path(os.environ["INVENTORY_PRODUCER_INBOX_DIR"]),
            source=production.get("source", "all"),
            revision=production.get("revision", 1),
            available_at_text=production.get("available_at"),
            correction_path=(Path(production["correction_path"])
                             if production.get("correction_path") else None),
        )

    @task
    def ingest(plan):
        import os
        from pathlib import Path

        from inventory.operations.ingestion import validate_and_record

        report = validate_and_record(
            Path(os.environ["INVENTORY_INBOX_DIR"]), plan["cutoff_at"],
            Path(os.environ["INVENTORY_PROCESSED_DIR"]),
        )
        return {"cutoff_at": report["cutoff_at"],
                "selected_deliveries": len(report["selected_deliveries"])}

    @task
    def prepare(plan):
        import os
        from pathlib import Path

        from inventory.operations.preparation import prepare_and_record

        if plan["mode"] == "refresh":
            return {"skipped": True}
        return prepare_and_record(
            Path(os.environ["INVENTORY_INBOX_DIR"]),
            Path(os.environ["INVENTORY_PROCESSED_DIR"]),
            plan["start_date"], plan["end_date"], plan["cutoff_at"],
        )

    @task
    def publish(plan):
        import os
        from pathlib import Path

        from inventory.operations.weekly_forecast import publish_weekly_plan

        if plan["mode"] == "refresh":
            return {"skipped": True}
        return publish_weekly_plan(
            Path(os.environ["INVENTORY_INBOX_DIR"]),
            Path(os.environ["INVENTORY_PROCESSED_DIR"]),
            Path(os.environ["INVENTORY_PUBLISHED_DIR"]),
            Path(os.environ["INVENTORY_COSTS_PATH"]),
            get_current_context()["dag_run"].conf,
        )

    @task
    def monitor(plan):
        import os
        from pathlib import Path

        from inventory.operations.monitoring import monitor_visible_publications

        return monitor_visible_publications(
            Path(os.environ["INVENTORY_INBOX_DIR"]),
            Path(os.environ["INVENTORY_PUBLISHED_DIR"]),
            plan["evaluation_at"],
        )

    plan = validate()
    produced = produce(plan)
    ingested = ingest(plan)
    prepared = prepare(plan)
    published = publish(plan)
    monitored = monitor(plan)
    produced >> ingested >> prepared >> published >> monitored


inventory_operational_flow()
