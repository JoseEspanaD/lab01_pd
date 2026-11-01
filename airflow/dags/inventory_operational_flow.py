"""Airflow 3 chain for manual runs and an optional historical simulation clock."""

from datetime import timedelta

from airflow.sdk import dag, get_current_context, task

from inventory.operations.simulation_schedule import schedule_from_environment


_simulation = schedule_from_environment()
if _simulation is not None:
    from airflow.timetables.trigger import CronTriggerTimetable

    _timetable = CronTriggerTimetable(_simulation.cron, timezone="UTC")
else:
    _timetable = None


@dag(
    dag_id="inventory_operational_flow",
    schedule=_timetable,
    start_date=(_simulation.wall_start_at if _simulation else None),
    end_date=(_simulation.wall_end_at if _simulation else None),
    catchup=bool(_simulation),
    max_active_runs=1,
    max_active_tasks=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    is_paused_upon_creation=bool(_simulation),
    tags=["laboratorio", "operational"],
)
def inventory_operational_flow():
    @task
    def validate():
        from inventory.operations.simulation_schedule import resolve_run

        context = get_current_context()
        run = context["dag_run"]
        return resolve_run(run.conf, run.run_type, context.get("logical_date"), _simulation)

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
            plan["forecast_config"],
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
