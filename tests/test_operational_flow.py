"""Contracts for the manual chain and its incremental monitoring stage."""

from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
import runpy
import sys
from types import ModuleType

import pytest

from inventory.dated_source import publish_delivery
from inventory.forecasting import CostPolicy, Forecast
from inventory.operations.monitoring import monitor_visible_publications
from inventory.operations.workflow import validate_run
from inventory.published_forecasts import (
    InputReference, PublishedForecasts, _payload, _write_immutable,
)
from inventory.train_schema import TRAIN_COLUMNS


def _config() -> dict:
    return {
        "mode": "forecast", "forecast_start_date": "2017-08-01",
        "cutoff_at": "2017-07-31T06:00:00Z",
        "evaluation_at": "2017-08-01T06:00:00Z",
        "train_start": "2017-06-04", "validation_start": "2017-06-18",
        "selection_start": "2017-07-02", "test_start": "2017-07-16",
        "last_test_start": "2017-07-16", "report_cutoff_at": "2017-07-24T06:00:00Z",
        "calibration_first_start": "2017-07-16",
        "calibration_last_start": "2017-07-16", "kind": "historical_simulation",
        "production": {"observation_date": "2017-07-30"},
    }


def test_forecast_plan_propagates_one_decision_cut_and_training_range() -> None:
    plan = validate_run(_config())
    assert plan["cutoff_at"] == "2017-07-31T06:00:00+00:00"
    assert plan["evaluation_at"] == "2017-08-01T06:00:00+00:00"
    assert (plan["start_date"], plan["end_date"]) == ("2017-06-04", "2017-07-31")
    assert plan["production"] == {"observation_date": "2017-07-30"}


@pytest.mark.parametrize("changes", [
    {"cutoff_at": "2017-08-01T06:00:00Z"},
    {"evaluation_at": "2017-07-30T06:00:00Z"},
    {"report_cutoff_at": "2017-08-01T06:00:00Z"},
    {"production": {"observation_date": "2017-08-01"}},
    {"production": {"observation_date": "2017-07-30",
                    "available_at": "2017-08-01T06:00:00Z"}},
])
def test_plan_rejects_temporal_leak_before_production(changes: dict) -> None:
    with pytest.raises(ValueError):
        validate_run({**_config(), **changes})


def test_refresh_uses_evaluation_cut_and_excludes_forecast_inputs() -> None:
    plan = validate_run({"mode": "refresh", "evaluation_at": "2017-08-02T06:00:00Z",
                         "production": {"observation_date": "2017-08-01"}})
    assert plan["cutoff_at"] == plan["evaluation_at"]
    with pytest.raises(ValueError):
        validate_run({"mode": "refresh", "evaluation_at": "2017-08-02T06:00:00Z",
                      "forecast_start_date": "2017-08-01"})


def test_airflow_graph_orders_stages_and_retries(monkeypatch) -> None:
    monkeypatch.setenv("INVENTORY_SIMULATION_ENABLED", "false")
    edges = []
    settings = {}

    class Node:
        def __init__(self, name):
            self.name = name

        def __rshift__(self, other):
            edges.append((self.name, other.name))
            return other

    def fake_dag(**kwargs):
        settings.update(kwargs)
        return lambda function: function

    def fake_task(function):
        def call(*args):
            node = Node(function.__name__)
            edges.extend((arg.name, node.name) for arg in args if isinstance(arg, Node))
            return node
        return call

    airflow = ModuleType("airflow")
    sdk = ModuleType("airflow.sdk")
    sdk.dag, sdk.task = fake_dag, fake_task
    sdk.get_current_context = lambda: None
    monkeypatch.setitem(sys.modules, "airflow", airflow)
    monkeypatch.setitem(sys.modules, "airflow.sdk", sdk)
    dag_path = Path(__file__).parents[1] / "airflow/dags/inventory_operational_flow.py"
    runpy.run_path(str(dag_path))
    assert edges == [
        ("validate", "produce"), ("validate", "ingest"),
        ("validate", "prepare"), ("validate", "publish"),
        ("validate", "monitor"),
        ("produce", "ingest"), ("ingest", "prepare"),
        ("prepare", "publish"), ("publish", "monitor"),
    ]
    assert settings["schedule"] is None
    assert settings["max_active_runs"] == 1
    assert settings["default_args"]["retries"] == 1


def test_only_integrated_dag_has_an_opt_in_schedule(monkeypatch) -> None:
    monkeypatch.setenv("INVENTORY_SIMULATION_ENABLED", "true")
    monkeypatch.setenv("INVENTORY_SIMULATION_WALL_START_AT", "2025-01-01T12:00:00Z")
    monkeypatch.setenv("INVENTORY_SIMULATION_FIRST_CUTOFF_AT", "2017-07-24T06:00:00Z")
    monkeypatch.setenv("INVENTORY_SIMULATION_TICK_MINUTES", "5")
    monkeypatch.setenv("INVENTORY_SIMULATION_TICKS", "10")
    settings = {}

    class Node:
        def __rshift__(self, other):
            return other

    def fake_dag(**kwargs):
        settings.update(kwargs)
        return lambda function: function

    def fake_task(function):
        return lambda *args: Node()

    class FakeTimetable:
        def __init__(self, cron, timezone):
            self.cron, self.timezone = cron, timezone

    airflow = ModuleType("airflow")
    airflow.__path__ = []
    sdk = ModuleType("airflow.sdk")
    timetables = ModuleType("airflow.timetables")
    timetables.__path__ = []
    trigger = ModuleType("airflow.timetables.trigger")
    sdk.dag, sdk.task = fake_dag, fake_task
    sdk.get_current_context = lambda: None
    trigger.CronTriggerTimetable = FakeTimetable
    monkeypatch.setitem(sys.modules, "airflow", airflow)
    monkeypatch.setitem(sys.modules, "airflow.sdk", sdk)
    monkeypatch.setitem(sys.modules, "airflow.timetables", timetables)
    monkeypatch.setitem(sys.modules, "airflow.timetables.trigger", trigger)
    dag_path = Path(__file__).parents[1] / "airflow/dags/inventory_operational_flow.py"
    runpy.run_path(str(dag_path))
    assert settings["schedule"].cron == "*/5 * * * *"
    assert settings["schedule"].timezone == "UTC"
    assert settings["start_date"] == datetime(2025, 1, 1, 12, tzinfo=timezone.utc)
    assert settings["end_date"] == datetime(2025, 1, 1, 12, 45, tzinfo=timezone.utc)
    assert settings["catchup"] is True
    assert settings["is_paused_upon_creation"] is True
    for name in ("airflow_smoke", "ingest_deliveries", "prepare_training_data",
                 "weekly_forecast_orders", "monitor_published_forecasts"):
        path = dag_path.with_name(f"{name}.py")
        runpy.run_path(str(path))
        assert settings["schedule"] is None


def _at(day: date) -> datetime:
    return datetime.combine(day, time(6), tzinfo=timezone.utc)


def test_pending_backfill_late_revision_and_retry(tmp_path: Path) -> None:
    inbox, published = tmp_path / "inbox", tmp_path / "published"
    target = date(2017, 8, 1)
    publication_id = "flow-monitor-demo"
    cutoff = _at(target - timedelta(days=1))
    forecasts = tuple(Forecast(
        target, target + timedelta(days=offset), 1, "A", cutoff,
        0.0, 10.0, 20.0, 15.0,
    ) for offset in range(7))
    item = PublishedForecasts(
        publication_id, "historical_simulation", _at(date(2020, 1, 1)), None,
        target, cutoff, "baseline", "a" * 64, cutoff, cutoff,
        "b" * 64, "config/forecast_costs.json", "c" * 64,
        CostPolicy(5.0, 1.0, 0.05),
        (InputReference("source=train/observation_date=2017-07-30/data.csv", "d" * 64),),
        forecasts,
    )
    _write_immutable(published / "forecasts" / f"{publication_id}.json", _payload(item))

    initial = monitor_visible_publications(inbox, published, "2017-08-01T06:00:00Z")
    assert initial["reports"][0]["metrics"]["pending_pairs"] == 7
    initial_path = Path(initial["reports"][0]["report_path"])
    initial_bytes = initial_path.read_bytes()
    assert monitor_visible_publications(inbox, published, "2017-08-01T06:00:00Z")["reports"] == []

    publish_delivery("train", target, _at(target + timedelta(days=1)), 1,
                     TRAIN_COLUMNS, [["1", target.isoformat(), "1", "A", "0", "0"]], inbox)
    at_arrival = monitor_visible_publications(inbox, published, "2017-08-02T06:00:00Z")
    assert at_arrival["reports"][0]["metrics"]["evaluated_pairs"] == 1
    assert at_arrival["reports"][0]["metrics"]["bias"] == -10.0
    report_path = Path(at_arrival["reports"][0]["report_path"])
    saved = (report_path.read_bytes(), report_path.stat().st_mtime_ns)
    assert monitor_visible_publications(inbox, published, "2017-08-02T06:00:00Z")["reports"] == []
    assert (report_path.read_bytes(), report_path.stat().st_mtime_ns) == saved

    publish_delivery("train", target, _at(target + timedelta(days=2)), 2,
                     TRAIN_COLUMNS, [["1", target.isoformat(), "1", "A", "25", "0"]], inbox)
    revised = monitor_visible_publications(inbox, published, "2017-08-03T06:00:00Z")
    assert revised["reports"][0]["metrics"]["bias"] == 15.0
    assert initial_path.read_bytes() == initial_bytes
    assert report_path.read_bytes() == saved[0]
    assert monitor_visible_publications(inbox, published, "2017-08-02T06:00:00Z")["reports"] == []
