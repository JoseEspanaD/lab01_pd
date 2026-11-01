"""Finite historical replay derived from scheduler ticks, without live sales dates."""

from datetime import datetime, timedelta, timezone

import pytest

from inventory.operations.simulation_schedule import (
    config_for_tick, resolve_run, schedule_from_environment,
)


WALL = datetime(2025, 1, 1, 12, tzinfo=timezone.utc)


def _env(**changes: str) -> dict[str, str]:
    return {
        "INVENTORY_SIMULATION_ENABLED": "true",
        "INVENTORY_SIMULATION_WALL_START_AT": "2025-01-01T12:00:00Z",
        "INVENTORY_SIMULATION_FIRST_CUTOFF_AT": "2017-07-24T06:00:00Z",
        "INVENTORY_SIMULATION_TICK_MINUTES": "5",
        "INVENTORY_SIMULATION_TICKS": "10",
        **changes,
    }


def test_disabled_by_default_and_requires_complete_configuration() -> None:
    assert schedule_from_environment({}) is None
    assert schedule_from_environment({"INVENTORY_SIMULATION_ENABLED": "false"}) is None
    for changes in (
        {"INVENTORY_SIMULATION_ENABLED": "yes"},
        {"INVENTORY_SIMULATION_FIRST_CUTOFF_AT": ""},
        {"INVENTORY_SIMULATION_FIRST_CUTOFF_AT": "2017-07-24T05:00:00Z"},
        {"INVENTORY_SIMULATION_WALL_START_AT": "2025-01-01T12:01:00Z"},
        {"INVENTORY_SIMULATION_TICK_MINUTES": "7"},
        {"INVENTORY_SIMULATION_TICKS": "0"},
    ):
        with pytest.raises(ValueError):
            schedule_from_environment(_env(**changes))


def test_calendar_is_bounded_and_publication_ticks_are_seven_days_apart() -> None:
    schedule = schedule_from_environment(_env())
    assert schedule is not None
    assert schedule.cron == "*/5 * * * *"
    assert schedule.wall_end_at == WALL + timedelta(minutes=45)
    plans = [resolve_run({}, "scheduled", WALL + timedelta(minutes=5 * i), schedule)
             for i in range(10)]
    assert [i for i, plan in enumerate(plans) if plan["mode"] == "forecast"] == [0, 7]
    first, second = plans[0], plans[7]
    assert first["cutoff_at"] == "2017-07-24T06:00:00+00:00"
    assert first["evaluation_at"] == first["cutoff_at"]
    assert first["production"] == {"observation_date": "2017-07-23"}
    assert first["forecast_config"]["forecast_start_date"] == "2017-07-25"
    assert first["forecast_config"]["kind"] == "historical_simulation"
    assert first["forecast_config"]["train_start"] == "2017-06-04"
    assert second["cutoff_at"] == "2017-07-31T06:00:00+00:00"
    assert second["forecast_config"]["forecast_start_date"] == "2017-08-01"
    assert second["forecast_config"]["test_start"] == "2017-07-23"
    assert plans[2]["mode"] == "refresh"
    assert plans[2]["cutoff_at"] == "2017-07-26T06:00:00+00:00"
    assert plans[2]["production"] == {"observation_date": "2017-07-25"}
    assert "forecast_config" not in plans[2]
    assert plans[9]["cutoff_at"] == "2017-08-02T06:00:00+00:00"


def test_tick_mapping_is_deterministic_and_rejects_other_times() -> None:
    schedule = schedule_from_environment(_env())
    assert schedule is not None
    assert config_for_tick(WALL, schedule) == config_for_tick(WALL, schedule)
    for instant in (WALL - timedelta(minutes=5), WALL + timedelta(minutes=1),
                    WALL + timedelta(minutes=50), WALL.replace(tzinfo=None)):
        with pytest.raises(ValueError):
            config_for_tick(instant, schedule)
    with pytest.raises(ValueError):
        resolve_run({"mode": "refresh"}, "scheduled", WALL, schedule)
    with pytest.raises(ValueError):
        resolve_run({}, "scheduled", WALL, None)
    with pytest.raises(ValueError):
        resolve_run({}, "scheduled", None, schedule)


def test_historical_anchor_is_configurable_and_manual_backfill_preserved() -> None:
    schedule = schedule_from_environment(_env(
        INVENTORY_SIMULATION_FIRST_CUTOFF_AT="2016-03-14T06:00:00Z"))
    assert schedule is not None
    first = resolve_run(None, "scheduled", WALL, schedule)
    assert first["forecast_config"]["forecast_start_date"] == "2016-03-15"
    manual = {"mode": "refresh", "evaluation_at": "2017-08-03T06:00:00Z",
              "production": {"observation_date": "2017-08-02"}}
    assert resolve_run(manual, "manual", None, schedule)["cutoff_at"] == \
        "2017-08-03T06:00:00+00:00"
    correction = {"mode": "refresh", "evaluation_at": "2017-08-03T06:00:00Z",
                  "production": {"observation_date": "2017-08-01",
                                 "source": "transactions", "revision": 2,
                                 "available_at": "2017-08-03T06:00:00Z",
                                 "correction_path": "/opt/airflow/lab/data/producer-input/fix.csv"}}
    assert resolve_run(correction, "manual", None, schedule)["production"] == correction["production"]
    assert resolve_run({}, "backfill", WALL, schedule) == \
        resolve_run({}, "scheduled", WALL, schedule)
