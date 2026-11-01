"""Map bounded Airflow scheduler ticks to historical UTC simulation cuts."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Mapping


@dataclass(frozen=True)
class SimulationSchedule:
    wall_start_at: datetime
    first_cutoff_at: datetime
    tick_minutes: int
    ticks: int

    @property
    def wall_end_at(self) -> datetime:
        return self.wall_start_at + timedelta(minutes=self.tick_minutes * (self.ticks - 1))

    @property
    def cron(self) -> str:
        return "0 * * * *" if self.tick_minutes == 60 else f"*/{self.tick_minutes} * * * *"


def schedule_from_environment(env: Mapping[str, str] | None = None) -> SimulationSchedule | None:
    """Return a schedule only after an explicit opt-in with complete UTC anchors."""
    env = os.environ if env is None else env
    enabled = env.get("INVENTORY_SIMULATION_ENABLED", "false").lower()
    if enabled == "false":
        return None
    if enabled != "true":
        raise ValueError("INVENTORY_SIMULATION_ENABLED debe ser true o false")
    from inventory.operations.ingestion import parse_cutoff_at

    wall = parse_cutoff_at(env.get("INVENTORY_SIMULATION_WALL_START_AT"))
    first = parse_cutoff_at(env.get("INVENTORY_SIMULATION_FIRST_CUTOFF_AT"))
    try:
        minutes = int(env["INVENTORY_SIMULATION_TICK_MINUTES"])
        ticks = int(env["INVENTORY_SIMULATION_TICKS"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("Se requieren TICK_MINUTES y TICKS enteros") from exc
    if minutes not in (1, 2, 3, 4, 5, 6, 10, 12, 15, 20, 30, 60):
        raise ValueError("TICK_MINUTES debe dividir una hora")
    if not 1 <= ticks <= 366:
        raise ValueError("TICKS debe estar entre 1 y 366")
    if wall.second or wall.microsecond or wall.minute % minutes:
        raise ValueError("WALL_START_AT debe coincidir con un tick del calendario UTC")
    if first.hour != 6 or first.minute or first.second or first.microsecond:
        raise ValueError("FIRST_CUTOFF_AT debe ser a las 06:00 UTC")
    try:
        result = SimulationSchedule(wall, first, minutes, ticks)
        result.wall_end_at
        first + timedelta(days=ticks)
    except OverflowError as exc:
        raise ValueError("El calendario excede el rango de fechas") from exc
    return result


def config_for_tick(logical_date: datetime, schedule: SimulationSchedule) -> dict:
    """Build the same explicit configuration used by manual runs for one tick."""
    if logical_date.tzinfo is None or logical_date.utcoffset() != timedelta(0):
        raise ValueError("La fecha lógica del tick debe ser UTC")
    # Airflow passes a Pendulum DateTime. Convert it to stdlib so timedelta
    # arithmetic (including modulo) has the same behavior in tests and workers.
    logical_date = datetime.fromisoformat(logical_date.isoformat()).astimezone(timezone.utc)
    elapsed = logical_date - schedule.wall_start_at
    interval = timedelta(minutes=schedule.tick_minutes)
    if elapsed < timedelta(0) or elapsed % interval != timedelta(0):
        raise ValueError("Tick fuera del calendario simulado")
    index = elapsed // interval
    if index >= schedule.ticks:
        raise ValueError("Tick fuera del intervalo histórico configurado")
    cutoff = schedule.first_cutoff_at + timedelta(days=index)
    observation = (cutoff - timedelta(days=1)).date().isoformat()
    production = {"observation_date": observation}
    if index % 7:
        return {"mode": "refresh", "evaluation_at": cutoff.isoformat(),
                "production": production}

    target = cutoff.date() + timedelta(days=1)
    dated = lambda days: (target - timedelta(days=days)).isoformat()
    return {
        "mode": "forecast", "forecast_start_date": target.isoformat(),
        "cutoff_at": cutoff.isoformat(), "evaluation_at": cutoff.isoformat(),
        "train_start": dated(51), "validation_start": dated(37),
        "selection_start": dated(23), "test_start": dated(9),
        "last_test_start": dated(9), "report_cutoff_at": cutoff.isoformat(),
        "calibration_first_start": dated(9), "calibration_last_start": dated(9),
        "kind": "historical_simulation", "production": production,
    }


def resolve_run(config: dict | None, run_type: str, logical_date: datetime | None,
                schedule: SimulationSchedule | None) -> dict:
    """Keep manual configurations intact and normalize scheduled/backfill runs."""
    from inventory.operations.workflow import validate_run

    kind = getattr(run_type, "value", run_type)
    if kind in ("scheduled", "backfill") and schedule is not None and not config:
        if logical_date is None:
            raise ValueError("Un run del calendario requiere fecha lógica UTC")
        config = config_for_tick(logical_date, schedule)
    elif kind == "scheduled":
        raise ValueError("Un run programado requiere calendario activo y conf vacío")
    plan = validate_run(config)
    if plan["mode"] == "forecast":
        plan["forecast_config"] = config
    return plan
