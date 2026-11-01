"""Validate one explicit operational run before any task mutates artifacts."""

from __future__ import annotations

from datetime import datetime, timezone, timedelta

from inventory.data_preparation import _prediction_cutoff
from inventory.operations.ingestion import parse_cutoff_at
from inventory.operations.preparation import _date
from inventory.operations.weekly_forecast import SPLIT_NAMES


def validate_run(config: dict) -> dict:
    """Return normalized task inputs for a forecast or an as-of refresh run."""
    if not isinstance(config, dict):
        raise ValueError("Se requiere dag_run.conf explícito")
    mode = config.get("mode")
    if mode not in ("forecast", "refresh"):
        raise ValueError("mode debe ser forecast o refresh")
    evaluation = parse_cutoff_at(config.get("evaluation_at"))
    if evaluation > datetime.now(timezone.utc):
        raise ValueError("evaluation_at no puede ser futuro")
    production = config.get("production")
    if production is not None:
        if not isinstance(production, dict):
            raise ValueError("production debe ser un objeto")
        _date(production.get("observation_date"), "observation_date")
        if set(production) - {"observation_date", "source", "revision",
                              "available_at", "correction_path"}:
            raise ValueError("Parámetros de producción desconocidos")
        if production.get("available_at") is not None:
            available = parse_cutoff_at(production["available_at"])
            if available > evaluation:
                raise ValueError("available_at supera evaluation_at")
    if mode == "forecast":
        target = _date(config.get("forecast_start_date"), "forecast_start_date")
        cutoff = parse_cutoff_at(config.get("cutoff_at"))
        if cutoff != _prediction_cutoff(target):
            raise ValueError("cutoff_at debe ser D-1 06:00 UTC")
        if evaluation < cutoff:
            raise ValueError("evaluation_at no puede preceder el corte del pronóstico")
        for name in (*SPLIT_NAMES,):
            _date(config.get(name), name)
        if parse_cutoff_at(config.get("report_cutoff_at")) > cutoff:
            raise ValueError("report_cutoff_at supera cutoff_at")
        if _date(config["calibration_last_start"], "calibration_last_start") + timedelta(days=6) >= target:
            raise ValueError("La calibración semanal debe terminar antes del objetivo")
        if config.get("kind") not in ("real", "historical_simulation"):
            raise ValueError("kind debe ser real o historical_simulation")
        if production is not None and _date(production["observation_date"], "observation_date") >= target:
            raise ValueError("La producción del pronóstico no puede incluir fechas objetivo")
        if production is not None and production.get("available_at") is not None:
            if parse_cutoff_at(production["available_at"]) > cutoff:
                raise ValueError("available_at supera cutoff_at")
        return {"mode": mode, "cutoff_at": cutoff.isoformat(),
                "evaluation_at": evaluation.isoformat(),
                "production": production,
                "start_date": config["train_start"],
                "end_date": (target - timedelta(days=1)).isoformat()}
    if "cutoff_at" in config or "forecast_start_date" in config:
        raise ValueError("refresh solo recibe evaluation_at y production")
    return {"mode": mode, "cutoff_at": evaluation.isoformat(),
            "evaluation_at": evaluation.isoformat(), "production": production}
