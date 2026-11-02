"""Persist the selected operational model and the calibrations used for a plan."""

from __future__ import annotations

from dataclasses import asdict
from datetime import date, datetime
from pathlib import Path

from inventory.data_preparation import prepare_forecast_examples
from inventory.forecasting import (
    EvaluationResult, forecast_week, load_costs, model_state_payload, predict_rows,
)
from inventory.ordering import (
    WeeklyOrder, calibrate_weekly_totals, require_evaluation_provenance,
    suggest_orders,
)
from inventory.published_forecasts import (
    _canonical, _read_envelope, _sha256, _write_immutable, read_publication,
)


def weekly_residuals(
    inbox_dir: Path, target: date, cutoff: datetime, evaluation: EvaluationResult,
    first: date, last: date, costs_path: Path, orders: list[WeeklyOrder],
) -> list[float]:
    """Reconstruct and check the existing ordering calculation without changing it."""
    selected = require_evaluation_provenance(evaluation, cutoff)
    costs = load_costs(costs_path)
    forecasts = forecast_week(
        inbox_dir, target, cutoff, selected.model, selected.daily_calibration, costs,
    )
    rows = prepare_forecast_examples(inbox_dir, first, last, cutoff, require_labels=True)
    historical = predict_rows(
        selected.model, selected.daily_calibration, rows, costs,
    )
    calibration = calibrate_weekly_totals(historical, rows, cutoff)
    if suggest_orders(forecasts, calibration, costs, selected) != orders:
        raise ValueError("La calibración semanal no reproduce los pedidos emitidos")
    return list(calibration.residuals)


def record_model(
    inbox_dir: Path, published_dir: Path, costs_path: Path, publication_id: str,
    target: date, cutoff: datetime, evaluation: EvaluationResult,
    first: date, last: date, orders: list[WeeklyOrder],
) -> Path:
    """Write one immutable model state linked to the original publication."""
    selected = require_evaluation_provenance(evaluation, cutoff)
    publication = read_publication(publication_id, published_dir=published_dir)
    if (publication.model_name != selected.name
            or publication.model_state_sha256 != selected.model_state_sha256
            or publication.daily_calibration_sha256
            != _sha256(_canonical(selected.daily_calibration.residuals))
            or publication.prediction_cutoff_at != cutoff):
        raise ValueError("La publicación no coincide con el modelo seleccionado")
    residuals = weekly_residuals(
        inbox_dir, target, cutoff, evaluation, first, last, costs_path, orders,
    )
    payload = {
        "schema_version": 1,
        "publication_id": publication_id,
        "forecast_start_date": target.isoformat(),
        "cutoff_at": cutoff.isoformat(),
        "name": selected.name,
        "state": model_state_payload(selected.model),
        "state_sha256": selected.model_state_sha256,
        "daily_residuals": list(selected.daily_calibration.residuals),
        "weekly_residuals": residuals,
        "model_available_at": selected.model_available_at.isoformat(),
        "daily_calibration_available_at": selected.daily_calibration_available_at.isoformat(),
        "ridge_penalty": evaluation.ridge_penalty,
        "selection_baseline": asdict(evaluation.validation_baseline),
        "selection_ml": asdict(evaluation.validation_ml),
        "test_baseline": asdict(evaluation.test_baseline),
        "test_ml": asdict(evaluation.test_ml),
        "costs_sha256": _sha256(Path(costs_path).read_bytes()),
    }
    path = Path(published_dir) / "model_states" / f"{publication_id}.json"
    _write_immutable(path, payload)
    if _read_envelope(path) != payload:
        raise ValueError("Conflicto: estado de modelo distinto")
    return path
