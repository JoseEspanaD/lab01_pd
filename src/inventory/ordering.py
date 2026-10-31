"""Gross weekly coverage from seven daily forecasts and historical weekly errors."""

import math
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

from .data_preparation import (
    ForecastExample, _prediction_cutoff, _validate_dates_and_cutoff,
    prepare_forecast_examples,
)
from .forecasting import (
    CostPolicy, EvaluationResult, Forecast, MeanBaseline, ResidualCalibration, RidgeForecaster,
    SelectedForecast, forecast_week, load_costs, model_state_sha256, predict_rows,
)


@dataclass(frozen=True)
class WeeklyOrder:
    forecast_start_date: date
    store_nbr: int
    family: str
    prediction_cutoff_at: datetime
    demand_q10: float
    demand_q50: float
    demand_q90: float
    decision_demand: float
    suggested_units: int
    costs: CostPolicy
    calibration_weeks: int
    selected_model: str
    model_available_at: datetime
    daily_calibration_available_at: datetime
    model_state_sha256: str


def _require_available(artifact: SelectedForecast, cutoff_at: datetime) -> None:
    """Reject absent or future provenance before a model is used at a cutoff."""
    if not isinstance(artifact, SelectedForecast):
        raise ValueError("No se puede acreditar la procedencia temporal del pronóstico")
    if (artifact.name not in ("baseline", "ml")
            or (artifact.name == "baseline" and type(artifact.model) is not MeanBaseline)
            or (artifact.name == "ml" and type(artifact.model) is not RidgeForecaster)
            or not isinstance(artifact.daily_calibration, ResidualCalibration)
            or any(not isinstance(available_at, datetime)
                   or available_at.tzinfo is None
                   or available_at.utcoffset() != timedelta(0)
                   for available_at in (
                       artifact.model_available_at,
                       artifact.daily_calibration_available_at,
                   ))):
        raise ValueError("No se puede acreditar la procedencia temporal del pronóstico")
    if (artifact.model_available_at > cutoff_at
            or artifact.daily_calibration_available_at > cutoff_at):
        raise ValueError("El modelo o la calibración diaria no estaban disponibles en el corte histórico")
    try:
        current_hash = model_state_sha256(artifact.model)
    except (TypeError, ValueError) as exc:
        raise ValueError("El estado del modelo seleccionado no puede acreditarse") from exc
    if current_hash != artifact.model_state_sha256:
        raise ValueError("El estado del modelo seleccionado cambió desde su evaluación")


def _group_forecasts(forecasts: list[Forecast]) -> dict[tuple[date, int, str], list[Forecast]]:
    groups: dict[tuple[date, int, str], list[Forecast]] = {}
    for forecast in forecasts:
        start = forecast.forecast_start_date
        if (not isinstance(start, date) or isinstance(start, datetime)
                or not isinstance(forecast.target_date, date)
                or isinstance(forecast.target_date, datetime)
                or type(forecast.store_nbr) is not int or forecast.store_nbr <= 0
                or not isinstance(forecast.family, str) or not forecast.family
                or forecast.prediction_cutoff_at != _prediction_cutoff(start)
                or not all(math.isfinite(value) and value >= 0 for value in (
                    forecast.q10, forecast.q50, forecast.q90, forecast.business_target))
                or not forecast.q10 <= forecast.q50 <= forecast.q90):
            raise ValueError("Pronóstico semanal inválido")
        groups.setdefault((start, forecast.store_nbr, forecast.family), []).append(forecast)
    for (start, _, _), group in groups.items():
        if (len(group) != 7
                or {item.target_date for item in group}
                != {start + timedelta(days=day) for day in range(7)}):
            raise ValueError("Se requieren siete pronósticos diarios distintos por serie y semana")
        group.sort(key=lambda item: item.target_date)
    return groups


def calibrate_weekly_totals(
    forecasts: list[Forecast], rows: list[ForecastExample], as_of_cutoff_at: datetime,
) -> ResidualCalibration:
    """Fit errors of complete weekly totals against sums of daily medians.

    ``rows`` must come from point-in-time preparation with labels visible at
    ``as_of_cutoff_at``. Only earlier, fully observed weeks are accepted.
    """
    if (not isinstance(as_of_cutoff_at, datetime) or as_of_cutoff_at.tzinfo is None
            or as_of_cutoff_at.utcoffset() != timedelta(0)):
        raise ValueError("as_of_cutoff_at debe ser un datetime UTC")
    groups = _group_forecasts(forecasts)
    if not groups or len(forecasts) != len(rows):
        raise ValueError("Se requieren semanas completas para calibrar")
    labels: dict[tuple[date, int, str, date], float] = {}
    for row in rows:
        key = (row.forecast_start_date, row.store_nbr, row.family, row.target_date)
        if (row.label_sales is None or key in labels
                or row.prediction_cutoff_at != _prediction_cutoff(row.forecast_start_date)
                or row.target_date >= as_of_cutoff_at.date()
                or row.prediction_cutoff_at >= as_of_cutoff_at):
            raise ValueError("Etiquetas semanales incompletas o posteriores al corte")
        value = float(row.label_sales)
        if not math.isfinite(value) or value < 0:
            raise ValueError("La demanda observada debe ser finita y no negativa")
        labels[key] = value
    expected = {
        (start, store, family, item.target_date)
        for (start, store, family), group in groups.items() for item in group
    }
    if set(labels) != expected:
        raise ValueError("Pronósticos y etiquetas semanales no coinciden")
    residuals = []
    for (start, store, family), group in sorted(groups.items()):
        actual = math.fsum(labels[start, store, family, item.target_date] for item in group)
        median_total = math.fsum(item.q50 for item in group)
        residuals.append(actual - median_total)
    return ResidualCalibration(tuple(sorted(residuals)))


def suggest_orders(
    forecasts: list[Forecast], weekly_calibration: ResidualCalibration, costs: CostPolicy,
    selected_forecast: SelectedForecast,
) -> list[WeeklyOrder]:
    """Choose the weekly cost quantile without netting stock or inbound orders."""
    groups = _group_forecasts(forecasts)
    if not weekly_calibration.residuals:
        raise ValueError("Se requiere calibración semanal")
    if (not all(math.isfinite(value) for value in weekly_calibration.residuals)
            or tuple(sorted(weekly_calibration.residuals)) != weekly_calibration.residuals):
        raise ValueError("La calibración semanal debe ser finita y ordenada")
    if (not math.isfinite(costs.underforecast_per_unit)
            or not math.isfinite(costs.overforecast_per_unit)
            or costs.underforecast_per_unit < 0 or costs.overforecast_per_unit < 0
            or not math.isfinite(costs.underforecast_per_unit + costs.overforecast_per_unit)
            or costs.underforecast_per_unit + costs.overforecast_per_unit <= 0):
        raise ValueError("Los costos son inválidos")
    orders = []
    for (start, store, family), group in sorted(groups.items()):
        _require_available(selected_forecast, group[0].prediction_cutoff_at)
        median_total = math.fsum(item.q50 for item in group)
        q10 = weekly_calibration.adjusted(median_total, 0.1)
        q50 = weekly_calibration.adjusted(median_total, 0.5)
        q90 = weekly_calibration.adjusted(median_total, 0.9)
        target = weekly_calibration.adjusted(median_total, costs.decision_quantile)
        if not all(math.isfinite(value) for value in (q10, q50, q90, target)):
            raise ValueError("El total semanal no es finito")
        orders.append(WeeklyOrder(
            start, store, family, group[0].prediction_cutoff_at,
            q10, q50, q90, target, math.ceil(target), costs,
            len(weekly_calibration.residuals),
            selected_forecast.name,
            selected_forecast.model_available_at,
            selected_forecast.daily_calibration_available_at,
            selected_forecast.model_state_sha256,
        ))
    return orders


def suggest_weekly_orders(
    inbox_dir: Path, start_date: date, cutoff_at: datetime,
    evaluation: EvaluationResult,
    calibration_first_start: date, calibration_last_start: date, costs_path: Path,
) -> list[WeeklyOrder]:
    """Read historical labels as of the decision cutoff and suggest gross coverage.

    ``evaluation`` must come from ``evaluate_models``, which derives availability
    from the temporal train/calibration/selection cutoffs.
    """
    _validate_dates_and_cutoff(calibration_first_start, calibration_last_start, cutoff_at)
    if not isinstance(start_date, date) or isinstance(start_date, datetime):
        raise ValueError("start_date debe ser una fecha sin hora")
    if cutoff_at != _prediction_cutoff(start_date):
        raise ValueError("cutoff_at debe ser el corte semanal de predicción")
    if calibration_last_start + timedelta(days=6) >= cutoff_at.date():
        raise ValueError("La calibración requiere semanas terminadas antes del corte")
    if not isinstance(evaluation, EvaluationResult):
        raise ValueError("Se requiere una evaluación con procedencia temporal acreditada")
    selected_forecast = evaluation.selected_forecast
    _require_available(selected_forecast, cutoff_at)
    if (not isinstance(evaluation.selection_start, date)
            or isinstance(evaluation.selection_start, datetime)
            or not isinstance(evaluation.test_start, date)
            or isinstance(evaluation.test_start, datetime)
            or evaluation.selection_start >= evaluation.test_start
            or selected_forecast.model_available_at != _prediction_cutoff(evaluation.test_start)
            or selected_forecast.daily_calibration_available_at
            != _prediction_cutoff(evaluation.selection_start)
            or selected_forecast.name != evaluation.selected_model
            or selected_forecast.model is not (
                evaluation.ml if evaluation.selected_model == "ml" else evaluation.baseline)
            or selected_forecast.daily_calibration is not (
                evaluation.ml_calibration if evaluation.selected_model == "ml"
                else evaluation.baseline_calibration)):
        raise ValueError("El modelo seleccionado y su procedencia temporal no coinciden")
    costs = load_costs(costs_path)
    forecasts = forecast_week(
        inbox_dir, start_date, cutoff_at,
        selected_forecast.model, selected_forecast.daily_calibration, costs,
    )
    if not forecasts:
        return []
    rows = prepare_forecast_examples(
        inbox_dir, calibration_first_start, calibration_last_start, cutoff_at,
        require_labels=True,
    )
    if not rows:
        raise ValueError("No hay semanas históricas completas para calibrar")
    for row in rows:
        _require_available(selected_forecast, row.prediction_cutoff_at)
    historical = predict_rows(
        selected_forecast.model, selected_forecast.daily_calibration, rows, costs,
    )
    weekly_calibration = calibrate_weekly_totals(historical, rows, cutoff_at)
    return suggest_orders(forecasts, weekly_calibration, costs, selected_forecast)
