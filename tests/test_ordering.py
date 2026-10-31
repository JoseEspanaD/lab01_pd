"""Weekly total calibration, cost decisions, and historical cutoff behavior."""

import csv
from dataclasses import replace
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from inventory.data_preparation import Features, ForecastExample
from inventory.forecasting import (
    CostPolicy, Forecast, MeanBaseline, ResidualCalibration, RidgeForecaster, SelectedForecast,
    evaluate_models, model_state_sha256,
)
from inventory.ordering import (
    _require_available, calibrate_weekly_totals, suggest_orders, suggest_weekly_orders,
)
from inventory.train_schema import TRAIN_COLUMNS


def _cutoff(start: date) -> datetime:
    return datetime.combine(start - timedelta(days=1), time(6), tzinfo=timezone.utc)


def _week(start: date, *, store: int = 1, family: str = "A",
          median: float = 10) -> list[Forecast]:
    return [Forecast(
        start, start + timedelta(days=day), store, family, _cutoff(start),
        0, median, median + 100, median + 50,
    ) for day in range(7)]


def _selected(cutoff: datetime) -> SelectedForecast:
    model = MeanBaseline()
    return SelectedForecast(
        "baseline", model, ResidualCalibration((0.0,)), cutoff, cutoff,
        model_state_sha256(model),
    )


def _rows(forecasts: list[Forecast], actual_per_day: str) -> list[ForecastExample]:
    features = Features(
        day_of_week=0, month=1, city=None, state=None, store_type=None,
        cluster=None, last_sales=None, sales_age_days=None, mean_sales_7d=None,
        last_onpromotion=None, last_transactions=None, transactions_age_days=None,
        last_oil_price=None, oil_age_days=None, known_events_7d=0,
    )
    return [ForecastExample(
        item.forecast_start_date, item.target_date, item.store_nbr, item.family,
        item.prediction_cutoff_at, features, Decimal(actual_per_day),
    ) for item in forecasts]


def _deliver_sale(inbox: Path, day: date, sales: str, *, revision: int = 1,
                  available_at: datetime | None = None) -> None:
    available_at = available_at or datetime.combine(
        day + timedelta(days=1), time(6), tzinfo=timezone.utc,
    )
    path = (inbox / "source=train" / f"observation_date={day.isoformat()}"
            / f"available_at={available_at.strftime('%Y-%m-%dT%H%M%SZ')}"
            / f"revision={revision:04d}"
            / f"delivery_id=train-{day.isoformat()}-r{revision:04d}" / "data.csv")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.writer(output)
        writer.writerow(TRAIN_COLUMNS)
        writer.writerow([1, day.isoformat(), 1, "A", sales, 0])


def test_weekly_quantile_is_calibrated_from_complete_total_errors_and_costs() -> None:
    start = date(2017, 2, 5)
    history = (_week(start - timedelta(days=21))
               + _week(start - timedelta(days=14)))
    rows = (_rows(history[:7], "9") + _rows(history[7:], "12"))
    calibration = calibrate_weekly_totals(history[::-1], rows, _cutoff(start))
    assert calibration.residuals == (-7.0, 14.0)
    future = _week(start, store=2)
    high_shortage = CostPolicy(5, 1, 0.05)
    high_waste = CostPolicy(1, 5, 0.05)
    selected = _selected(_cutoff(start))
    expensive_shortage = suggest_orders(future[::-1], calibration, high_shortage, selected)[0]
    expensive_waste = suggest_orders(future, calibration, high_waste, selected)[0]
    assert expensive_shortage.decision_demand == pytest.approx(80.5)
    assert expensive_shortage.suggested_units == 81
    assert expensive_waste.decision_demand == pytest.approx(66.5)
    assert expensive_waste.suggested_units == 67
    assert expensive_shortage.demand_q10 == expensive_waste.demand_q10
    assert expensive_shortage.demand_q90 == expensive_waste.demand_q90
    assert expensive_shortage.demand_q90 != sum(item.q90 for item in future)
    assert expensive_shortage.calibration_weeks == 2


def test_zero_floor_ordering_and_stable_sorted_groups() -> None:
    start = date(2017, 2, 5)
    forecasts = _week(start, store=2, median=0) + _week(start, store=1, median=0)
    calibration = ResidualCalibration((-10.0, 0.0))
    selected = _selected(_cutoff(start))
    orders = suggest_orders(forecasts[::-1], calibration, CostPolicy(1, 5, 0.05), selected)
    assert [order.store_nbr for order in orders] == [1, 2]
    assert all(order.suggested_units == 0 for order in orders)
    assert orders == suggest_orders(forecasts, calibration, CostPolicy(1, 5, 0.05), selected)


def test_invalid_or_incomplete_inputs_fail_explicitly() -> None:
    start = date(2017, 2, 5)
    week = _week(start)
    calibration = ResidualCalibration((0.0,))
    costs = CostPolicy(5, 1, 0.05)
    selected = _selected(_cutoff(start))
    for invalid in (week[:6], week + week[:1],
                    [replace(week[0], q50=float("nan")), *week[1:]],
                    [replace(week[0], prediction_cutoff_at=_cutoff(start) + timedelta(days=1)),
                     *week[1:]]):
        with pytest.raises(ValueError):
            suggest_orders(invalid, calibration, costs, selected)
    with pytest.raises(ValueError, match="calibración"):
        suggest_orders(week, ResidualCalibration(()), costs, selected)
    with pytest.raises(ValueError, match="costos"):
        suggest_orders(week, calibration, CostPolicy(0, 0, 0.05), selected)
    historical = _week(start - timedelta(days=14))
    rows = _rows(historical, "10")
    with pytest.raises(ValueError):
        calibrate_weekly_totals(historical, rows[:6], _cutoff(start))
    with pytest.raises(ValueError):
        calibrate_weekly_totals(historical, [replace(rows[0], label_sales=None), *rows[1:]],
                                _cutoff(start))
    with pytest.raises(ValueError):
        calibrate_weekly_totals(historical, rows, _cutoff(start - timedelta(days=14)))
    with pytest.raises(ValueError, match="UTC"):
        calibrate_weekly_totals(historical, rows, datetime(2017, 2, 4, 6))


def test_provenance_rejects_future_or_missing_dates_and_accepts_equality() -> None:
    cutoff = _cutoff(date(2017, 1, 15))
    selected = _selected(cutoff)
    _require_available(selected, cutoff)
    with pytest.raises(ValueError, match="no estaban disponibles"):
        _require_available(replace(selected, model_available_at=cutoff + timedelta(seconds=1)),
                           cutoff)
    with pytest.raises(ValueError, match="no estaban disponibles"):
        _require_available(replace(selected,
                                   daily_calibration_available_at=cutoff + timedelta(seconds=1)),
                           cutoff)
    with pytest.raises(ValueError, match="No se puede acreditar"):
        _require_available(replace(selected, model_available_at=None), cutoff)
    model = RidgeForecaster().fit(_rows(_week(date(2016, 12, 18)), "10"))
    fitted = SelectedForecast(
        "ml", model, ResidualCalibration((0.0,)), cutoff, cutoff,
        model_state_sha256(model),
    )
    _require_available(fitted, cutoff)
    model.coefficients[0] += 1
    with pytest.raises(ValueError, match="cambió"):
        _require_available(fitted, cutoff)


def test_point_in_time_integration_cost_changes_and_late_corrections(tmp_path: Path) -> None:
    inbox = tmp_path / "inbox"
    first = date(2016, 12, 1)
    start = date(2017, 2, 12)
    for offset in range((start - first).days):
        day = first + timedelta(days=offset)
        if day != date(2017, 1, 25):  # Incomplete week is excluded, never filled with zero.
            sales = "8" if date(2017, 1, 8) <= day <= date(2017, 1, 14) else (
                "12" if date(2017, 1, 15) <= day <= date(2017, 1, 21) else "10"
            )
            _deliver_sale(inbox, day, sales)
    original = tmp_path / "costs.json"
    original.write_text('{"underforecast_per_unit": 5, "overforecast_per_unit": 1, '
                        '"min_relative_improvement": 0.05}', encoding="utf-8")
    cutoff = _cutoff(start)
    result = evaluate_models(
        inbox, date(2016, 12, 4), date(2016, 12, 18),
        date(2017, 1, 1), date(2017, 1, 15), date(2017, 1, 15),
        datetime(2017, 1, 23, 6, tzinfo=timezone.utc), original,
    )
    assert result.selected_model == "baseline"
    assert result.selected_forecast.model_available_at == _cutoff(date(2017, 1, 15))
    assert result.selected_forecast.daily_calibration_available_at == _cutoff(date(2017, 1, 1))
    args = (inbox, start, cutoff, result, date(2017, 1, 15), date(2017, 1, 29))
    orders = suggest_weekly_orders(*args, original)
    assert len(orders) == 1
    assert orders[0].calibration_weeks == 2
    assert orders[0].model_available_at == _cutoff(date(2017, 1, 15))
    assert orders[0].daily_calibration_available_at == _cutoff(date(2017, 1, 1))
    assert orders[0].model_state_sha256 == result.selected_forecast.model_state_sha256
    assert orders == suggest_weekly_orders(*args, original)
    assert not (tmp_path / "processed").exists()

    _deliver_sale(inbox, date(2017, 1, 18), "1000", revision=2,
                  available_at=cutoff + timedelta(days=2))
    assert suggest_weekly_orders(*args, original) == orders
    original.write_text('{"underforecast_per_unit": 1, "overforecast_per_unit": 5, '
                        '"min_relative_improvement": 0.05}', encoding="utf-8")
    changed = suggest_weekly_orders(*args, original)
    assert changed[0].demand_q50 == orders[0].demand_q50
    assert changed[0].costs != orders[0].costs
    assert changed[0].suggested_units < orders[0].suggested_units
    with pytest.raises(ValueError, match="No hay semanas"):
        suggest_weekly_orders(inbox, start, cutoff, result, date(2017, 1, 22),
                              date(2017, 1, 22), original)
    with pytest.raises(ValueError, match="terminadas"):
        suggest_weekly_orders(inbox, start, cutoff, result, date(2017, 2, 5),
                              date(2017, 2, 5), original)
    with pytest.raises(ValueError, match="corte semanal"):
        suggest_weekly_orders(inbox, start, cutoff + timedelta(days=1), result,
                              date(2017, 1, 15), date(2017, 1, 29), original)

    later_model = replace(
        result,
        test_start=date(2017, 1, 22),
        selected_forecast=replace(
            result.selected_forecast,
            model_available_at=_cutoff(date(2017, 1, 22)),
        ),
    )
    with pytest.raises(ValueError, match="no estaban disponibles"):
        suggest_weekly_orders(inbox, start, cutoff, later_model,
                              date(2017, 1, 15), date(2017, 1, 29), original)
    with pytest.raises(ValueError, match="procedencia"):
        suggest_weekly_orders(inbox, start, cutoff, None,
                              date(2017, 1, 15), date(2017, 1, 29), original)
