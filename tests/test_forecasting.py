"""Weekly cutoffs, complete daily labels, and fair temporal model evaluation."""

import csv
import math
from dataclasses import fields, replace
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

import inventory.forecasting as forecasting_module
from inventory.data_preparation import Features, ForecastExample, prepare_forecast_examples
from inventory.forecasting import (
    RIDGE_PENALTIES, Forecast, MeanBaseline, Metrics, ResidualCalibration, RidgeForecaster,
    _raw_features, evaluate_models, forecast_week, load_costs, predict_rows,
    score_forecasts, score_points,
    should_replace,
)
from inventory.holidays_events import HOLIDAYS_COLUMNS
from inventory.oil import OIL_COLUMNS
from inventory.stores import STORES_COLUMNS
from inventory.train_schema import TRAIN_COLUMNS
from inventory.transactions import TRANSACTIONS_COLUMNS


COLUMNS = {
    "train": TRAIN_COLUMNS,
    "transactions": TRANSACTIONS_COLUMNS,
    "oil": OIL_COLUMNS,
    "holidays_events": HOLIDAYS_COLUMNS,
    "stores": STORES_COLUMNS,
}
COSTS = Path(__file__).resolve().parents[1] / "config/forecast_costs.json"


def _at(day: date) -> datetime:
    return datetime.combine(day, time(6), tzinfo=timezone.utc)


def _delivery(inbox: Path, source: str, observed: date, rows: list[list[str]],
              *, available: datetime | None = None, revision: int = 1) -> None:
    if available is None:
        available = _at(observed + timedelta(days=1))
    path = (inbox / f"source={source}" / f"observation_date={observed.isoformat()}"
            / f"available_at={available.strftime('%Y-%m-%dT%H%M%SZ')}"
            / f"revision={revision:04d}"
            / f"delivery_id={source}-{observed.isoformat()}-r{revision:04d}" / "data.csv")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.writer(output)
        writer.writerow(COLUMNS[source])
        writer.writerows(rows)


def _sale(day: date, amount: str, identifier: int = 1) -> list[str]:
    return [str(identifier), day.isoformat(), "1", "GROCERY", amount, "0"]


def test_weekly_rows_use_one_cutoff_and_never_replace_missing_sales_with_zero(tmp_path: Path) -> None:
    inbox = tmp_path / "inbox"
    observed = date(2017, 8, 8)
    start = date(2017, 8, 10)
    _delivery(inbox, "stores", date(2013, 1, 1),
              [["1", "Quito", "Pichincha", "D", "13"]],
              available=datetime(2013, 1, 1, tzinfo=timezone.utc))
    _delivery(inbox, "train", observed, [_sale(observed, "2")])
    _delivery(inbox, "transactions", observed, [[observed.isoformat(), "1", "20"]])
    _delivery(inbox, "oil", observed, [[observed.isoformat(), ""]])
    _delivery(inbox, "holidays_events", observed, [
        [observed.isoformat(), "Holiday", "Regional", "Pichincha", "A", "False"],
    ])
    # The previous day's sale is delivered only after this forecast cutoff.
    _delivery(inbox, "train", date(2017, 8, 9), [_sale(date(2017, 8, 9), "999")])
    cutoff = _at(date(2017, 8, 9))
    rows = prepare_forecast_examples(inbox, start, start, cutoff)
    assert len(rows) == 7
    assert [row.target_date for row in rows] == [start + timedelta(days=i) for i in range(7)]
    assert {row.prediction_cutoff_at for row in rows} == {cutoff}
    assert {row.features.last_sales for row in rows} == {Decimal(2)}
    assert {row.features.last_transactions for row in rows} == {20}
    assert {row.features.last_oil_price for row in rows} == {None}
    assert {row.features.known_events_7d for row in rows} == {1}
    assert all(row.label_sales is None for row in rows)
    assert not any(field.name.startswith("label") for field in fields(Features))
    assert prepare_forecast_examples(inbox, start, start, _at(date(2017, 8, 20))) == rows

    # Incomplete weeks are omitted from supervised training and evaluation.
    for offset in range(6):
        day = start + timedelta(days=offset)
        _delivery(inbox, "train", day, [_sale(day, str(10 + offset))])
    assert prepare_forecast_examples(inbox, start, start, _at(date(2017, 8, 20)),
                                     require_labels=True) == []
    final_day = start + timedelta(days=6)
    _delivery(inbox, "train", final_day, [_sale(final_day, "16")])
    labeled = prepare_forecast_examples(inbox, start, start, _at(date(2017, 8, 20)),
                                        require_labels=True)
    assert [row.label_sales for row in labeled] == [Decimal(i) for i in range(10, 17)]
    assert [row.features for row in labeled] == [row.features for row in rows]

    _delivery(inbox, "train", observed, [_sale(observed, "500")],
              available=_at(date(2017, 8, 21)), revision=2)
    _delivery(inbox, "train", final_day, [_sale(final_day, "60")],
              available=_at(date(2017, 8, 21)), revision=2)
    corrected = prepare_forecast_examples(inbox, start, start, _at(date(2017, 8, 21)),
                                          require_labels=True)
    assert corrected[-1].label_sales == Decimal(60)
    assert [row.features for row in corrected] == [row.features for row in labeled]
    assert prepare_forecast_examples(inbox, start, start, _at(date(2017, 8, 20)),
                                     require_labels=True) == labeled


def test_quantiles_costs_and_promotion_rule(tmp_path: Path) -> None:
    costs = load_costs(COSTS)
    assert costs.decision_quantile == pytest.approx(5 / 6)
    calibration = ResidualCalibration((-2.0, 0.0, 3.0))
    assert calibration.adjusted(5.0, 0.1) <= calibration.adjusted(5.0, 0.5)
    assert calibration.adjusted(5.0, 0.5) <= calibration.adjusted(5.0, 0.9)
    assert calibration.adjusted(0.0, 0.1) == 0.0
    incumbent = Metrics(2.0, 3.0, 10.0)
    assert should_replace(incumbent, Metrics(2.0, 3.0, 9.4), costs)
    assert not should_replace(incumbent, Metrics(2.1, 3.0, 8.0), costs)
    assert not should_replace(incumbent, Metrics(2.0, 3.0, 9.5), costs)
    invalid = tmp_path / "invalid.json"
    invalid.write_text('{"underforecast_per_unit": -1, "overforecast_per_unit": 1, '
                       '"min_relative_improvement": 0.05}', encoding="utf-8")
    with pytest.raises(ValueError):
        load_costs(invalid)
    with pytest.raises(ValueError):
        RidgeForecaster().predict_point(None)


def test_technical_and_asymmetric_business_metrics() -> None:
    costs = load_costs(COSTS)
    start = date(2017, 1, 2)
    cutoff = _at(start - timedelta(days=1))
    features = Features(
        day_of_week=0, month=1, city=None, state=None, store_type=None, cluster=None,
        last_sales=Decimal(4), sales_age_days=2, mean_sales_7d=Decimal(4),
        last_onpromotion=None, last_transactions=None, transactions_age_days=None,
        last_oil_price=None, oil_age_days=None, known_events_7d=0,
    )
    row = ForecastExample(start, start, 1, "A", cutoff, features, Decimal(10))
    point = score_points(MeanBaseline(), [row], costs)
    assert (point.mae, point.rmse, point.business_cost) == (6, 6, 30)
    forecast = Forecast(start, start, 1, "A", cutoff, 2, 4, 8, 8)
    scored = score_forecasts([forecast], [row], costs)
    assert (scored.mae, scored.rmse, scored.business_cost, scored.coverage_80) == (
        6, 6, 10, 0,
    )
    assert scored.pinball_loss == pytest.approx((0.8 + 3 + 1.8) / 3)


def test_ridge_log_volumes_missing_flags_and_sample_size_stability() -> None:
    start = date(2017, 1, 2)
    cutoff = _at(start - timedelta(days=1))
    rows = []
    for index, volume in enumerate((2, 5, 10, 30, 100, 500, 2000, 10000)):
        features = Features(
            day_of_week=index % 7, month=1, city=None, state=None, store_type=None,
            cluster=None, last_sales=Decimal(volume), sales_age_days=2,
            mean_sales_7d=Decimal(volume), last_onpromotion=index * 10,
            last_transactions=index * 500, transactions_age_days=2,
            last_oil_price=None, oil_age_days=None, known_events_7d=0,
        )
        rows.append(ForecastExample(
            start, start + timedelta(days=index % 7), 1, "A", cutoff,
            features, Decimal(volume),
        ))
    raw = _raw_features(rows[-1])
    assert raw[:3] == pytest.approx([
        math.log1p(10000), math.log1p(10000), math.log1p(70),
    ])
    assert raw[4] == pytest.approx(math.log1p(3500))
    assert raw[9:18] == [0, 0, 0, 0, 0, 0, 1, 1, 1]
    assert all(math.isfinite(value) for value in raw)
    model = RidgeForecaster(0.1).fit(rows)
    repeated = RidgeForecaster(0.1).fit(rows * 4)
    assert repeated.coefficients == pytest.approx(model.coefficients, rel=1e-9, abs=1e-9)
    assert all(0 <= model.predict_point(row) < 100000 for row in rows)
    missing = replace(rows[0], features=replace(
        rows[0].features, last_sales=None, mean_sales_7d=None,
        last_onpromotion=None, last_transactions=None,
    ))
    missing_raw = _raw_features(missing)
    assert missing_raw[0] == missing_raw[1] == missing_raw[2] == missing_raw[4] == 0
    assert [missing_raw[index] for index in (9, 10, 11, 13)] == [1, 1, 1, 1]


def test_each_week_uses_its_own_revision_cutoff(tmp_path: Path) -> None:
    inbox = tmp_path / "inbox"
    observed = date(2017, 1, 8)
    first = date(2017, 1, 10)
    second = first + timedelta(days=7)
    _delivery(inbox, "train", observed, [_sale(observed, "2")])
    _delivery(inbox, "train", date(2017, 1, 15), [_sale(date(2017, 1, 15), "4")])
    original = prepare_forecast_examples(inbox, first, second, _at(date(2017, 1, 16)))
    assert len(original) == 14
    assert {row.features.last_sales for row in original[:7]} == {Decimal(2)}
    assert {row.features.last_sales for row in original[7:]} == {Decimal(4)}
    revised_day = date(2017, 1, 15)
    _delivery(inbox, "train", revised_day, [_sale(revised_day, "40")],
              available=_at(date(2017, 1, 16)), revision=2)
    corrected = prepare_forecast_examples(inbox, first, second, _at(date(2017, 1, 16)))
    assert corrected[:7] == original[:7]
    assert {row.features.mean_sales_7d for row in corrected[7:]} == {Decimal(40)}
    assert {row.features.mean_sales_7d for row in original[7:]} == {Decimal(4)}


def _history(inbox: Path) -> tuple[date, date, date, date, date, datetime]:
    first = date(2017, 1, 1)
    train_start = date(2017, 1, 15)
    validation_start = date(2017, 1, 29)
    selection_start = date(2017, 2, 12)
    test_start = date(2017, 2, 26)
    last_test_start = test_start
    for offset in range((date(2017, 3, 4) - first).days + 1):
        day = first + timedelta(days=offset)
        _delivery(inbox, "train", day, [_sale(day, str(10 + offset % 7))])
    _delivery(inbox, "stores", date(2013, 1, 1),
              [["1", "Quito", "Pichincha", "D", "13"]],
              available=datetime(2013, 1, 1, tzinfo=timezone.utc))
    for day in (date(2017, 1, 10), date(2017, 2, 7)):
        _delivery(inbox, "transactions", day, [[day.isoformat(), "1", "20"]])
        _delivery(inbox, "oil", day, [[day.isoformat(), "50"]])
        _delivery(inbox, "holidays_events", day, [
            [day.isoformat(), "Holiday", "National", "Ecuador", "A", "False"],
        ])
    return (train_start, validation_start, selection_start, test_start,
            last_test_start, _at(date(2017, 3, 6)))


def test_temporal_evaluation_and_seven_day_forecasts_are_reproducible(tmp_path: Path) -> None:
    inbox = tmp_path / "inbox"
    args = _history(inbox)
    result = evaluate_models(inbox, *args, COSTS)
    assert (result.train_rows, result.calibration_rows, result.validation_rows,
            result.test_rows) == (7, 7, 7, 7)
    assert result.selected_model in ("baseline", "ml")
    assert result.selected_model == (
        "ml" if should_replace(result.validation_baseline, result.validation_ml, result.costs)
        else "baseline"
    )
    train = prepare_forecast_examples(
        inbox, args[0], args[1] - timedelta(days=14),
        _at(args[1] - timedelta(days=1)), require_labels=True,
    )
    calibration = prepare_forecast_examples(
        inbox, args[1], args[2] - timedelta(days=14),
        _at(args[2] - timedelta(days=1)), require_labels=True,
    )
    selection = prepare_forecast_examples(
        inbox, args[2], args[3] - timedelta(days=14),
        _at(args[3] - timedelta(days=1)), require_labels=True,
    )
    candidate_scores = {
        penalty: score_forecasts(
            predict_rows(model, ResidualCalibration.fit(model, calibration),
                         selection, result.costs),
            selection, result.costs,
        )
        for penalty in RIDGE_PENALTIES
        for model in (RidgeForecaster(penalty).fit(train),)
    }
    assert result.validation_baseline == score_forecasts(
        predict_rows(result.baseline, result.baseline_calibration, selection, result.costs),
        selection, result.costs,
    )
    assert result.validation_baseline.business_cost != score_points(
        result.baseline, selection, result.costs,
    ).business_cost
    assert result.validation_ml == candidate_scores[result.ridge_penalty]
    assert result.ridge_penalty == min(
        RIDGE_PENALTIES,
        key=lambda penalty: (
            candidate_scores[penalty].business_cost,
            candidate_scores[penalty].mae,
            penalty,
        ),
    )
    repeated = evaluate_models(inbox, *args, COSTS)
    assert (result.validation_baseline, result.validation_ml, result.test_baseline,
            result.test_ml, result.selected_model) == (
                repeated.validation_baseline, repeated.validation_ml,
                repeated.test_baseline, repeated.test_ml, repeated.selected_model,
            )
    assert result.ml.coefficients == repeated.ml.coefficients
    assert result.test_baseline.pinball_loss is not None
    assert result.test_ml.pinball_loss is not None
    assert 0 <= result.test_baseline.coverage_80 <= 1
    assert 0 <= result.test_ml.coverage_80 <= 1
    start = date(2017, 3, 12)
    cutoff = _at(start - timedelta(days=1))
    for model, calibration in (
        (result.baseline, result.baseline_calibration),
        (result.ml, result.ml_calibration),
    ):
        forecasts = forecast_week(inbox, start, cutoff, model, calibration, result.costs)
        assert len(forecasts) == 7
        assert all(forecast.q10 <= forecast.q50 <= forecast.q90 for forecast in forecasts)
        assert all(forecast.q10 >= 0 and forecast.business_target >= 0 for forecast in forecasts)
        assert {forecast.prediction_cutoff_at for forecast in forecasts} == {cutoff}
        assert forecasts == forecast_week(inbox, start, cutoff, model, calibration, result.costs)
    labeled_test = prepare_forecast_examples(inbox, args[3], args[4], args[5],
                                             require_labels=True)[0]
    different_label = replace(labeled_test, label_sales=Decimal("99999"))
    assert result.baseline.predict_point(labeled_test) == result.baseline.predict_point(different_label)
    assert result.ml.predict_point(labeled_test) == result.ml.predict_point(different_label)
    with pytest.raises(ValueError, match="corte semanal"):
        forecast_week(inbox, start, _at(start), result.baseline,
                      result.baseline_calibration, result.costs)
    assert not (tmp_path / "processed").exists()

    # A later revision of a train label cannot change the earlier training fit.
    corrected_train_day = date(2017, 1, 16)
    _delivery(inbox, "train", corrected_train_day, [_sale(corrected_train_day, "1000")],
              available=_at(date(2017, 3, 6)), revision=2)
    same = evaluate_models(inbox, *args, COSTS)
    assert same.ml.coefficients == result.ml.coefficients
    assert same.ridge_penalty == result.ridge_penalty
    assert (same.validation_baseline, same.validation_ml, same.test_baseline,
            same.test_ml, same.selected_model) == (
                result.validation_baseline, result.validation_ml, result.test_baseline,
                result.test_ml, result.selected_model,
            )

    # A test-label correction at the report cutoff changes test scores only.
    corrected_test_day = date(2017, 2, 26)
    _delivery(inbox, "train", corrected_test_day, [_sale(corrected_test_day, "1000")],
              available=_at(date(2017, 3, 6)), revision=2)
    after = evaluate_models(inbox, *args, COSTS)
    assert after.validation_baseline == result.validation_baseline
    assert after.validation_ml == result.validation_ml
    assert after.ridge_penalty == result.ridge_penalty
    assert after.ml.coefficients == result.ml.coefficients
    assert after.selected_model == result.selected_model
    assert after.test_baseline != result.test_baseline

    # Calibration labels arriving after selection begins cannot alter its forecasts.
    corrected_validation_day = date(2017, 1, 29)
    _delivery(inbox, "train", corrected_validation_day,
              [_sale(corrected_validation_day, "1000")],
              available=_at(date(2017, 2, 20)), revision=2)
    again = evaluate_models(inbox, *args, COSTS)
    assert again.baseline_calibration == after.baseline_calibration
    assert again.ml_calibration == after.ml_calibration
    assert again.test_baseline == after.test_baseline
    assert again.test_ml == after.test_ml

    # Selection labels revised after the test origin cannot change promotion.
    corrected_selection_day = date(2017, 2, 12)
    _delivery(inbox, "train", corrected_selection_day,
              [_sale(corrected_selection_day, "1000")],
              available=_at(date(2017, 3, 6)), revision=2)
    final = evaluate_models(inbox, *args, COSTS)
    assert final.validation_baseline == after.validation_baseline
    assert final.validation_ml == after.validation_ml
    assert final.selected_model == after.selected_model
    assert final.ridge_penalty == after.ridge_penalty


def test_temporal_split_rejects_missing_embargo_and_unlabeled_test(tmp_path: Path) -> None:
    inbox = tmp_path / "inbox"
    args = _history(inbox)
    with pytest.raises(ValueError, match="embargo"):
        evaluate_models(inbox, args[0], args[0] + timedelta(days=7),
                        args[2], args[3], args[4], args[5], COSTS)
    with pytest.raises(ValueError, match="embargo"):
        evaluate_models(inbox, args[0], args[1], args[1] + timedelta(days=7),
                        args[3], args[4], args[5], COSTS)
    with pytest.raises(ValueError, match="embargo"):
        evaluate_models(inbox, args[0], args[1], args[2], args[2] + timedelta(days=7),
                        args[4], args[5], COSTS)
    with pytest.raises(ValueError, match="partición"):
        evaluate_models(inbox, *args[:5], _at(date(2017, 3, 4)), COSTS)


def test_calibration_selection_cutoffs_and_test_is_read_after_decision(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    inbox = tmp_path / "inbox"
    args = _history(inbox)
    original_prepare = forecasting_module.prepare_forecast_examples
    original_replace = forecasting_module.should_replace
    calls = []
    decision_fixed = False

    def tracked_prepare(inbox_dir, first, last, cutoff, *, require_labels=False):
        if first == args[3]:
            assert decision_fixed
        calls.append((first, last, cutoff, require_labels))
        return original_prepare(inbox_dir, first, last, cutoff,
                                require_labels=require_labels)

    def tracked_replace(incumbent, challenger, costs):
        nonlocal decision_fixed
        decision_fixed = True
        return original_replace(incumbent, challenger, costs)

    monkeypatch.setattr(forecasting_module, "prepare_forecast_examples", tracked_prepare)
    monkeypatch.setattr(forecasting_module, "should_replace", tracked_replace)
    forecasting_module.evaluate_models(inbox, *args, COSTS)
    assert calls == [
        (args[0], args[1] - timedelta(days=14), _at(args[1] - timedelta(days=1)), True),
        (args[1], args[2] - timedelta(days=14), _at(args[2] - timedelta(days=1)), True),
        (args[2], args[3] - timedelta(days=14), _at(args[3] - timedelta(days=1)), True),
        (args[3], args[4], args[5], True),
    ]
