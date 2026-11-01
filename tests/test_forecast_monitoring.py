"""Immutable forecast publications and point-in-time operational scoring."""

import csv
import json
import math
from dataclasses import replace
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

import pytest

import inventory.forecast_monitoring as monitoring_module
import inventory.published_forecasts as published_module
from inventory.forecast_monitoring import evaluate_publication, read_report
from inventory.forecasting import (
    CostPolicy, EvaluationResult, MeanBaseline, Metrics, ResidualCalibration,
    RidgeForecaster, SelectedForecast, model_state_sha256,
)
from inventory.published_forecasts import publish_forecasts, read_publication
from inventory.train_schema import TRAIN_COLUMNS


def _at(day: date, hour: int = 6) -> datetime:
    return datetime.combine(day, time(hour), tzinfo=timezone.utc)


def _sale(inbox: Path, day: date, amount: str | None, *, store: int = 1,
          revision: int = 1, available_at: datetime | None = None) -> None:
    available_at = available_at or _at(day + timedelta(days=1))
    path = (inbox / "source=train" / f"observation_date={day.isoformat()}"
            / f"available_at={available_at.strftime('%Y-%m-%dT%H%M%SZ')}"
            / f"revision={revision:04d}"
            / f"delivery_id=train-{day.isoformat()}-r{revision:04d}" / "data.csv")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.writer(output)
        writer.writerow(TRAIN_COLUMNS)
        if amount is not None:
            writer.writerow([store, day.isoformat(), store, "GROCERY", amount, 0])


@pytest.fixture
def context(tmp_path: Path, monkeypatch):
    inbox = tmp_path / "inbox"
    published = tmp_path / "published"
    start = date(2017, 8, 10)
    cutoff = _at(start - timedelta(days=1))
    _sale(inbox, start - timedelta(days=2), "10")
    costs_path = tmp_path / "costs.json"
    costs_path.write_text(json.dumps({
        "underforecast_per_unit": 5,
        "overforecast_per_unit": 1,
        "min_relative_improvement": 0.05,
    }), encoding="utf-8")
    baseline = MeanBaseline()
    calibration = ResidualCalibration((0.0,))
    selection_start = start - timedelta(days=28)
    test_start = start - timedelta(days=14)
    selected = SelectedForecast(
        "baseline", baseline, calibration,
        _at(test_start - timedelta(days=1)),
        _at(selection_start - timedelta(days=1)),
        model_state_sha256(baseline),
    )
    empty = Metrics(0.0, 0.0, 0.0)
    evaluation = EvaluationResult(
        baseline, RidgeForecaster(), calibration, calibration,
        CostPolicy(5.0, 1.0, 0.05), empty, empty, empty, empty,
        "baseline", selected, selection_start, test_start, 0.1, 1, 1, 1, 1,
    )
    monkeypatch.setattr(published_module, "_utc_now", lambda: cutoff + timedelta(hours=1))
    monkeypatch.setattr(monitoring_module, "_utc_now", lambda: _at(start + timedelta(days=30)))
    return inbox, published, start, cutoff, evaluation, costs_path


def _publish(context, publication_id="weekly-20170810", *, kind="real"):
    inbox, published, start, _, evaluation, costs_path = context
    return publish_forecasts(inbox, start, evaluation, costs_path, publication_id,
                             published_dir=published, kind=kind)


def test_publication_identity_lineage_and_idempotent_retry(context, monkeypatch) -> None:
    inbox, published, start, cutoff, evaluation, costs_path = context
    original = _publish(context)
    path = published / "forecasts" / "weekly-20170810.json"
    raw = path.read_bytes()
    assert original == read_publication(original.publication_id, published_dir=published)
    assert original.kind == "real"
    assert original.recorded_at == original.published_at == cutoff + timedelta(hours=1)
    assert original.prediction_cutoff_at == cutoff
    assert original.model_name == "baseline"
    assert original.model_state_sha256 == evaluation.selected_forecast.model_state_sha256
    assert original.daily_calibration_sha256
    assert original.costs_sha256 and original.costs_path == costs_path.as_posix()
    assert len(original.input_deliveries) == 1
    assert len(original.forecasts) == 7
    assert {row.target_date for row in original.forecasts} == {
        start + timedelta(days=offset) for offset in range(7)
    }
    assert {row.prediction_cutoff_at for row in original.forecasts} == {cutoff}
    assert {row.q50 for row in original.forecasts} == {10.0}
    monkeypatch.setattr(published_module, "_utc_now", lambda: _at(start + timedelta(days=10)))
    assert _publish(context) == original
    assert path.read_bytes() == raw
    assert len(list((published / "forecasts").glob("*.json"))) == 1
    with pytest.raises(ValueError, match="Conflicto"):
        _publish(context, kind="historical_simulation")
    costs_path.write_text(json.dumps({
        "underforecast_per_unit": 1,
        "overforecast_per_unit": 5,
        "min_relative_improvement": 0.05,
    }), encoding="utf-8")
    with pytest.raises(ValueError, match="Conflicto"):
        _publish(context)
    assert path.read_bytes() == raw


def test_historical_simulation_is_explicit_and_real_clock_is_enforced(context, monkeypatch) -> None:
    _, published, start, cutoff, _, _ = context
    monkeypatch.setattr(published_module, "_utc_now", lambda: _at(start + timedelta(days=2)))
    with pytest.raises(ValueError, match="publicación real"):
        _publish(context, "late-real")
    simulated = _publish(context, "historical-20170810", kind="historical_simulation")
    assert simulated.kind == "historical_simulation"
    assert simulated.published_at is None
    assert simulated.recorded_at == _at(start + timedelta(days=2))
    assert simulated.prediction_cutoff_at == cutoff
    assert read_publication(simulated.publication_id, published_dir=published) == simulated
    assert not (published / "forecasts" / "late-real.json").exists()


def test_publication_accepts_cutoff_boundary_and_rejects_future_cutoff(context, monkeypatch) -> None:
    _, _, start, cutoff, _, _ = context
    monkeypatch.setattr(published_module, "_utc_now", lambda: cutoff - timedelta(seconds=1))
    with pytest.raises(ValueError, match="aún no ha ocurrido"):
        _publish(context, "before-cutoff")
    monkeypatch.setattr(published_module, "_utc_now", lambda: cutoff)
    exactly_at_cutoff = _publish(context, "at-cutoff")
    assert exactly_at_cutoff.published_at == cutoff
    assert exactly_at_cutoff.forecast_start_date == start


def test_two_series_remain_separate_in_publication_and_monitoring(context) -> None:
    inbox, published, start, _, _, _ = context
    original_batch = next(inbox.rglob("data.csv"))
    with original_batch.open("a", newline="", encoding="utf-8") as output:
        csv.writer(output).writerow([2, (start - timedelta(days=2)).isoformat(),
                                     2, "AUTOMOTIVE", 30, 0])
    publication = _publish(context, "two-series")
    assert len(publication.forecasts) == 14
    assert {(row.store_nbr, row.family, row.q50) for row in publication.forecasts} == {
        (1, "GROCERY", 10.0), (2, "AUTOMOTIVE", 30.0),
    }
    _sale(inbox, start, "12", available_at=_at(start + timedelta(days=2)))
    report = evaluate_publication(inbox, publication.publication_id,
                                  _at(start + timedelta(days=2)), published_dir=published)
    assert report.metrics.evaluated_pairs == 1
    assert report.metrics.pending_pairs == 13
    assert {(row.store_nbr, row.family) for row in report.rows if row.status == "observed"} == {
        (1, "GROCERY"),
    }


def test_invalid_identity_duplicates_and_temporal_provenance_are_rejected(context) -> None:
    inbox, published, start, _, evaluation, costs_path = context
    for bad in ("../escape", "space id", ""):
        with pytest.raises(ValueError, match="publication_id"):
            publish_forecasts(inbox, start, evaluation, costs_path, bad,
                              published_dir=published)
    bad_model = replace(
        evaluation,
        selected_forecast=replace(evaluation.selected_forecast,
                                  model_available_at=_at(start + timedelta(days=1))),
    )
    with pytest.raises(ValueError, match="no estaban disponibles"):
        publish_forecasts(inbox, start, bad_model, costs_path, "future-model",
                          published_dir=published)
    bad_calibration = replace(
        evaluation,
        selected_forecast=replace(evaluation.selected_forecast,
                                  daily_calibration_available_at=_at(start + timedelta(days=1))),
    )
    with pytest.raises(ValueError, match="no estaban disponibles"):
        publish_forecasts(inbox, start, bad_calibration, costs_path, "future-calibration",
                          published_dir=published)
    with pytest.raises(ValueError, match="No se puede acreditar"):
        publish_forecasts(inbox, start,
                          replace(evaluation, selected_forecast=replace(
                              evaluation.selected_forecast, model_available_at=None)),
                          costs_path, "missing-provenance", published_dir=published)
    original = _publish(context)
    with pytest.raises(ValueError, match="siete pronósticos"):
        published_module._validate_publication(replace(
            original, forecasts=original.forecasts + (original.forecasts[0],)))
    with pytest.raises(ValueError, match="un único corte"):
        published_module._validate_publication(replace(
            original, forecasts=tuple(replace(row, forecast_start_date=start + timedelta(days=7))
                                      if index == 0 else row
                                      for index, row in enumerate(original.forecasts))))


def test_pending_late_sales_corrections_metrics_and_report_idempotence(context) -> None:
    inbox, published, start, _, _, _ = context
    original = _publish(context)
    first_at = _at(start)
    pending = evaluate_publication(inbox, original.publication_id, first_at,
                                   published_dir=published)
    assert pending.metrics.evaluated_pairs == 0
    assert pending.metrics.pending_pairs == 7
    assert (pending.metrics.mae, pending.metrics.rmse, pending.metrics.bias,
            pending.metrics.coverage_80) == (None, None, None, None)
    assert all(row.status == "pending" and row.actual_sales is None for row in pending.rows)
    assert pending == evaluate_publication(inbox, original.publication_id, first_at,
                                           published_dir=published)

    # The first day's label is delivered later than the usual D+1 simulation.
    first = start
    late_at = _at(start + timedelta(days=3))
    _sale(inbox, first, "12", available_at=late_at)
    before_late = evaluate_publication(inbox, original.publication_id,
                                       late_at - timedelta(seconds=1), published_dir=published)
    assert before_late.metrics.evaluated_pairs == 0
    late = evaluate_publication(inbox, original.publication_id, late_at,
                                published_dir=published)
    assert late.metrics.evaluated_pairs == 1
    assert late.metrics.pending_pairs == 6
    assert (late.metrics.mae, late.metrics.rmse, late.metrics.bias,
            late.metrics.coverage_80) == (2.0, 2.0, 2.0, 0.0)
    assert late.rows[0].actual_sales == 12.0
    assert late.rows[0].error == 2.0
    assert len(late.sales_deliveries) == 1
    assert late == read_report(original.publication_id, late_at, published_dir=published)

    second = start + timedelta(days=1)
    second_at = late_at + timedelta(hours=1)
    _sale(inbox, second, "10", available_at=second_at)
    combined = evaluate_publication(inbox, original.publication_id, second_at,
                                    published_dir=published)
    assert combined.metrics.evaluated_pairs == 2
    assert combined.metrics.pending_pairs == 5
    assert combined.metrics.mae == 1.0
    assert combined.metrics.rmse == math.sqrt(2)
    assert combined.metrics.bias == 1.0
    assert combined.metrics.coverage_80 == 0.5

    corrected_at = _at(start + timedelta(days=4))
    _sale(inbox, first, "8", revision=2, available_at=corrected_at)
    corrected = evaluate_publication(inbox, original.publication_id, corrected_at,
                                     published_dir=published)
    assert corrected.rows[0].actual_sales == 8.0
    assert corrected.rows[0].error == -2.0
    assert corrected.metrics.bias == -1.0
    assert read_report(original.publication_id, late_at, published_dir=published) == late
    assert read_publication(original.publication_id, published_dir=published) == original
    assert evaluate_publication(inbox, original.publication_id, late_at,
                                published_dir=published) == late

    removed_at = _at(start + timedelta(days=5))
    _sale(inbox, first, None, revision=3, available_at=removed_at)
    removed = evaluate_publication(inbox, original.publication_id, removed_at,
                                   published_dir=published)
    assert removed.rows[0].status == "pending"
    assert removed.metrics.evaluated_pairs == 1
    assert removed.metrics.pending_pairs == 6


def test_observed_zero_is_scored_while_unavailable_sale_stays_pending(context) -> None:
    inbox, published, start, _, _, _ = context
    publication = _publish(context)
    available_at = _at(start + timedelta(days=1))
    _sale(inbox, start, "0", available_at=available_at)

    before = evaluate_publication(inbox, publication.publication_id,
                                  available_at - timedelta(seconds=1), published_dir=published)
    assert before.metrics.evaluated_pairs == 0
    assert before.metrics.pending_pairs == 7

    report = evaluate_publication(inbox, publication.publication_id,
                                  available_at, published_dir=published)
    assert report.rows[0].status == "observed"
    assert report.rows[0].actual_sales == 0.0
    assert report.rows[0].error == -10.0
    assert report.rows[1].status == "pending"
    assert report.rows[1].actual_sales is None
    assert report.rows[1].error is None
    assert report.metrics.evaluated_pairs == 1
    assert report.metrics.pending_pairs == 6
    assert (report.metrics.mae, report.metrics.rmse, report.metrics.bias,
            report.metrics.coverage_80) == (10.0, 10.0, -10.0, 0.0)


def test_backdated_change_conflicts_with_existing_as_of_report(context) -> None:
    inbox, published, start, _, _, _ = context
    original = _publish(context)
    evaluation_at = _at(start + timedelta(days=3))
    initial = evaluate_publication(inbox, original.publication_id, evaluation_at,
                                   published_dir=published)
    path = published / "monitoring" / original.publication_id / f"{evaluation_at:%Y-%m-%dT%H%M%S.%fZ}.json"
    raw = path.read_bytes()
    _sale(inbox, start, "20", available_at=evaluation_at)
    with pytest.raises(ValueError, match="Conflicto"):
        evaluate_publication(inbox, original.publication_id, evaluation_at,
                             published_dir=published)
    assert read_report(original.publication_id, evaluation_at,
                       published_dir=published) == initial
    assert path.read_bytes() == raw


def test_invalid_evaluation_dates_and_artifact_tampering(context) -> None:
    inbox, published, start, cutoff, _, _ = context
    original = _publish(context)
    with pytest.raises(ValueError, match="UTC"):
        evaluate_publication(inbox, original.publication_id,
                             datetime.combine(start, time(6)), published_dir=published)
    with pytest.raises(ValueError, match="fecha de evaluación"):
        evaluate_publication(inbox, original.publication_id, cutoff,
                             published_dir=published)
    with pytest.raises(ValueError, match="fecha de evaluación"):
        evaluate_publication(inbox, original.publication_id,
                             _at(start + timedelta(days=31)), published_dir=published)
    path = published / "forecasts" / f"{original.publication_id}.json"
    raw = path.read_bytes()
    path.write_bytes(raw.replace(b'"q50":10.0', b'"q50":11.0'))
    with pytest.raises(ValueError, match="Huella"):
        read_publication(original.publication_id, published_dir=published)
