"""Manual weekly forecast depends on prepared as-of data and immutable outputs."""

from datetime import date, datetime, time, timedelta, timezone
import json
import math
from pathlib import Path

import pytest

from inventory.dated_source import publish_delivery
from inventory.data_preparation import prepare_forecast_examples
from inventory.operations.ingestion import validate_and_record
from inventory.operations.preparation import prepare_and_record
from inventory.operations.weekly_forecast import publish_weekly_plan
from inventory.published_forecasts import _read_envelope, read_publication
from inventory.train_schema import TRAIN_COLUMNS


START = date(2017, 8, 1)
CUTOFF = "2017-07-31T06:00:00Z"


def _at(day: date) -> datetime:
    return datetime.combine(day, time(6), tzinfo=timezone.utc)


def _config() -> dict:
    return {
        "forecast_start_date": START.isoformat(), "cutoff_at": CUTOFF,
        "train_start": "2017-06-04", "validation_start": "2017-06-18",
        "selection_start": "2017-07-02", "test_start": "2017-07-16",
        "last_test_start": "2017-07-16", "report_cutoff_at": "2017-07-24T06:00:00Z",
        "calibration_first_start": "2017-07-16",
        "calibration_last_start": "2017-07-16", "kind": "historical_simulation",
    }


@pytest.fixture
def context(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    inbox, processed, published = (tmp_path / name for name in
                                   ("inbox", "processed", "published"))
    first = date(2017, 5, 27)
    for offset in range(65):
        day = first + timedelta(days=offset)
        publish_delivery(
            "train", day, _at(day + timedelta(days=1)), 1, TRAIN_COLUMNS,
            [[str(offset + 1), day.isoformat(), "1", "A", str(10 + offset % 7), "0"]],
            inbox,
        )
    costs = tmp_path / "costs.json"
    costs.write_text(json.dumps({"underforecast_per_unit": 5,
                                 "overforecast_per_unit": 1,
                                 "min_relative_improvement": 0.05}), encoding="utf-8")
    return inbox, processed, published, costs


def _prepare(context) -> None:
    inbox, processed, _, _ = context
    validate_and_record(inbox, CUTOFF, processed)
    prepare_and_record(inbox, processed, "2017-06-04", "2017-07-31", CUTOFF)


def test_requires_matching_ingestion_and_preparation(context) -> None:
    inbox, processed, published, costs = context
    with pytest.raises(FileNotFoundError, match="ingesta y preparación"):
        publish_weekly_plan(inbox, processed, published, costs, _config())
    validate_and_record(inbox, CUTOFF, processed)
    with pytest.raises(FileNotFoundError, match="ingesta y preparación"):
        publish_weekly_plan(inbox, processed, published, costs, _config())
    assert not list(published.rglob("*.json"))


def test_idempotent_publication_and_weekly_order_consistency(context) -> None:
    inbox, processed, published, costs = context
    _prepare(context)
    first = publish_weekly_plan(inbox, processed, published, costs, _config())
    assert first["forecast_rows"] == 7 and first["order_count"] == 1
    forecast = read_publication(first["publication_id"], published_dir=published)
    order_path = Path(first["orders_path"])
    saved = (order_path.read_bytes(), order_path.stat().st_mtime_ns)
    payload = _read_envelope(order_path)
    order = payload["orders"][0]
    assert forecast.kind == "historical_simulation" and forecast.published_at is None
    assert {row.target_date for row in forecast.forecasts} == {
        START + timedelta(days=offset) for offset in range(7)}
    assert order["selected_model"] == forecast.model_name
    assert order["model_state_sha256"] == forecast.model_state_sha256
    assert order["costs"] == {"underforecast_per_unit": 5.0,
                              "overforecast_per_unit": 1.0,
                              "min_relative_improvement": 0.05}
    assert order["suggested_units"] == math.ceil(order["decision_demand"])
    assert order["demand_q10"] <= order["demand_q50"] <= order["demand_q90"]
    assert order["calibration_weeks"] == 1
    assert publish_weekly_plan(inbox, processed, published, costs, _config()) == first
    assert (order_path.read_bytes(), order_path.stat().st_mtime_ns) == saved


def test_cutoff_future_inputs_and_conflict(context) -> None:
    inbox, processed, published, costs = context
    _prepare(context)
    original = publish_weekly_plan(inbox, processed, published, costs, _config())
    before = Path(original["orders_path"]).read_bytes()
    future = date(2017, 8, 1)
    publish_delivery("train", future, _at(future + timedelta(days=1)), 1,
                     TRAIN_COLUMNS, [["100", future.isoformat(), "1", "A", "999", "0"]],
                     inbox)
    assert publish_weekly_plan(inbox, processed, published, costs, _config()) == original
    assert Path(original["orders_path"]).read_bytes() == before
    for changed in ({**_config(), "cutoff_at": "2017-08-01T06:00:00Z"},
                    {**_config(), "report_cutoff_at": "2017-08-01T06:00:00Z"}):
        with pytest.raises(ValueError, match="corte|cutoff_at"):
            publish_weekly_plan(inbox, processed, published, costs, changed)

    corrected_day = date(2017, 7, 29)
    publish_delivery("train", corrected_day, _at(date(2017, 7, 31)), 2,
                     TRAIN_COLUMNS,
                     [["999", corrected_day.isoformat(), "1", "A", "100", "0"]], inbox)
    with pytest.raises(ValueError, match="Conflicto"):
        publish_weekly_plan(inbox, processed, published, costs, _config())
    assert Path(original["orders_path"]).read_bytes() == before


def test_visible_correction_changes_forecast_and_cost_conflict(tmp_path: Path) -> None:
    results = []
    for variant in ("original", "corrected"):
        root = tmp_path / variant
        root.mkdir()
        inbox, processed, published = (root / name for name in
                                       ("inbox", "processed", "published"))
        costs = root / "costs.json"
        costs.write_text('{"underforecast_per_unit":5,"overforecast_per_unit":1,'
                         '"min_relative_improvement":0.05}', encoding="utf-8")
        first = date(2017, 5, 27)
        for offset in range(65):
            day = first + timedelta(days=offset)
            publish_delivery("train", day, _at(day + timedelta(days=1)), 1,
                             TRAIN_COLUMNS,
                             [[str(offset + 1), day.isoformat(), "1", "A",
                               str(10 + offset % 7), "0"]], inbox)
        if variant == "corrected":
            day = date(2017, 7, 29)
            publish_delivery("train", day, _at(date(2017, 7, 31)), 2,
                             TRAIN_COLUMNS,
                             [["999", day.isoformat(), "1", "A", "100", "0"]], inbox)
        validate_and_record(inbox, CUTOFF, processed)
        prepare_and_record(inbox, processed, "2017-06-04", "2017-07-31", CUTOFF)
        result = publish_weekly_plan(inbox, processed, published, costs, _config())
        forecast = read_publication(result["publication_id"], published_dir=published)
        input_row = prepare_forecast_examples(
            inbox, START, START, _at(date(2017, 7, 31)))[0]
        results.append((input_row.features.mean_sales_7d,
                        _read_envelope(Path(result["orders_path"]))["preparation_sha256"],
                        forecast.input_deliveries))
        if variant == "original":
            costs.write_text('{"underforecast_per_unit":1,"overforecast_per_unit":5,'
                             '"min_relative_improvement":0.05}', encoding="utf-8")
            with pytest.raises(ValueError, match="Conflicto"):
                publish_weekly_plan(inbox, processed, published, costs, _config())
    assert results[0][0] != results[1][0]
    assert results[0][1] != results[1][1]
    assert results[0][2] != results[1][2]
