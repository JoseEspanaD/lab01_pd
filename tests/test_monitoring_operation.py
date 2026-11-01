"""DAG monitoring adapter preserves the existing as-of report contract."""

from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

import pytest

from inventory.dated_source import publish_delivery
from inventory.forecast_monitoring import read_report
from inventory.forecasting import CostPolicy, Forecast
from inventory.operations.monitoring import monitor_and_record
from inventory.published_forecasts import (
    InputReference, PublishedForecasts, _payload, _write_immutable,
)
from inventory.train_schema import TRAIN_COLUMNS


START = date(2017, 8, 10)
PUBLICATION_ID = "monitor-demo"


def _at(day: date) -> datetime:
    return datetime.combine(day, time(6), tzinfo=timezone.utc)


@pytest.fixture
def publication(tmp_path: Path) -> tuple[Path, Path]:
    inbox, published = tmp_path / "inbox", tmp_path / "published"
    cutoff = _at(START - timedelta(days=1))
    forecasts = tuple(Forecast(
        START, START + timedelta(days=offset), 1, "A", cutoff,
        0.0, 10.0, 20.0, 15.0,
    ) for offset in range(7))
    item = PublishedForecasts(
        PUBLICATION_ID, "historical_simulation",
        datetime(2020, 1, 1, tzinfo=timezone.utc), None,
        START, cutoff, "baseline", "a" * 64, cutoff, cutoff,
        "b" * 64, "config/forecast_costs.json", "c" * 64,
        CostPolicy(5.0, 1.0, 0.05),
        (InputReference("source=train/observation_date=2017-08-08/data.csv", "d" * 64),),
        forecasts,
    )
    _write_immutable(published / "forecasts" / f"{PUBLICATION_ID}.json", _payload(item))
    return inbox, published


def test_pending_missing_sales_are_not_zero_and_retry_is_idempotent(publication) -> None:
    inbox, published = publication
    before = monitor_and_record(inbox, published, PUBLICATION_ID, "2017-08-10T06:00:00Z")
    metrics = before["metrics"]
    assert metrics == {"evaluated_pairs": 0, "pending_pairs": 7,
                       "mae": None, "rmse": None, "bias": None, "coverage_80": None}
    report = read_report(PUBLICATION_ID, _at(START), published_dir=published)
    assert all(row.status == "pending" and row.actual_sales is None and row.error is None
               for row in report.rows)
    assert report.sales_deliveries == ()
    path = Path(before["report_path"])
    saved = (path.read_bytes(), path.stat().st_mtime_ns)
    assert monitor_and_record(inbox, published, PUBLICATION_ID,
                              "2017-08-10T06:00:00+00:00") == before
    assert (path.read_bytes(), path.stat().st_mtime_ns) == saved


def test_zero_is_observed_and_future_correction_only_changes_later_cut(publication) -> None:
    inbox, published = publication
    original = publish_delivery(
        "train", START, _at(START + timedelta(days=1)), 1, TRAIN_COLUMNS,
        [["1", START.isoformat(), "1", "A", "0", "0"]], inbox,
    )
    corrected = publish_delivery(
        "train", START, _at(START + timedelta(days=3)), 2, TRAIN_COLUMNS,
        [["1", START.isoformat(), "1", "A", "25", "0"]], inbox,
    )
    first_cut = "2017-08-11T06:00:00Z"
    first = monitor_and_record(inbox, published, PUBLICATION_ID, first_cut)
    assert first["metrics"] == {"evaluated_pairs": 1, "pending_pairs": 6,
                                "mae": 10.0, "rmse": 10.0, "bias": -10.0,
                                "coverage_80": 1.0}
    old = read_report(PUBLICATION_ID, _at(START + timedelta(days=1)), published_dir=published)
    assert old.rows[0].status == "observed" and old.rows[0].actual_sales == 0.0
    assert old.sales_deliveries[0].path == original.relative_to(inbox).as_posix()
    old_bytes = Path(first["report_path"]).read_bytes()

    before_correction = monitor_and_record(
        inbox, published, PUBLICATION_ID, "2017-08-12T06:00:00Z")
    assert before_correction["metrics"] == first["metrics"]
    later = monitor_and_record(inbox, published, PUBLICATION_ID, "2017-08-13T06:00:00Z")
    assert later["metrics"] == {"evaluated_pairs": 1, "pending_pairs": 6,
                                "mae": 15.0, "rmse": 15.0, "bias": 15.0,
                                "coverage_80": 0.0}
    revised = read_report(PUBLICATION_ID, _at(START + timedelta(days=3)),
                          published_dir=published)
    assert revised.sales_deliveries[0].path == corrected.relative_to(inbox).as_posix()
    assert Path(first["report_path"]).read_bytes() == old_bytes


def test_explicit_cut_and_immutable_conflict(publication) -> None:
    inbox, published = publication
    for cut in (None, "2017-08-11", "2017-08-11T07:00:00+01:00"):
        with pytest.raises(ValueError, match="cutoff_at"):
            monitor_and_record(inbox, published, PUBLICATION_ID, cut)
    with pytest.raises(ValueError, match="fecha de evaluación"):
        monitor_and_record(inbox, published, PUBLICATION_ID, "2017-08-08T06:00:00Z")
    with pytest.raises(FileNotFoundError):
        monitor_and_record(inbox, published, "missing", "2017-08-11T06:00:00Z")

    path = publish_delivery(
        "train", START, _at(START + timedelta(days=1)), 1, TRAIN_COLUMNS,
        [["1", START.isoformat(), "1", "A", "3", "0"]], inbox,
    )
    result = monitor_and_record(inbox, published, PUBLICATION_ID, "2017-08-11T06:00:00Z")
    report_path = Path(result["report_path"])
    saved = report_path.read_bytes()
    path.write_text("id,date,store_nbr,family,sales,onpromotion\n"
                    "1,2017-08-10,1,A,4,0\n", encoding="utf-8")
    with pytest.raises(ValueError, match="Conflicto"):
        monitor_and_record(inbox, published, PUBLICATION_ID, "2017-08-11T06:00:00Z")
    assert report_path.read_bytes() == saved
