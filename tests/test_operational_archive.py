"""Operational model state and frozen DVC input are immutable by UTC cut."""

from datetime import date, datetime, time, timedelta, timezone
import json
from pathlib import Path

import pytest

from inventory.dated_source import publish_delivery
from inventory.operations.ingestion import validate_and_record
from inventory.operations.operational_archive import freeze, verify
from inventory.operations.preparation import prepare_and_record
from inventory.operations.weekly_forecast import publish_weekly_plan
from inventory.forecast_monitoring import evaluate_publication
from inventory.published_forecasts import _read_envelope, read_publication
from inventory.train_schema import TRAIN_COLUMNS


REPO = Path(__file__).resolve().parents[1]
START = date(2017, 8, 1)
CUTOFF = "2017-07-31T06:00:00Z"
EVALUATION = "2017-08-01T06:00:00Z"


def _at(day: date) -> datetime:
    return datetime.combine(day, time(6), tzinfo=timezone.utc)


@pytest.fixture
def period(tmp_path: Path) -> tuple[Path, str]:
    root = tmp_path / "source"
    inbox = root / "data/inbox"
    processed = root / "data/processed"
    published = root / "data/published"
    first = date(2017, 5, 27)
    for offset in range(65):
        day = first + timedelta(days=offset)
        publish_delivery(
            "train", day, _at(day + timedelta(days=1)), 1, TRAIN_COLUMNS,
            [[str(offset + 1), day.isoformat(), "1", "A", str(10 + offset % 7), "0"]],
            inbox,
        )
    costs = root / "config/forecast_costs.json"
    costs.parent.mkdir(parents=True)
    costs.write_text(json.dumps({"underforecast_per_unit": 5,
                                 "overforecast_per_unit": 1,
                                 "min_relative_improvement": 0.05}), encoding="utf-8")
    validate_and_record(inbox, CUTOFF, processed)
    prepare_and_record(inbox, processed, "2017-06-04", "2017-07-31", CUTOFF)
    result = publish_weekly_plan(inbox, processed, published, costs, {
        "forecast_start_date": START.isoformat(), "cutoff_at": CUTOFF,
        "train_start": "2017-06-04", "validation_start": "2017-06-18",
        "selection_start": "2017-07-02", "test_start": "2017-07-16",
        "last_test_start": "2017-07-16", "report_cutoff_at": "2017-07-24T06:00:00Z",
        "calibration_first_start": "2017-07-16",
        "calibration_last_start": "2017-07-16", "kind": "historical_simulation",
    })
    evaluate_publication(inbox, result["publication_id"], _at(START),
                         published_dir=published)
    return root, result["publication_id"]


def test_model_state_matches_forecasts_and_freeze_is_idempotent(period, tmp_path: Path) -> None:
    root, publication_id = period
    model_path = root / "data/published/model_states" / f"{publication_id}.json"
    model = _read_envelope(model_path)
    publication = read_publication(publication_id, published_dir=root / "data/published")
    assert model["name"] == publication.model_name
    assert model["state_sha256"] == publication.model_state_sha256
    assert model["daily_residuals"] and model["weekly_residuals"]
    archive_root = tmp_path / "archives"
    first = freeze(root, archive_root, publication_id, EVALUATION, repo_root=REPO)
    before = {path.relative_to(first): (path.read_bytes(), path.stat().st_mtime_ns)
              for path in first.rglob("*") if path.is_file()}
    assert verify(first, REPO)["forecast_rows"] == 7
    assert freeze(root, archive_root, publication_id, EVALUATION, repo_root=REPO) == first
    assert before == {path.relative_to(first): (path.read_bytes(), path.stat().st_mtime_ns)
                      for path in first.rglob("*") if path.is_file()}
    future = date(2017, 8, 2)
    publish_delivery("train", future, _at(future + timedelta(days=1)), 1,
                     TRAIN_COLUMNS,
                     [["100", future.isoformat(), "1", "A", "999", "0"]],
                     root / "data/inbox")
    assert freeze(root, archive_root, publication_id, EVALUATION, repo_root=REPO) == first
    assert before == {path.relative_to(first): (path.read_bytes(), path.stat().st_mtime_ns)
                      for path in first.rglob("*") if path.is_file()}


def test_late_revision_conflicts_and_frozen_copy_survives(period, tmp_path: Path) -> None:
    root, publication_id = period
    archive = freeze(root, tmp_path / "archives", publication_id, EVALUATION,
                     repo_root=REPO)
    day = date(2017, 7, 29)
    publish_delivery("train", day, _at(date(2017, 7, 31)), 2,
                     TRAIN_COLUMNS,
                     [["999", day.isoformat(), "1", "A", "100", "0"]],
                     root / "data/inbox")
    with pytest.raises(ValueError, match="Conflicto"):
        freeze(root, tmp_path / "archives", publication_id, EVALUATION,
               repo_root=REPO)
    assert verify(archive, REPO)["order_count"] == 1
    (archive / "model/model.json").write_text("alterado", encoding="utf-8")
    with pytest.raises(ValueError, match="huella"):
        verify(archive, REPO)


def test_post_decision_revision_is_only_visible_to_later_evaluation(period, tmp_path: Path) -> None:
    root, publication_id = period
    inbox = root / "data/inbox"
    revised_day = date(2017, 7, 29)
    publish_delivery("train", revised_day, _at(START), 2,
                     TRAIN_COLUMNS,
                     [["999", revised_day.isoformat(), "1", "A", "100", "0"]], inbox)
    publish_delivery("train", START, _at(START + timedelta(days=1)), 1,
                     TRAIN_COLUMNS,
                     [["1000", START.isoformat(), "1", "A", "20", "0"]], inbox)
    later = _at(START + timedelta(days=1))
    evaluate_publication(inbox, publication_id, later,
                         published_dir=root / "data/published")
    archive = freeze(root, tmp_path / "archives", publication_id,
                     "2017-08-02T06:00:00Z", repo_root=REPO)
    manifest = _read_envelope(archive / "manifest.json")
    assert len(manifest["evaluation_deliveries"]) == len(manifest["decision_deliveries"]) + 2
    assert verify(archive, REPO)["evaluated_pairs"] == 1
