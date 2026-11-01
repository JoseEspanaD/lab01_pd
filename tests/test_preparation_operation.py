"""Immutable preparation by ingestion cut and each row's prediction cut."""

import csv
from datetime import datetime, timezone
from pathlib import Path

import pytest

from inventory.operations.ingestion import validate_and_record
from inventory.operations.preparation import prepare_and_record
from inventory.published_forecasts import _read_envelope
from inventory.stores import STORES_COLUMNS, simulate_stores_delivery
from inventory.train_delivery import simulate_train_delivery
from inventory.train_schema import TRAIN_COLUMNS
from inventory.transactions import TRANSACTIONS_COLUMNS, simulate_transactions_delivery


def _csv(path: Path, columns: tuple[str, ...], rows: list[list[str]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.writer(output)
        writer.writerow(columns)
        writer.writerows(rows)
    return path


@pytest.fixture
def deliveries(tmp_path: Path) -> tuple[Path, Path]:
    inbox, processed = tmp_path / "inbox", tmp_path / "processed"
    simulate_stores_delivery(_csv(tmp_path / "stores.csv", STORES_COLUMNS,
                                  [["1", "Quito", "Pichincha", "D", "13"]]), inbox)
    for day, sales in ((10, "2"), (12, "5"), (13, "999")):
        simulate_train_delivery(
            f"2017-08-{day}",
            _csv(tmp_path / f"train-{day}.csv", TRAIN_COLUMNS,
                 [[str(day), f"2017-08-{day}", "1", "A", sales, "0"]]), inbox,
        )
    simulate_transactions_delivery(
        "2017-08-10", _csv(tmp_path / "transactions.csv", TRANSACTIONS_COLUMNS,
                              [["2017-08-10", "1", "20"]]), inbox,
    )
    return inbox, processed


def _at(day: int) -> str:
    return f"2017-08-{day:02d}T06:00:00Z"


def _prepare(inbox: Path, processed: Path, cutoff: str) -> dict:
    return prepare_and_record(inbox, processed, "2017-08-12", "2017-08-12", cutoff)


def test_requires_matching_ingestion_report_and_explicit_inputs(deliveries) -> None:
    inbox, processed = deliveries
    with pytest.raises(FileNotFoundError, match="ingest_deliveries"):
        _prepare(inbox, processed, _at(13))
    validate_and_record(inbox, _at(13), processed)
    for start, end, cutoff in ((None, "2017-08-12", _at(13)),
                               ("2017-08-13", "2017-08-12", _at(13)),
                               ("2017-08-12", "2017-08-12", "2017-08-13T07:00:00+01:00")):
        with pytest.raises(ValueError):
            prepare_and_record(inbox, processed, start, end, cutoff)


def test_cache_reuses_exact_result_without_repreparing(deliveries, monkeypatch) -> None:
    inbox, processed = deliveries
    validate_and_record(inbox, _at(13), processed)
    first = _prepare(inbox, processed, _at(13))
    assert first["row_count"] == 1 and first["reused"] is False
    path = Path(first["path"])
    saved = (path.read_bytes(), path.stat().st_mtime_ns)
    row = _read_envelope(path)["rows"][0]
    assert row["label_sales"] == "5"
    assert row["prediction_cutoff_at"] == "2017-08-11T06:00:00+00:00"
    assert row["features"]["last_sales"] == "2"
    assert row["features"]["last_transactions"] == 20

    def unexpected(*args, **kwargs):
        raise AssertionError("La preparación no debe repetirse")

    monkeypatch.setattr("inventory.operations.preparation.prepare_training_data", unexpected)
    repeated = _prepare(inbox, processed, _at(13))
    assert repeated["reused"] is True and repeated["row_count"] == 1
    assert (path.read_bytes(), path.stat().st_mtime_ns) == saved
    assert all(item["target_date"] != "2017-08-13"
               for item in _read_envelope(path)["rows"])


def test_later_corrections_change_later_label_but_not_historical_features(deliveries,
                                                                          tmp_path: Path) -> None:
    inbox, processed = deliveries
    validate_and_record(inbox, _at(13), processed)
    old = _prepare(inbox, processed, _at(13))
    old_path = Path(old["path"])
    old_bytes = old_path.read_bytes()

    simulate_transactions_delivery(
        "2017-08-10", _csv(tmp_path / "corrected-transactions.csv", TRANSACTIONS_COLUMNS,
                              [["2017-08-10", "1", "99"]]), inbox,
        revision=2, available_at=datetime(2017, 8, 14, 6, tzinfo=timezone.utc),
    )
    # A complete external train correction follows the same inbox contract.
    corrected_label = (inbox / "source=train/observation_date=2017-08-12"
                       / "available_at=2017-08-14T060000Z/revision=0002"
                       / "delivery_id=train-2017-08-12-r0002/data.csv")
    _csv(corrected_label, TRAIN_COLUMNS, [["12", "2017-08-12", "1", "A", "8", "0"]])

    assert _prepare(inbox, processed, _at(13))["reused"] is True
    assert old_path.read_bytes() == old_bytes
    validate_and_record(inbox, _at(14), processed)
    later = _prepare(inbox, processed, _at(14))
    later_row = _read_envelope(Path(later["path"]))["rows"][0]
    old_row = _read_envelope(old_path)["rows"][0]
    assert later_row["label_sales"] == "8"
    assert later_row["features"] == old_row["features"]
    assert later_row["prediction_cutoff_at"] == old_row["prediction_cutoff_at"]
    assert len(_read_envelope(Path(later["path"]))["history_deliveries"]) > len(
        _read_envelope(old_path)["history_deliveries"])


def test_retroactive_input_conflicts_without_overwriting(deliveries, tmp_path: Path) -> None:
    inbox, processed = deliveries
    validate_and_record(inbox, _at(13), processed)
    old = _prepare(inbox, processed, _at(13))
    path = Path(old["path"])
    saved = path.read_bytes()
    simulate_transactions_delivery(
        "2017-08-10", _csv(tmp_path / "retroactive.csv", TRANSACTIONS_COLUMNS,
                              [["2017-08-10", "1", "99"]]), inbox,
        revision=2, available_at=datetime(2017, 8, 12, 6, tzinfo=timezone.utc),
    )
    with pytest.raises(ValueError, match="Conflicto"):
        _prepare(inbox, processed, _at(13))
    assert path.read_bytes() == saved


def test_future_label_is_absent_until_its_cut_and_future_sales_are_not_features(deliveries) -> None:
    inbox, processed = deliveries
    validate_and_record(inbox, _at(13), processed)
    before = prepare_and_record(inbox, processed, "2017-08-13", "2017-08-13", _at(13))
    assert before["row_count"] == 0
    assert _read_envelope(Path(before["path"]))["rows"] == []

    validate_and_record(inbox, _at(14), processed)
    after = prepare_and_record(inbox, processed, "2017-08-13", "2017-08-13", _at(14))
    row = _read_envelope(Path(after["path"]))["rows"][0]
    assert row["label_sales"] == "999"
    assert row["prediction_cutoff_at"] == "2017-08-12T06:00:00+00:00"
    assert row["features"]["last_sales"] == "2"
