"""Contracts for the read-only, as-of ingestion operation used by Airflow."""

import csv
from datetime import datetime, timezone
from pathlib import Path

import pytest

from inventory.holidays_events import HOLIDAYS_COLUMNS, simulate_holidays_delivery
from inventory.oil import OIL_COLUMNS, simulate_oil_delivery
from inventory.operations.ingestion import parse_cutoff_at, validate_and_record
from inventory.published_forecasts import _read_envelope
from inventory.stores import STORES_COLUMNS, simulate_stores_delivery
from inventory.train_delivery import simulate_train_delivery
from inventory.train_schema import TRAIN_COLUMNS
from inventory.transactions import TRANSACTIONS_COLUMNS, simulate_transactions_delivery


def _csv(path: Path, header: tuple[str, ...], rows: list[list[str]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.writer(output)
        writer.writerow(header)
        writer.writerows(rows)
    return path


@pytest.fixture
def deliveries(tmp_path: Path) -> tuple[Path, Path, dict[str, Path]]:
    raw = tmp_path / "raw"
    inbox = tmp_path / "inbox"
    processed = tmp_path / "processed"
    paths = {
        "train": simulate_train_delivery("2013-01-01", _csv(
            raw / "train.csv", TRAIN_COLUMNS,
            [["1", "2013-01-01", "1", "GROCERY I", "2.5", "0"]],
        ), inbox),
        "transactions": simulate_transactions_delivery("2013-01-01", _csv(
            raw / "transactions.csv", TRANSACTIONS_COLUMNS,
            [["2013-01-01", "1", "7"]],
        ), inbox),
        "oil": simulate_oil_delivery("2013-01-01", _csv(
            raw / "oil.csv", OIL_COLUMNS, [["2013-01-01", ""]],
        ), inbox),
        "holidays_events": simulate_holidays_delivery("2013-01-01", _csv(
            raw / "holidays_events.csv", HOLIDAYS_COLUMNS,
            [["2013-01-01", "Holiday", "National", "Ecuador", "Año Nuevo", "False"]],
        ), inbox),
        "stores": simulate_stores_delivery(_csv(
            raw / "stores.csv", STORES_COLUMNS,
            [["1", "Quito", "Pichincha", "D", "13"]],
        ), inbox),
    }
    assert all(paths.values())
    return inbox, processed, paths


def _report_path(processed: Path, instant: str) -> Path:
    return processed / "ingestion" / f"{instant}.json"


def test_t0_d_plus_one_and_idempotent_read_only_ingestion(deliveries) -> None:
    inbox, processed, paths = deliveries
    original = {name: path.read_bytes() for name, path in paths.items()}
    t0 = validate_and_record(inbox, "2013-01-01T00:00:00Z", processed)
    assert t0["record_counts"] == {
        "train": 0, "transactions": 0, "oil": 0, "holidays_events": 0, "stores": 1,
    }
    assert len(t0["selected_deliveries"]) == 1

    before = validate_and_record(inbox, "2013-01-02T05:59:59Z", processed)
    assert before["record_counts"] == t0["record_counts"]
    at_arrival = validate_and_record(inbox, "2013-01-02T06:00:00Z", processed)
    assert at_arrival["record_counts"] == dict.fromkeys(t0["record_counts"], 1)
    assert len(at_arrival["selected_deliveries"]) == 5
    path = _report_path(processed, "2013-01-02T060000Z")
    saved, modified = path.read_bytes(), path.stat().st_mtime_ns
    assert _read_envelope(path) == at_arrival
    assert validate_and_record(inbox, "2013-01-02T06:00:00+00:00", processed) == at_arrival
    assert path.read_bytes() == saved and path.stat().st_mtime_ns == modified
    assert {name: path.read_bytes() for name, path in paths.items()} == original


def test_late_correction_creates_new_cut_without_changing_old(deliveries, tmp_path: Path) -> None:
    inbox, processed, paths = deliveries
    old = validate_and_record(inbox, "2013-01-02T06:00:00Z", processed)
    old_path = _report_path(processed, "2013-01-02T060000Z")
    old_bytes = old_path.read_bytes()
    corrected_raw = _csv(tmp_path / "corrected.csv", TRANSACTIONS_COLUMNS,
                         [["2013-01-01", "1", "9"]])
    correction = simulate_transactions_delivery(
        "2013-01-01", corrected_raw, inbox, revision=2,
        available_at=datetime(2013, 1, 3, 6, tzinfo=timezone.utc),
    )
    assert correction is not None
    before_correction = validate_and_record(inbox, "2013-01-03T05:59:59Z", processed)
    assert before_correction["selected_deliveries"] == old["selected_deliveries"]
    new = validate_and_record(inbox, "2013-01-03T06:00:00Z", processed)
    assert any(ref["path"] == correction.relative_to(inbox).as_posix()
               for ref in new["selected_deliveries"])
    assert new["selected_deliveries"] != old["selected_deliveries"]
    assert old_path.read_bytes() == old_bytes
    assert validate_and_record(inbox, "2013-01-03T06:00:00Z", processed) == new
    assert paths["transactions"].is_file()


def test_retroactive_delivery_conflicts_with_existing_cut(deliveries, tmp_path: Path) -> None:
    inbox, processed, _ = deliveries
    cutoff = "2013-01-03T06:00:00Z"
    original = validate_and_record(inbox, cutoff, processed)
    saved = _report_path(processed, "2013-01-03T060000Z").read_bytes()
    corrected_raw = _csv(tmp_path / "corrected.csv", TRANSACTIONS_COLUMNS,
                         [["2013-01-01", "1", "9"]])
    simulate_transactions_delivery(
        "2013-01-01", corrected_raw, inbox, revision=2,
        available_at=datetime(2013, 1, 3, 5, 59, tzinfo=timezone.utc),
    )
    with pytest.raises(ValueError, match="Conflicto"):
        validate_and_record(inbox, cutoff, processed)
    assert _read_envelope(_report_path(processed, "2013-01-03T060000Z")) == original
    assert _report_path(processed, "2013-01-03T060000Z").read_bytes() == saved


@pytest.mark.parametrize("value", [None, "", "2013-01-02", "2013-01-02T06:00:00",
                                    "2013-01-02T07:00:00+01:00", "2013-02-30T06:00:00Z"])
def test_cutoff_requires_valid_explicit_utc(value) -> None:
    with pytest.raises(ValueError, match="cutoff_at"):
        parse_cutoff_at(value)


def test_missing_or_empty_inbox_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="inbox"):
        validate_and_record(tmp_path / "missing", "2013-01-02T06:00:00Z", tmp_path)
    inbox = tmp_path / "empty"
    inbox.mkdir()
    with pytest.raises(ValueError, match="No hay entregas visibles"):
        validate_and_record(inbox, "2013-01-02T06:00:00Z", tmp_path)
    assert not (tmp_path / "ingestion").exists()


def test_invalid_selected_batch_is_rejected_without_report(deliveries) -> None:
    inbox, processed, paths = deliveries
    paths["oil"].write_text("incorrect\n", encoding="utf-8")
    with pytest.raises(ValueError, match="oil.csv debe tener columnas"):
        validate_and_record(inbox, "2013-01-02T06:00:00Z", processed)
    assert not (processed / "ingestion").exists()


def test_invalid_future_correction_does_not_affect_earlier_cut(deliveries, tmp_path: Path) -> None:
    inbox, processed, _ = deliveries
    later_raw = _csv(tmp_path / "later.csv", OIL_COLUMNS, [["2013-01-01", "11"]])
    later = simulate_oil_delivery(
        "2013-01-01", later_raw, inbox, revision=2,
        available_at=datetime(2013, 1, 3, 6, tzinfo=timezone.utc),
    )
    assert later is not None
    later.write_text("incorrect\n", encoding="utf-8")
    earlier = validate_and_record(inbox, "2013-01-02T06:00:00Z", processed)
    assert earlier["record_counts"]["oil"] == 1
    with pytest.raises(ValueError, match="oil.csv debe tener columnas"):
        validate_and_record(inbox, "2013-01-03T06:00:00Z", processed)


def test_premature_delivery_is_rejected_even_if_not_selected(deliveries) -> None:
    inbox, processed, _ = deliveries
    early = inbox / ("source=oil/observation_date=2013-01-01/"
                     "available_at=2013-01-02T055959Z/revision=0002/"
                     "delivery_id=early-oil/data.csv")
    _csv(early, OIL_COLUMNS, [["2013-01-01", "10"]])
    with pytest.raises(ValueError, match="Entrega anticipada de oil"):
        validate_and_record(inbox, "2013-01-01T00:00:00Z", processed)
    assert not (processed / "ingestion").exists()
