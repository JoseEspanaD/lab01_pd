"""Producer and consumer share immutable, time-bounded delivery contracts."""

import csv
from pathlib import Path

import pytest

from inventory.holidays_events import HOLIDAYS_COLUMNS
from inventory.oil import OIL_COLUMNS
from inventory.operations.ingestion import validate_and_record
from inventory.operations.production import produce_for_date
from inventory.stores import STORES_COLUMNS
from inventory.train_schema import TRAIN_COLUMNS
from inventory.transactions import TRANSACTIONS_COLUMNS


def _csv(path: Path, columns: tuple[str, ...], rows: list[list[str]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.writer(output)
        writer.writerow(columns)
        writer.writerows(rows)
    return path


@pytest.fixture
def raw(tmp_path: Path) -> Path:
    root = tmp_path / "raw"
    _csv(root / "train.csv", TRAIN_COLUMNS, [
        ["1", "2013-01-01", "1", "GROCERY I", "2.5", "0"],
        ["2", "2013-01-02", "1", "GROCERY I", "3.0", "0"],
    ])
    _csv(root / "transactions.csv", TRANSACTIONS_COLUMNS, [
        ["2013-01-01", "1", "7"], ["2013-01-02", "1", "8"],
    ])
    _csv(root / "oil.csv", OIL_COLUMNS, [
        ["2013-01-01", ""], ["2013-01-02", "9.5"],
    ])
    _csv(root / "holidays_events.csv", HOLIDAYS_COLUMNS, [
        ["2013-01-01", "Holiday", "National", "Ecuador", "New Year", "False"],
        ["2013-01-02", "Event", "Local", "Quito", "Event", "False"],
    ])
    _csv(root / "stores.csv", STORES_COLUMNS, [["1", "Quito", "Pichincha", "D", "13"]])
    return root


def test_one_date_is_idempotent_and_does_not_publish_future_batches(raw: Path, tmp_path: Path) -> None:
    inbox, processed = tmp_path / "inbox", tmp_path / "processed"
    early = produce_for_date("2013-01-01", "2013-01-02T05:59:59Z", raw, inbox)
    assert len(early["deliveries"]) == 1
    assert early["deliveries"][0].startswith("source=stores/")
    first = produce_for_date("2013-01-01", "2013-01-02T06:00:00Z", raw, inbox)
    assert len(first["deliveries"]) == 5
    assert all("observation_date=2013-01-02" not in path for path in first["deliveries"])
    saved = {path: (path.read_bytes(), path.stat().st_mtime_ns)
             for path in inbox.rglob("data.csv")}
    assert produce_for_date("2013-01-01", "2013-01-02T06:00:00+00:00", raw, inbox)[
        "deliveries"] == first["deliveries"]
    assert saved == {path: (path.read_bytes(), path.stat().st_mtime_ns)
                     for path in inbox.rglob("data.csv")}
    report = validate_and_record(inbox, "2013-01-02T06:00:00Z", processed)
    assert report["record_counts"] == dict.fromkeys(
        ("train", "transactions", "oil", "holidays_events", "stores"), 1)

    second = produce_for_date("2013-01-02", "2013-01-03T06:00:00Z", raw, inbox)
    assert len(second["deliveries"]) == 5
    assert {path: (path.read_bytes(), path.stat().st_mtime_ns)
            for path in saved} == saved
    assert len(list(inbox.rglob("data.csv"))) == 9
    assert validate_and_record(inbox, "2013-01-02T06:00:00Z", processed) == report


def test_later_correction_changes_only_later_cut(raw: Path, tmp_path: Path) -> None:
    inbox, processed = tmp_path / "inbox", tmp_path / "processed"
    produce_for_date("2013-01-01", "2013-01-02T06:00:00Z", raw, inbox)
    old = validate_and_record(inbox, "2013-01-02T06:00:00Z", processed)
    corrected = _csv(tmp_path / "correction.csv", TRANSACTIONS_COLUMNS,
                     [["2013-01-01", "1", "11"]])
    kwargs = dict(source="transactions", revision=2,
                  available_at_text="2013-01-03T06:00:00Z", correction_path=corrected)
    new_delivery = produce_for_date("2013-01-01", "2013-01-03T06:00:00Z", raw, inbox,
                                    **kwargs)
    assert len(new_delivery["deliveries"]) == 1
    assert "revision=0002" in new_delivery["deliveries"][0]
    assert produce_for_date("2013-01-01", "2013-01-03T06:00:00Z", raw, inbox,
                            **kwargs) == new_delivery
    assert validate_and_record(inbox, "2013-01-02T06:00:00Z", processed) == old
    later = validate_and_record(inbox, "2013-01-03T06:00:00Z", processed)
    assert later["record_counts"] == old["record_counts"]
    assert later["selected_deliveries"] != old["selected_deliveries"]
    corrected.write_text("date,store_nbr,transactions\n2013-01-01,1,12\n", encoding="utf-8")
    with pytest.raises(FileExistsError, match="difiere"):
        produce_for_date("2013-01-01", "2013-01-03T06:00:00Z", raw, inbox, **kwargs)
    assert validate_and_record(inbox, "2013-01-03T06:00:00Z", processed) == later


def test_correction_requires_explicit_valid_availability(raw: Path, tmp_path: Path) -> None:
    inbox = tmp_path / "inbox"
    corrected = _csv(tmp_path / "correction.csv", OIL_COLUMNS, [["2013-01-01", "12"]])
    for available in (None, "2013-01-02T05:59:59Z", "2013-01-02T06:00:00Z",
                      "2013-01-03T06:00:01Z",
                      "2013-01-03T07:00:00+01:00"):
        with pytest.raises(ValueError):
            produce_for_date("2013-01-01", "2013-01-03T06:00:00Z", raw, inbox,
                             source="oil", revision=2, available_at_text=available,
                             correction_path=corrected)
    assert not list(inbox.rglob("data.csv"))


def test_initial_source_change_conflicts_instead_of_overwriting(raw: Path, tmp_path: Path) -> None:
    inbox = tmp_path / "inbox"
    produce_for_date("2013-01-01", "2013-01-02T06:00:00Z", raw, inbox)
    first = next((inbox / "source=transactions").rglob("data.csv"))
    saved = first.read_bytes()
    _csv(raw / "transactions.csv", TRANSACTIONS_COLUMNS, [["2013-01-01", "1", "12"]])
    with pytest.raises(FileExistsError, match="difiere"):
        produce_for_date("2013-01-01", "2013-01-02T06:00:00Z", raw, inbox)
    assert first.read_bytes() == saved
