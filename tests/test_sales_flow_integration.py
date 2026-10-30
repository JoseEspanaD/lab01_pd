"""Exercise the delivery, selection, and sales ingestion boundary together."""

import csv
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from inventory.delivery_selector import select_deliveries
from inventory.sales_ingestion import ingest_sales
from inventory.train_delivery import simulate_train_delivery
from inventory.train_schema import SaleRecord, TRAIN_COLUMNS


def _write_train(path: Path, rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.writer(output)
        writer.writerow(TRAIN_COLUMNS)
        writer.writerows(rows)


def test_original_and_external_correction_keep_historical_cuts_stable(tmp_path: Path) -> None:
    raw = tmp_path / "raw" / "train.csv"
    inbox = tmp_path / "inbox"
    _write_train(
        raw,
        [
            ["1", "2017-08-15", "1", "AUTOMOTIVE", "2.0", "0"],
            ["2", "2017-08-15", "2", "GROCERY I", "3.0", "1"],
        ],
    )
    raw_before = raw.read_bytes()

    original_path = simulate_train_delivery("2017-08-15", raw, inbox)
    original_bytes = original_path.read_bytes()
    original_cutoff = datetime(2017, 8, 16, 6, tzinfo=timezone.utc)
    original_records = [
        SaleRecord(1, date(2017, 8, 15), 1, "AUTOMOTIVE", Decimal("2.0"), 0),
        SaleRecord(2, date(2017, 8, 15), 2, "GROCERY I", Decimal("3.0"), 1),
    ]
    assert select_deliveries(inbox, original_cutoff) == [original_path]
    assert ingest_sales(inbox, original_cutoff) == original_records

    # The first simulator publishes only revision 0001. A separate producer
    # provides this complete correction using the same immutable path contract.
    correction_path = (
        inbox
        / "source=train/observation_date=2017-08-15"
        / "available_at=2017-08-18T060000Z/revision=0002"
        / "delivery_id=train-2017-08-15-r0002/data.csv"
    )
    _write_train(correction_path, [["1", "2017-08-15", "1", "AUTOMOTIVE", "9.5", "0"]])
    corrected_cutoff = datetime(2017, 8, 18, 6, tzinfo=timezone.utc)
    corrected_records = [
        SaleRecord(1, date(2017, 8, 15), 1, "AUTOMOTIVE", Decimal("9.5"), 0)
    ]

    assert select_deliveries(inbox, corrected_cutoff) == [correction_path]
    assert ingest_sales(inbox, corrected_cutoff) == corrected_records
    assert select_deliveries(inbox, original_cutoff) == [original_path]
    assert ingest_sales(inbox, original_cutoff) == original_records
    assert ingest_sales(inbox, original_cutoff) == original_records
    assert ingest_sales(inbox, corrected_cutoff) == corrected_records
    assert original_path.read_bytes() == original_bytes
    assert raw.read_bytes() == raw_before
    assert set(inbox.rglob("data.csv")) == {original_path, correction_path}
    assert not (tmp_path / "processed").exists()


def test_ingestion_rejects_invalid_csv_placed_directly_in_inbox(tmp_path: Path) -> None:
    inbox = tmp_path / "inbox"
    invalid_path = (
        inbox
        / "source=train/observation_date=2017-08-15"
        / "available_at=2017-08-16T060000Z/revision=0001"
        / "delivery_id=manual-invalid/data.csv"
    )
    _write_train(invalid_path, [["1", "2017-08-15", "1", "AUTOMOTIVE", "NaN", "0"]])

    with pytest.raises(ValueError, match="sales debe ser finito"):
        ingest_sales(inbox, datetime(2017, 8, 16, 6, tzinfo=timezone.utc))
    assert invalid_path.exists()
