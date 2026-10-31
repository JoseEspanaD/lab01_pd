"""Exercise all five source producers and ingesters at shared historical cutoffs."""

import csv
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from inventory.delivery_selector import select_deliveries
from inventory.holidays_events import (
    HOLIDAYS_COLUMNS,
    HolidayEventRecord,
    ingest_holidays,
    simulate_holidays_delivery,
)
from inventory.oil import OIL_COLUMNS, OilRecord, ingest_oil, simulate_oil_delivery
from inventory.sales_ingestion import ingest_sales
from inventory.stores import STORES_COLUMNS, T0, StoreRecord, ingest_stores, simulate_stores_delivery
from inventory.train_delivery import simulate_train_delivery
from inventory.train_schema import TRAIN_COLUMNS, SaleRecord
from inventory.transactions import (
    TRANSACTIONS_COLUMNS,
    TransactionRecord,
    ingest_transactions,
    simulate_transactions_delivery,
)


def _write_csv(path: Path, columns: tuple[str, ...], rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.writer(output)
        writer.writerow(columns)
        writer.writerows(rows)


def _ingest_all(inbox: Path, cutoff: datetime) -> dict[str, list]:
    return {
        "train": ingest_sales(inbox, cutoff),
        "transactions": ingest_transactions(inbox, cutoff),
        "oil": ingest_oil(inbox, cutoff),
        "holidays_events": ingest_holidays(inbox, cutoff),
        "stores": ingest_stores(inbox, cutoff),
    }


def test_five_sources_share_cutoff_and_keep_historical_results(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    inbox = tmp_path / "inbox"
    raw = {name: raw_dir / f"{name}.csv" for name in ("train", "transactions", "oil", "holidays_events", "stores")}
    _write_csv(raw["train"], TRAIN_COLUMNS, [
        ["1", "2013-01-01", "1", "GROCERY I", "2.50", "0"],
        ["2", "2013-01-01", "2", "AUTOMOTIVE", "3.00", "1"],
    ])
    _write_csv(raw["transactions"], TRANSACTIONS_COLUMNS, [
        ["2013-01-01", "1", "7"],
        ["2013-01-01", "2", "8"],
    ])
    _write_csv(raw["oil"], OIL_COLUMNS, [["2013-01-01", ""]])
    _write_csv(raw["holidays_events"], HOLIDAYS_COLUMNS, [
        ["2013-01-01", "Holiday", "Local", "Quito", "Fundacion", "False"],
        ["2013-01-01", "Holiday", "Regional", "Pichincha", "Fundacion", "True"],
        ["2013-01-02", "Event", "National", "Ecuador", "Festival", "False"],
    ])
    _write_csv(raw["stores"], STORES_COLUMNS, [
        ["1", "Quito", "Pichincha", "D", "13"],
        ["2", "Manta", "Manabi", "C", "4"],
    ])
    original_raw = {name: path.read_bytes() for name, path in raw.items()}

    paths = {
        "train": simulate_train_delivery("2013-01-01", raw["train"], inbox),
        "transactions": simulate_transactions_delivery("2013-01-01", raw["transactions"], inbox),
        "oil": simulate_oil_delivery("2013-01-01", raw["oil"], inbox),
        "holidays_events": simulate_holidays_delivery("2013-01-01", raw["holidays_events"], inbox),
        "stores": simulate_stores_delivery(raw["stores"], inbox),
    }
    future_holiday = simulate_holidays_delivery("2013-01-02", raw["holidays_events"], inbox)
    before_arrival = datetime(2013, 1, 2, 5, 59, 59, tzinfo=timezone.utc)
    first_cut = datetime(2013, 1, 2, 6, tzinfo=timezone.utc)
    later_cut = datetime(2013, 1, 3, 6, tzinfo=timezone.utc)
    stores_only = [StoreRecord(1, "Quito", "Pichincha", "D", 13), StoreRecord(2, "Manta", "Manabi", "C", 4)]
    empty_dated = {"train": [], "transactions": [], "oil": [], "holidays_events": [], "stores": stores_only}

    assert select_deliveries(inbox, T0 - timedelta(seconds=1)) == []
    assert _ingest_all(inbox, T0 - timedelta(seconds=1)) == {name: [] for name in empty_dated}
    assert select_deliveries(inbox, T0) == [paths["stores"]]
    assert _ingest_all(inbox, before_arrival) == empty_dated
    assert select_deliveries(inbox, first_cut) == sorted(paths.values())

    first_records = {
        "train": [
            SaleRecord(1, date(2013, 1, 1), 1, "GROCERY I", Decimal("2.50"), 0),
            SaleRecord(2, date(2013, 1, 1), 2, "AUTOMOTIVE", Decimal("3.00"), 1),
        ],
        "transactions": [
            TransactionRecord(date(2013, 1, 1), 1, 7),
            TransactionRecord(date(2013, 1, 1), 2, 8),
        ],
        "oil": [OilRecord(date(2013, 1, 1), None)],
        "holidays_events": [
            HolidayEventRecord(date(2013, 1, 1), "Holiday", "Local", "Quito", "Fundacion", False),
            HolidayEventRecord(date(2013, 1, 1), "Holiday", "Regional", "Pichincha", "Fundacion", True),
        ],
        "stores": stores_only,
    }
    assert _ingest_all(inbox, first_cut) == first_records
    assert _ingest_all(inbox, later_cut - timedelta(seconds=1)) == first_records

    correction_raw = tmp_path / "correction" / "transactions.csv"
    _write_csv(correction_raw, TRANSACTIONS_COLUMNS, [["2013-01-01", "1", "11"]])
    transaction_correction = simulate_transactions_delivery(
        "2013-01-01", correction_raw, inbox, revision=2, available_at=later_cut
    )
    expected_paths = sorted([paths["train"], transaction_correction, paths["oil"], paths["stores"], paths["holidays_events"], future_holiday])
    assert select_deliveries(inbox, later_cut) == expected_paths
    later_records = _ingest_all(inbox, later_cut)
    assert later_records["transactions"] == [TransactionRecord(date(2013, 1, 1), 1, 11)]
    assert later_records["holidays_events"] == first_records["holidays_events"] + [
        HolidayEventRecord(date(2013, 1, 2), "Event", "National", "Ecuador", "Festival", False)
    ]
    for source in ("train", "oil", "stores"):
        assert later_records[source] == first_records[source]

    inbox_bytes = {path: path.read_bytes() for path in inbox.rglob("data.csv")}
    assert select_deliveries(inbox, first_cut) == sorted(paths.values())
    assert _ingest_all(inbox, first_cut) == first_records
    assert _ingest_all(inbox, first_cut) == first_records
    assert _ingest_all(inbox, later_cut) == later_records
    assert {path: path.read_bytes() for path in inbox.rglob("data.csv")} == inbox_bytes
    assert {name: path.read_bytes() for name, path in raw.items()} == original_raw
    assert not (tmp_path / "processed").exists()
