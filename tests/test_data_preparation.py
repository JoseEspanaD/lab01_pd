"""Point-in-time training rows from all five inbox sources."""

import csv
from dataclasses import fields
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from inventory.data_preparation import Features, TrainingExample, prepare_training_data
from inventory.holidays_events import HOLIDAYS_COLUMNS, ingest_holidays, simulate_holidays_delivery
from inventory.oil import OIL_COLUMNS, ingest_oil, simulate_oil_delivery
from inventory.sales_ingestion import ingest_sales
from inventory.stores import STORES_COLUMNS, ingest_stores, simulate_stores_delivery
from inventory.train_delivery import simulate_train_delivery
from inventory.train_schema import TRAIN_COLUMNS
from inventory.transactions import TRANSACTIONS_COLUMNS, ingest_transactions, simulate_transactions_delivery


def _at(day: int) -> datetime:
    return datetime(2017, 8, day, 6, tzinfo=timezone.utc)


def _write(path: Path, columns: tuple[str, ...], rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.writer(output)
        writer.writerow(columns)
        writer.writerows(rows)


def _deliver(tmp_path: Path, inbox: Path, name: str, day: int, rows: list[list[str]]) -> None:
    sources = {
        "train": (TRAIN_COLUMNS, simulate_train_delivery),
        "transactions": (TRANSACTIONS_COLUMNS, simulate_transactions_delivery),
        "oil": (OIL_COLUMNS, simulate_oil_delivery),
        "holidays_events": (HOLIDAYS_COLUMNS, simulate_holidays_delivery),
    }
    columns, simulate = sources[name]
    raw = tmp_path / f"{name}-{day}.csv"
    _write(raw, columns, rows)
    simulate(f"2017-08-{day:02d}", raw, inbox)


def test_five_sources_geography_missing_and_label_isolation(tmp_path: Path) -> None:
    inbox = tmp_path / "inbox"
    stores = tmp_path / "stores.csv"
    _write(stores, STORES_COLUMNS, [
        ["1", "Quito", "Pichincha", "D", "13"],
        ["2", "Manta", "Manabi", "C", "4"],
    ])
    simulate_stores_delivery(stores, inbox)
    _deliver(tmp_path, inbox, "train", 10, [
        ["1", "2017-08-10", "1", "GROCERY", "2", "1"],
        ["2", "2017-08-10", "2", "GROCERY", "3", "0"],
    ])
    _deliver(tmp_path, inbox, "train", 11, [
        ["3", "2017-08-11", "1", "GROCERY", "999", "9"],
    ])
    _deliver(tmp_path, inbox, "train", 12, [
        ["4", "2017-08-12", "1", "GROCERY", "5", "0"],
        ["5", "2017-08-12", "2", "GROCERY", "6", "0"],
        ["6", "2017-08-12", "1", "NEW", "7", "0"],
    ])
    _deliver(tmp_path, inbox, "transactions", 10, [["2017-08-10", "1", "20"]])
    _deliver(tmp_path, inbox, "oil", 10, [["2017-08-10", ""]])
    _deliver(tmp_path, inbox, "holidays_events", 10, [
        ["2017-08-10", "Holiday", "National", "Ecuador", "A", "False"],
        ["2017-08-10", "Holiday", "Regional", "Pichincha", "B", "False"],
        ["2017-08-10", "Holiday", "Local", "Quito", "C", "False"],
        ["2017-08-10", "Holiday", "Local", "Manta", "D", "False"],
    ])
    _deliver(tmp_path, inbox, "holidays_events", 11, [
        ["2017-08-11", "Event", "National", "Ecuador", "Future", "False"],
    ])
    _deliver(tmp_path, inbox, "transactions", 11, [["2017-08-11", "1", "999"]])
    _deliver(tmp_path, inbox, "oil", 11, [["2017-08-11", "999"]])

    target = date(2017, 8, 12)
    assert prepare_training_data(inbox, target, target, _at(12)) == []  # Label not yet delivered.
    rows = prepare_training_data(inbox, target, target, _at(13))
    assert [(row.store_nbr, row.family, row.label_sales) for row in rows] == [
        (1, "GROCERY", Decimal(5)), (2, "GROCERY", Decimal(6))
    ]
    assert all(row.prediction_cutoff_at == _at(11) for row in rows)
    assert all("label" not in field.name for field in fields(Features))
    quito, manta = (row.features for row in rows)
    assert quito.last_sales == Decimal(2)
    assert quito.mean_sales_7d == Decimal(2)
    assert quito.sales_age_days == 2
    assert quito.last_onpromotion == 1
    assert quito.last_transactions == 20
    assert quito.transactions_age_days == 2
    assert quito.last_oil_price is None and quito.oil_age_days == 2
    assert quito.known_events_7d == 3
    assert (quito.city, quito.state, quito.store_type, quito.cluster) == ("Quito", "Pichincha", "D", 13)
    assert manta.last_sales == Decimal(3)
    assert manta.last_transactions is None and manta.transactions_age_days is None
    assert manta.known_events_7d == 2
    assert prepare_training_data(inbox, target, target, _at(13)) == rows
    assert not (tmp_path / "processed").exists()


def test_later_corrections_change_labels_but_not_historical_features(tmp_path: Path) -> None:
    inbox = tmp_path / "inbox"
    _deliver(tmp_path, inbox, "train", 10, [["1", "2017-08-10", "1", "A", "2", "0"]])
    _deliver(tmp_path, inbox, "train", 12, [["2", "2017-08-12", "1", "A", "5", "0"]])
    _deliver(tmp_path, inbox, "transactions", 10, [["2017-08-10", "1", "20"]])
    target = date(2017, 8, 12)
    before = prepare_training_data(inbox, target, target, _at(13))

    corrected = tmp_path / "corrected.csv"
    _write(corrected, TRANSACTIONS_COLUMNS, [["2017-08-10", "1", "99"]])
    simulate_transactions_delivery("2017-08-10", corrected, inbox, revision=2, available_at=_at(14))
    history_correction = (inbox / "source=train/observation_date=2017-08-10"
                          / "available_at=2017-08-14T060000Z/revision=0002"
                          / "delivery_id=train-2017-08-10-r0002/data.csv")
    _write(history_correction, TRAIN_COLUMNS, [["1", "2017-08-10", "1", "A", "100", "0"]])
    # A complete external train correction follows the same immutable delivery contract.
    correction = (inbox / "source=train/observation_date=2017-08-12"
                  / "available_at=2017-08-14T060000Z/revision=0002"
                  / "delivery_id=train-2017-08-12-r0002/data.csv")
    _write(correction, TRAIN_COLUMNS, [["2", "2017-08-12", "1", "A", "8", "0"]])
    assert prepare_training_data(inbox, target, target, _at(13)) == before
    after = prepare_training_data(inbox, target, target, _at(14))
    assert after[0].label_sales == Decimal(8)
    assert after[0].features == before[0].features
    assert after[0].features.last_transactions == 20
    assert after[0].features.last_sales == Decimal(2)


def test_each_target_has_its_own_cutoff_and_missing_store_is_explicit(tmp_path: Path) -> None:
    inbox = tmp_path / "inbox"
    for day, amount in ((10, "2"), (11, "4"), (12, "6"), (13, "8")):
        _deliver(tmp_path, inbox, "train", day, [[str(day), f"2017-08-{day}", "1", "A", amount, "0"]])
    rows = prepare_training_data(inbox, date(2017, 8, 12), date(2017, 8, 13), _at(14))
    assert [row.prediction_cutoff_at for row in rows] == [_at(11), _at(12)]
    assert [row.features.last_sales for row in rows] == [Decimal(2), Decimal(4)]
    assert [row.label_sales for row in rows] == [Decimal(6), Decimal(8)]
    assert all(row.features.city is None and row.features.cluster is None for row in rows)


@pytest.mark.parametrize("start,end,cutoff", [
    (datetime(2017, 8, 12), date(2017, 8, 12), _at(13)),
    (date(2017, 8, 13), date(2017, 8, 12), _at(13)),
    (date(2017, 8, 12), date(2017, 8, 12), datetime(2017, 8, 13)),
])
def test_invalid_dates_or_cutoff_are_rejected(tmp_path: Path, start, end, cutoff) -> None:
    with pytest.raises(ValueError):
        prepare_training_data(tmp_path / "inbox", start, end, cutoff)


def _original_rows(inbox: Path, start: date, end: date, cutoff: datetime) -> list[TrainingExample]:
    """The former per-day ingestion algorithm, kept as an independent test oracle."""
    labels = ingest_sales(inbox, cutoff)
    result = []
    for day in sorted({sale.date for sale in labels if start <= sale.date <= end}):
        feature_cutoff = datetime(day.year, day.month, day.day - 1, 6, tzinfo=timezone.utc)
        sales = ingest_sales(inbox, feature_cutoff)
        transactions = ingest_transactions(inbox, feature_cutoff)
        oil = ingest_oil(inbox, feature_cutoff)
        events = ingest_holidays(inbox, feature_cutoff)
        stores = {store.store_nbr: store for store in ingest_stores(inbox, feature_cutoff)}
        week_start = date.fromordinal(day.toordinal() - 8)
        for label in [sale for sale in labels if sale.date == day]:
            history = [sale for sale in sales if sale.store_nbr == label.store_nbr and sale.family == label.family]
            if not history:
                continue
            last = history[-1]
            week = [sale.sales for sale in history if sale.date >= week_start]
            store = stores.get(label.store_nbr)
            store_transactions = [row for row in transactions if row.store_nbr == label.store_nbr]
            transaction = store_transactions[-1] if store_transactions else None
            price = oil[-1] if oil else None
            event_count = sum(
                1 for event in events if event.date >= week_start and (
                    event.locale == "National"
                    or (store is not None and event.locale == "Regional" and event.locale_name == store.state)
                    or (store is not None and event.locale == "Local" and event.locale_name == store.city)
                )
            )
            result.append(TrainingExample(day, label.store_nbr, label.family, feature_cutoff, Features(
                day.weekday(), day.month,
                store.city if store else None, store.state if store else None,
                store.type if store else None, store.cluster if store else None,
                last.sales, (day - last.date).days, sum(week, Decimal(0)) / len(week) if week else None,
                last.onpromotion,
                transaction.transactions if transaction else None,
                (day - transaction.date).days if transaction else None,
                price.dcoilwtico if price else None,
                (day - price.date).days if price else None,
                event_count,
            ), label.sales))
    return result


def test_range_matches_original_and_reads_batches_fewer_times(tmp_path: Path, monkeypatch) -> None:
    inbox = tmp_path / "inbox"
    stores = tmp_path / "stores.csv"
    _write(stores, STORES_COLUMNS, [["1", "Quito", "Pichincha", "D", "13"]])
    simulate_stores_delivery(stores, inbox)
    for day, amount in ((10, "2"), (11, "4"), (12, "6"), (13, "8"), (14, "10")):
        _deliver(tmp_path, inbox, "train", day, [[str(day), f"2017-08-{day}", "1", "A", amount, "0"]])
    for day, amount in ((10, "20"), (11, "40"), (12, "60")):
        _deliver(tmp_path, inbox, "transactions", day, [[f"2017-08-{day}", "1", amount]])
    _deliver(tmp_path, inbox, "oil", 10, [["2017-08-10", ""]])
    _deliver(tmp_path, inbox, "oil", 11, [["2017-08-11", "90"]])
    _deliver(tmp_path, inbox, "holidays_events", 10, [
        ["2017-08-10", "Holiday", "Local", "Quito", "A", "False"]
    ])
    corrected = tmp_path / "corrected.csv"
    _write(corrected, TRANSACTIONS_COLUMNS, [["2017-08-10", "1", "99"]])
    simulate_transactions_delivery("2017-08-10", corrected, inbox, revision=2, available_at=_at(13))
    later = tmp_path / "later.csv"
    _write(later, TRANSACTIONS_COLUMNS, [["2017-08-10", "1", "999"]])
    simulate_transactions_delivery("2017-08-10", later, inbox, revision=3, available_at=_at(17))
    history_correction = (inbox / "source=train/observation_date=2017-08-10"
                          / "available_at=2017-08-13T060000Z/revision=0002"
                          / "delivery_id=train-2017-08-10-r0002/data.csv")
    _write(history_correction, TRAIN_COLUMNS, [["10", "2017-08-10", "1", "A", "100", "0"]])
    label_correction = (inbox / "source=train/observation_date=2017-08-14"
                        / "available_at=2017-08-17T060000Z/revision=0002"
                        / "delivery_id=train-2017-08-14-r0002/data.csv")
    _write(label_correction, TRAIN_COLUMNS, [["14", "2017-08-14", "1", "A", "15", "0"]])

    original_open = Path.open
    opened = []

    def counting_open(path, *args, **kwargs):
        if path.name == "data.csv" and "delivery_id=" in str(path):
            opened.append(path)
        return original_open(path, *args, **kwargs)

    with monkeypatch.context() as patcher:
        patcher.setattr(Path, "open", counting_open)
        expected = _original_rows(inbox, date(2017, 8, 12), date(2017, 8, 14), _at(16))
        original_reads = len(opened)
        opened.clear()
        actual = prepare_training_data(inbox, date(2017, 8, 12), date(2017, 8, 14), _at(16))
        optimized_reads = len(opened)
    assert actual == expected
    assert [row.features.last_transactions for row in actual] == [20, 40, 60]
    assert [row.features.mean_sales_7d for row in actual] == [Decimal(2), Decimal(3), Decimal(110) / 3]
    assert optimized_reads < original_reads
    assert original_reads - optimized_reads >= 3
    assert prepare_training_data(inbox, date(2017, 8, 12), date(2017, 8, 14), _at(17)) == _original_rows(
        inbox, date(2017, 8, 12), date(2017, 8, 14), _at(17)
    )
