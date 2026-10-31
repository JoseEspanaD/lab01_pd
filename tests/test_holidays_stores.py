"""Synthetic historical delivery tests for the store catalog and events."""

import csv
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from inventory.holidays_events import (
    HOLIDAYS_COLUMNS,
    HolidayEventRecord,
    ingest_holidays,
    simulate_holidays_delivery,
)
from inventory.stores import (
    STORES_COLUMNS,
    T0,
    StoreRecord,
    ingest_stores,
    simulate_stores_delivery,
)


def _write_csv(path: Path, columns: tuple[str, ...], rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.writer(output)
        writer.writerow(columns)
        writer.writerows(rows)


def _path(inbox: Path, source: str, day: str, available: str, revision: int = 1) -> Path:
    return (
        inbox / f"source={source}" / f"observation_date={day}"
        / f"available_at={available}" / f"revision={revision:04d}"
        / f"delivery_id={source}-{day}-r{revision:04d}" / "data.csv"
    )


def test_stores_initial_catalog_t0_idempotence_and_read_only_ingestion(tmp_path: Path) -> None:
    raw = tmp_path / "stores.csv"
    inbox = tmp_path / "inbox"
    rows = [
        ["2", "Quito", "Pichincha", "D", "13"],
        ["1", "Santo Domingo", "Santo Domingo de los Tsachilas", "B", "4"],
    ]
    _write_csv(raw, STORES_COLUMNS, rows)
    raw_bytes = raw.read_bytes()

    path = simulate_stores_delivery(raw, inbox)

    assert path == _path(inbox, "stores", "2013-01-01", "2013-01-01T000000Z")
    assert simulate_stores_delivery(raw, inbox) == path
    assert raw.read_bytes() == raw_bytes
    with path.open(newline="", encoding="utf-8") as batch:
        assert list(csv.reader(batch)) == [list(STORES_COLUMNS), *rows]
    published_bytes = path.read_bytes()
    assert ingest_stores(inbox, T0 - timedelta(seconds=1)) == []
    expected = [
        StoreRecord(1, "Santo Domingo", "Santo Domingo de los Tsachilas", "B", 4),
        StoreRecord(2, "Quito", "Pichincha", "D", 13),
    ]
    assert ingest_stores(inbox, T0) == expected
    assert ingest_stores(inbox, T0) == expected
    assert path.read_bytes() == published_bytes
    assert not (tmp_path / "processed").exists()


def test_stores_correction_replaces_catalog_after_its_arrival(tmp_path: Path) -> None:
    raw = tmp_path / "stores.csv"
    inbox = tmp_path / "inbox"
    _write_csv(raw, STORES_COLUMNS, [["1", "Quito", "Pichincha", "D", "13"], ["2", "Manta", "Manabi", "C", "4"]])
    original = simulate_stores_delivery(raw, inbox)
    original_bytes = original.read_bytes()
    initial = ingest_stores(inbox, T0)
    corrected_at = T0 + timedelta(days=2)
    _write_csv(raw, STORES_COLUMNS, [["1", "Quito", "Pichincha", "A", "13"]])
    correction = simulate_stores_delivery(raw, inbox, revision=2, available_at=corrected_at)

    assert correction == _path(inbox, "stores", "2013-01-01", "2013-01-03T000000Z", 2)
    assert simulate_stores_delivery(raw, inbox, revision=2, available_at=corrected_at) == correction
    assert ingest_stores(inbox, corrected_at - timedelta(seconds=1)) == initial
    assert ingest_stores(inbox, corrected_at) == [StoreRecord(1, "Quito", "Pichincha", "A", 13)]
    assert ingest_stores(inbox, T0) == initial
    assert original.read_bytes() == original_bytes
    with pytest.raises(ValueError, match="posterior"):
        simulate_stores_delivery(raw, inbox, revision=3, available_at=corrected_at)


@pytest.mark.parametrize(
    "columns,rows",
    [
        (("city", "store_nbr", "state", "type", "cluster"), [["1", "Quito", "Pichincha", "D", "13"]]),
        (STORES_COLUMNS, [["0", "Quito", "Pichincha", "D", "13"]]),
        (STORES_COLUMNS, [["1", "", "Pichincha", "D", "13"]]),
        (STORES_COLUMNS, [["1", "Quito", "", "D", "13"]]),
        (STORES_COLUMNS, [["1", "Quito", "Pichincha", "X", "13"]]),
        (STORES_COLUMNS, [["1", "Quito", "Pichincha", "D", "0"]]),
        (STORES_COLUMNS, [["1", "Quito", "Pichincha", "D", "13"], ["01", "Manta", "Manabi", "C", "4"]]),
        (STORES_COLUMNS, []),
    ],
)
def test_stores_rejects_invalid_raw_catalog(tmp_path: Path, columns, rows) -> None:
    raw = tmp_path / "stores.csv"
    inbox = tmp_path / "inbox"
    _write_csv(raw, columns, rows)
    with pytest.raises(ValueError):
        simulate_stores_delivery(raw, inbox)
    assert not inbox.exists()


def test_stores_rejects_invalid_manual_inbox_metadata_and_rows(tmp_path: Path) -> None:
    inbox = tmp_path / "inbox"
    row = ["1", "Quito", "Pichincha", "D", "13"]
    wrong_day = _path(inbox, "stores", "2013-01-02", "2013-01-02T000000Z")
    _write_csv(wrong_day, STORES_COLUMNS, [row])
    with pytest.raises(ValueError, match="observation_date"):
        ingest_stores(inbox, T0 + timedelta(days=3))
    inbox = tmp_path / "early_inbox"
    early = _path(inbox, "stores", "2013-01-01", "2012-12-31T235959Z")
    _write_csv(early, STORES_COLUMNS, [row])
    with pytest.raises(ValueError, match="Disponibilidad"):
        ingest_stores(inbox, T0 - timedelta(seconds=1))
    inbox = tmp_path / "duplicate_inbox"
    valid_path = _path(inbox, "stores", "2013-01-01", "2013-01-01T000000Z")
    _write_csv(valid_path, STORES_COLUMNS, [row, row])
    with pytest.raises(ValueError, match="duplicado"):
        ingest_stores(inbox, T0)


def test_stores_original_conflict_and_revision_validation(tmp_path: Path) -> None:
    raw = tmp_path / "stores.csv"
    inbox = tmp_path / "inbox"
    _write_csv(raw, STORES_COLUMNS, [["1", "Quito", "Pichincha", "D", "13"]])
    original = simulate_stores_delivery(raw, inbox)
    before = original.read_bytes()
    _write_csv(raw, STORES_COLUMNS, [["1", "Quito", "Pichincha", "A", "13"]])
    with pytest.raises(FileExistsError, match="difiere"):
        simulate_stores_delivery(raw, inbox)
    with pytest.raises(ValueError, match="T0"):
        simulate_stores_delivery(raw, inbox, revision=2, available_at=T0 - timedelta(seconds=1))
    with pytest.raises(ValueError, match="corrección"):
        simulate_stores_delivery(raw, inbox, revision=2)
    assert original.read_bytes() == before


def test_holidays_keep_all_same_day_events_and_geographic_scope(tmp_path: Path) -> None:
    raw = tmp_path / "holidays_events.csv"
    inbox = tmp_path / "inbox"
    rows = [
        ["2017-08-15", "Holiday", "Local", "Quito", "Fundacion", "False"],
        ["2017-08-15", "Holiday", "Regional", "Pichincha", "Provincializacion", "True"],
        ["2017-08-15", "Event", "National", "Ecuador", "Festival, nacional", "False"],
        ["2017-08-14", "Holiday", "National", "Ecuador", "Dia anterior", "False"],
    ]
    _write_csv(raw, HOLIDAYS_COLUMNS, rows)
    raw_bytes = raw.read_bytes()
    path = simulate_holidays_delivery("2017-08-15", raw, inbox)

    assert path == _path(inbox, "holidays_events", "2017-08-15", "2017-08-16T060000Z")
    assert simulate_holidays_delivery("2017-08-15", raw, inbox) == path
    assert raw.read_bytes() == raw_bytes
    with path.open(newline="", encoding="utf-8") as batch:
        assert list(csv.reader(batch)) == [list(HOLIDAYS_COLUMNS), *rows[:3]]
    published_bytes = path.read_bytes()
    before = datetime(2017, 8, 16, 5, 59, 59, tzinfo=timezone.utc)
    at = datetime(2017, 8, 16, 6, tzinfo=timezone.utc)
    assert ingest_holidays(inbox, before) == []
    events = ingest_holidays(inbox, at)
    assert len(events) == 3
    assert {event.locale for event in events} == {"Local", "Regional", "National"}
    assert {event.locale_name for event in events} == {"Quito", "Pichincha", "Ecuador"}
    assert sum(event.transferred for event in events) == 1
    assert HolidayEventRecord(date(2017, 8, 15), "Event", "National", "Ecuador", "Festival, nacional", False) in events
    assert ingest_holidays(inbox, at) == events
    assert path.read_bytes() == published_bytes
    assert not (tmp_path / "processed").exists()


def test_holidays_correction_replaces_complete_day_and_preserves_old_cut(tmp_path: Path) -> None:
    raw = tmp_path / "holidays_events.csv"
    inbox = tmp_path / "inbox"
    first = ["2017-08-15", "Holiday", "Local", "Quito", "Fundacion", "False"]
    second = ["2017-08-15", "Event", "National", "Ecuador", "Festival", "False"]
    _write_csv(raw, HOLIDAYS_COLUMNS, [first, second])
    original = simulate_holidays_delivery("2017-08-15", raw, inbox)
    original_bytes = original.read_bytes()
    early_cut = datetime(2017, 8, 16, 6, tzinfo=timezone.utc)
    early_events = ingest_holidays(inbox, early_cut)

    _write_csv(raw, HOLIDAYS_COLUMNS, [["2017-08-15", "Transfer", "Local", "Quito", "Fundacion", "False"]])
    corrected_cut = datetime(2017, 8, 18, 6, tzinfo=timezone.utc)
    correction = simulate_holidays_delivery("2017-08-15", raw, inbox, revision=2, available_at=corrected_cut)

    assert simulate_holidays_delivery("2017-08-15", raw, inbox, revision=2, available_at=corrected_cut) == correction
    assert ingest_holidays(inbox, early_cut) == early_events
    assert ingest_holidays(inbox, corrected_cut - timedelta(seconds=1)) == early_events
    assert ingest_holidays(inbox, corrected_cut) == [HolidayEventRecord(date(2017, 8, 15), "Transfer", "Local", "Quito", "Fundacion", False)]
    assert original.read_bytes() == original_bytes
    assert set(inbox.rglob("data.csv")) == {original, correction}


@pytest.mark.parametrize(
    "columns,row",
    [
        (("type", "date", "locale", "locale_name", "description", "transferred"), ["2017-08-15", "Holiday", "Local", "Quito", "Dia", "False"]),
        (HOLIDAYS_COLUMNS, ["2017-02-30", "Holiday", "Local", "Quito", "Dia", "False"]),
        (HOLIDAYS_COLUMNS, ["2017-08-15", "Unknown", "Local", "Quito", "Dia", "False"]),
        (HOLIDAYS_COLUMNS, ["2017-08-15", "Holiday", "Unknown", "Quito", "Dia", "False"]),
        (HOLIDAYS_COLUMNS, ["2017-08-15", "Holiday", "Local", "", "Dia", "False"]),
        (HOLIDAYS_COLUMNS, ["2017-08-15", "Holiday", "Local", "Quito", "", "False"]),
        (HOLIDAYS_COLUMNS, ["2017-08-15", "Holiday", "Local", "Quito", "Dia", "false"]),
    ],
)
def test_holidays_rejects_invalid_raw_and_manual_batch(tmp_path: Path, columns, row) -> None:
    raw = tmp_path / "holidays_events.csv"
    inbox = tmp_path / "inbox"
    _write_csv(raw, columns, [row])
    with pytest.raises(ValueError):
        simulate_holidays_delivery("2017-08-15", raw, inbox)
    assert not inbox.exists()

    path = _path(inbox, "holidays_events", "2017-08-15", "2017-08-16T060000Z")
    _write_csv(path, columns, [row])
    with pytest.raises(ValueError):
        ingest_holidays(inbox, datetime(2017, 8, 16, 6, tzinfo=timezone.utc))


def test_holidays_rejects_duplicates_and_premature_deliveries(tmp_path: Path) -> None:
    raw = tmp_path / "holidays_events.csv"
    inbox = tmp_path / "inbox"
    row = ["2017-08-15", "Holiday", "Local", "Quito", "Dia", "False"]
    _write_csv(raw, HOLIDAYS_COLUMNS, [row, row])
    with pytest.raises(ValueError, match="duplicada"):
        simulate_holidays_delivery("2017-08-15", raw, inbox)
    assert not inbox.exists()

    path = _path(inbox, "holidays_events", "2017-08-15", "2017-08-16T055959Z")
    _write_csv(path, HOLIDAYS_COLUMNS, [row])
    with pytest.raises(ValueError, match="Entrega anticipada"):
        ingest_holidays(inbox, datetime(2017, 8, 16, 5, tzinfo=timezone.utc))
    inbox = tmp_path / "duplicate_inbox"
    valid = _path(inbox, "holidays_events", "2017-08-15", "2017-08-16T060000Z")
    _write_csv(valid, HOLIDAYS_COLUMNS, [row, row])
    with pytest.raises(ValueError, match="duplicada"):
        ingest_holidays(inbox, datetime(2017, 8, 16, 6, tzinfo=timezone.utc))


def test_holidays_missing_date_and_inconsistent_manual_date(tmp_path: Path) -> None:
    raw = tmp_path / "holidays_events.csv"
    inbox = tmp_path / "inbox"
    row = ["2017-08-15", "Holiday", "Local", "Quito", "Dia", "False"]
    _write_csv(raw, HOLIDAYS_COLUMNS, [row])
    assert simulate_holidays_delivery("2017-08-14", raw, inbox) is None
    assert not inbox.exists()
    inconsistent = _path(inbox, "holidays_events", "2017-08-14", "2017-08-15T060000Z")
    _write_csv(inconsistent, HOLIDAYS_COLUMNS, [row])
    with pytest.raises(ValueError, match="no coincide con la ruta"):
        ingest_holidays(inbox, datetime(2017, 8, 15, 6, tzinfo=timezone.utc))
