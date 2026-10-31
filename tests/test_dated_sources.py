"""Synthetic tests for transaction and oil delivery and historical ingestion."""

import csv
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

import inventory.dated_source as dated_source
from inventory.oil import OIL_COLUMNS, OilRecord, ingest_oil, simulate_oil_delivery
from inventory.transactions import (
    TRANSACTIONS_COLUMNS,
    TransactionRecord,
    ingest_transactions,
    simulate_transactions_delivery,
)


SOURCES = (
    ("transactions", TRANSACTIONS_COLUMNS, simulate_transactions_delivery, ingest_transactions),
    ("oil", OIL_COLUMNS, simulate_oil_delivery, ingest_oil),
)


def _write_csv(path: Path, header: tuple[str, ...], rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.writer(output)
        writer.writerow(header)
        writer.writerows(rows)


def _inbox_path(inbox: Path, source: str, day: str, available: str, revision: int = 1) -> Path:
    return (
        inbox / f"source={source}" / f"observation_date={day}"
        / f"available_at={available}" / f"revision={revision:04d}"
        / f"delivery_id={source}-{day}-r{revision:04d}" / "data.csv"
    )


def _at(day: int, hour: int = 6) -> datetime:
    return datetime(2017, 8, day, hour, tzinfo=timezone.utc)


@pytest.mark.parametrize("source,columns,simulate,ingest", SOURCES)
def test_one_date_cutoff_and_idempotence(tmp_path, source, columns, simulate, ingest) -> None:
    raw = tmp_path / "raw.csv"
    inbox = tmp_path / "inbox"
    if source == "transactions":
        rows = [["2017-08-14", "1", "9"], ["2017-08-15", "2", "4"], ["2017-08-15", "1", "0"]]
        expected = [
            TransactionRecord(date(2017, 8, 15), 1, 0),
            TransactionRecord(date(2017, 8, 15), 2, 4),
        ]
    else:
        rows = [["2017-08-14", "92.00"], ["2017-08-15", ""]]
        expected = [OilRecord(date(2017, 8, 15), None)]
    _write_csv(raw, columns, rows)
    before_raw = raw.read_bytes()

    path = simulate("2017-08-15", raw, inbox)

    assert path == _inbox_path(inbox, source, "2017-08-15", "2017-08-16T060000Z")
    assert simulate("2017-08-15", raw, inbox) == path
    assert list(inbox.rglob("data.csv")) == [path]
    assert raw.read_bytes() == before_raw
    with path.open(newline="", encoding="utf-8") as batch:
        batch_rows = list(csv.reader(batch))
    assert batch_rows == [list(columns)] + [row for row in rows if row[0] == "2017-08-15"]
    before_inbox = path.read_bytes()

    assert ingest(inbox, _at(16, 5)) == []
    assert ingest(inbox, _at(16)) == expected
    assert ingest(inbox, _at(16)) == expected
    assert path.read_bytes() == before_inbox
    assert not (tmp_path / "processed").exists()


@pytest.mark.parametrize("source,columns,simulate,ingest", SOURCES)
def test_correction_replaces_full_batch_without_changing_old_cut(tmp_path, source, columns, simulate, ingest) -> None:
    raw = tmp_path / "raw.csv"
    inbox = tmp_path / "inbox"
    original = [["2017-08-15", "1", "7"], ["2017-08-15", "2", "8"]] if source == "transactions" else [["2017-08-15", "93.140"]]
    corrected = [["2017-08-15", "1", "11"]] if source == "transactions" else [["2017-08-15", ""]]
    _write_csv(raw, columns, original)
    original_path = simulate("2017-08-15", raw, inbox)
    original_bytes = original_path.read_bytes()
    original_records = ingest(inbox, _at(16))

    _write_csv(raw, columns, corrected)
    correction_path = simulate("2017-08-15", raw, inbox, revision=2, available_at=_at(18))

    assert correction_path == _inbox_path(inbox, source, "2017-08-15", "2017-08-18T060000Z", 2)
    assert simulate("2017-08-15", raw, inbox, revision=2, available_at=_at(18)) == correction_path
    assert ingest(inbox, _at(18, 5)) == original_records
    expected = ([TransactionRecord(date(2017, 8, 15), 1, 11)] if source == "transactions"
                else [OilRecord(date(2017, 8, 15), None)])
    assert ingest(inbox, _at(18)) == expected
    assert ingest(inbox, _at(16)) == original_records
    assert ingest(inbox, _at(18)) == expected
    assert original_path.read_bytes() == original_bytes
    assert set(inbox.rglob("data.csv")) == {original_path, correction_path}


@pytest.mark.parametrize("source,columns,simulate,ingest", SOURCES)
def test_missing_date_and_empty_correction(tmp_path, source, columns, simulate, ingest) -> None:
    raw = tmp_path / "raw.csv"
    inbox = tmp_path / "inbox"
    row = ["2017-08-15", "1", "7"] if source == "transactions" else ["2017-08-15", "91"]
    _write_csv(raw, columns, [row])
    assert simulate("2017-08-14", raw, inbox) is None
    assert not inbox.exists()
    simulate("2017-08-15", raw, inbox)
    _write_csv(raw, columns, [])
    correction = simulate("2017-08-15", raw, inbox, revision=2, available_at=_at(18))
    assert correction.is_file()
    assert len(ingest(inbox, _at(16))) == 1
    assert ingest(inbox, _at(18)) == []


@pytest.mark.parametrize("source,columns,simulate,ingest", SOURCES)
def test_conflicting_revision_and_premature_manual_delivery(tmp_path, source, columns, simulate, ingest) -> None:
    raw = tmp_path / "raw.csv"
    inbox = tmp_path / "inbox"
    row = ["2017-08-15", "1", "7"] if source == "transactions" else ["2017-08-15", "91"]
    _write_csv(raw, columns, [row])
    path = simulate("2017-08-15", raw, inbox)
    original = path.read_bytes()
    changed = ["2017-08-15", "1", "8"] if source == "transactions" else ["2017-08-15", "92"]
    _write_csv(raw, columns, [changed])
    with pytest.raises(FileExistsError, match="difiere"):
        simulate("2017-08-15", raw, inbox)
    assert path.read_bytes() == original

    premature = _inbox_path(inbox, source, "2017-08-15", "2017-08-16T055959Z", 2)
    _write_csv(premature, columns, [changed])
    with pytest.raises(ValueError, match="Entrega anticipada"):
        ingest(inbox, _at(16))


@pytest.mark.parametrize("source,columns,simulate,ingest", SOURCES)
def test_invalid_dates_schema_and_duplicate_keys(tmp_path, source, columns, simulate, ingest) -> None:
    raw = tmp_path / "raw.csv"
    inbox = tmp_path / "inbox"
    row = ["2017-08-15", "1", "7"] if source == "transactions" else ["2017-08-15", "91"]
    _write_csv(raw, columns, [row])
    for invalid in ("2017-02-30", "2017-8-15", datetime(2017, 8, 15)):
        with pytest.raises(ValueError):
            simulate(invalid, raw, inbox)
    assert not inbox.exists()
    _write_csv(raw, tuple(reversed(columns)), [row])
    with pytest.raises(ValueError, match="columnas"):
        simulate("2017-08-15", raw, inbox)
    assert not inbox.exists()

    _write_csv(raw, columns, [row, row])
    with pytest.raises(ValueError, match="clave duplicada"):
        simulate("2017-08-15", raw, inbox)
    assert not inbox.exists()

    duplicate_path = _inbox_path(inbox, source, "2017-08-15", "2017-08-16T060000Z")
    _write_csv(duplicate_path, columns, [row, row])
    with pytest.raises(ValueError, match="clave duplicada"):
        ingest(inbox, _at(16))


@pytest.mark.parametrize(
    "source,columns,rows",
    [
        ("transactions", TRANSACTIONS_COLUMNS, [["2017-02-30", "1", "7"]]),
        ("transactions", TRANSACTIONS_COLUMNS, [["2017-08-15", "0", "7"]]),
        ("transactions", TRANSACTIONS_COLUMNS, [["2017-08-15", "1", ""]]),
        ("transactions", TRANSACTIONS_COLUMNS, [["2017-08-15", "1", "-1"]]),
        ("transactions", TRANSACTIONS_COLUMNS, [["2017-08-15", "1", "1.5"]]),
        ("oil", OIL_COLUMNS, [["2017-02-30", "91"]]),
        ("oil", OIL_COLUMNS, [["2017-08-15", "NaN"]]),
        ("oil", OIL_COLUMNS, [["2017-08-15", "Infinity"]]),
        ("oil", OIL_COLUMNS, [["2017-08-15", "oops"]]),
        ("oil", OIL_COLUMNS, [["2017-08-15", "-1"]]),
    ],
)
def test_invalid_rows_rejected_by_producer_and_ingester(tmp_path, source, columns, rows) -> None:
    raw = tmp_path / "raw.csv"
    inbox = tmp_path / "inbox"
    simulate, ingest = (simulate_transactions_delivery, ingest_transactions) if source == "transactions" else (simulate_oil_delivery, ingest_oil)
    _write_csv(raw, columns, rows)
    with pytest.raises(ValueError, match="Fila"):
        simulate("2017-08-15", raw, inbox)
    assert not inbox.exists()

    path = _inbox_path(inbox, source, "2017-08-15", "2017-08-16T060000Z")
    _write_csv(path, columns, rows)
    with pytest.raises(ValueError, match="Fila"):
        ingest(inbox, _at(16))


@pytest.mark.parametrize("source,columns,simulate,ingest", SOURCES)
def test_row_date_must_match_path(tmp_path, source, columns, simulate, ingest) -> None:
    inbox = tmp_path / "inbox"
    row = ["2017-08-14", "1", "7"] if source == "transactions" else ["2017-08-14", "91"]
    path = _inbox_path(inbox, source, "2017-08-15", "2017-08-16T060000Z")
    _write_csv(path, columns, [row])
    with pytest.raises(ValueError, match="no coincide con la ruta"):
        ingest(inbox, _at(16))


@pytest.mark.parametrize("source,columns,simulate,ingest", SOURCES)
def test_invalid_revision_availability_and_atomic_retry(tmp_path, monkeypatch, source, columns, simulate, ingest) -> None:
    raw = tmp_path / "raw.csv"
    inbox = tmp_path / "inbox"
    row = ["2017-08-15", "1", "7"] if source == "transactions" else ["2017-08-15", "91"]
    _write_csv(raw, columns, [row])
    with pytest.raises(ValueError, match="corrección"):
        simulate("2017-08-15", raw, inbox, revision=2)
    with pytest.raises(ValueError, match=r"D\+1"):
        simulate("2017-08-15", raw, inbox, revision=2, available_at=_at(16, 5))
    with pytest.raises(ValueError, match="revision"):
        simulate("2017-08-15", raw, inbox, revision=0)
    assert not inbox.exists()

    def fail_rename(source_path, target_path):
        raise OSError("publication failed")

    with monkeypatch.context() as patcher:
        patcher.setattr(dated_source.os, "rename", fail_rename)
        with pytest.raises(OSError, match="publication failed"):
            simulate("2017-08-15", raw, inbox)
    assert not list(inbox.rglob(".staging-*"))
    assert not list(inbox.rglob("delivery_id=*"))
    path = simulate("2017-08-15", raw, inbox)
    assert simulate("2017-08-15", raw, inbox) == path
    assert len(ingest(inbox, _at(16))) == 1


def test_oil_keeps_missing_and_decimal_values_across_dates(tmp_path: Path) -> None:
    raw = tmp_path / "raw.csv"
    inbox = tmp_path / "inbox"
    _write_csv(raw, OIL_COLUMNS, [["2017-08-14", ""], ["2017-08-15", "93.140"]])
    simulate_oil_delivery("2017-08-14", raw, inbox)
    simulate_oil_delivery("2017-08-15", raw, inbox)

    assert ingest_oil(inbox, _at(15)) == [OilRecord(date(2017, 8, 14), None)]
    assert ingest_oil(inbox, _at(16)) == [
        OilRecord(date(2017, 8, 14), None),
        OilRecord(date(2017, 8, 15), Decimal("93.140")),
    ]


@pytest.mark.parametrize("source,columns,simulate,ingest", SOURCES)
def test_selected_wrong_schema_and_revision_conflict(tmp_path, source, columns, simulate, ingest) -> None:
    inbox = tmp_path / "inbox"
    path = _inbox_path(inbox, source, "2017-08-15", "2017-08-16T060000Z")
    row = ["2017-08-15", "1", "7"] if source == "transactions" else ["2017-08-15", "91"]
    _write_csv(path, tuple(reversed(columns)), [row])
    with pytest.raises(ValueError, match="columnas"):
        ingest(inbox, _at(16))

    _write_csv(path, columns, [row])
    raw = tmp_path / "raw.csv"
    _write_csv(raw, columns, [row])
    correction = _inbox_path(inbox, source, "2017-08-15", "2017-08-18T060000Z", 2)
    _write_csv(correction, columns, [row])
    with pytest.raises(FileExistsError, match="revisión"):
        simulate("2017-08-15", raw, inbox, revision=2, available_at=_at(19))


@pytest.mark.parametrize("source,columns,simulate,ingest", SOURCES)
def test_new_revision_must_arrive_after_last_revision(tmp_path, source, columns, simulate, ingest) -> None:
    raw = tmp_path / "raw.csv"
    inbox = tmp_path / "inbox"
    values = (
        [["2017-08-15", "1", "7"], ["2017-08-15", "1", "8"], ["2017-08-15", "1", "9"]]
        if source == "transactions"
        else [["2017-08-15", "91"], ["2017-08-15", "92"], ["2017-08-15", "93"]]
    )
    _write_csv(raw, columns, [values[0]])
    original = simulate("2017-08-15", raw, inbox)
    _write_csv(raw, columns, [values[1]])
    correction = simulate("2017-08-15", raw, inbox, revision=2, available_at=_at(20))
    before_20 = ingest(inbox, _at(19))
    at_20 = ingest(inbox, _at(20))

    _write_csv(raw, columns, [values[2]])
    for day in (19, 20):
        with pytest.raises(ValueError, match="posterior"):
            simulate("2017-08-15", raw, inbox, revision=3, available_at=_at(day))
    assert set(inbox.rglob("data.csv")) == {original, correction}
    assert ingest(inbox, _at(19)) == before_20
    assert ingest(inbox, _at(20)) == at_20

    next_correction = simulate("2017-08-15", raw, inbox, revision=3, available_at=_at(21))
    assert next_correction.is_file()
    assert ingest(inbox, _at(20)) == at_20
    assert ingest(inbox, _at(21)) != at_20
    _write_csv(raw, columns, [values[1]])
    assert simulate("2017-08-15", raw, inbox, revision=2, available_at=_at(20)) == correction


@pytest.mark.parametrize("source,columns,simulate,ingest", SOURCES)
def test_retry_without_published_date_is_a_conflict(tmp_path, source, columns, simulate, ingest) -> None:
    raw = tmp_path / "raw.csv"
    inbox = tmp_path / "inbox"
    row = ["2017-08-15", "1", "7"] if source == "transactions" else ["2017-08-15", "91"]
    _write_csv(raw, columns, [row])
    original = simulate("2017-08-15", raw, inbox)
    before = original.read_bytes()

    _write_csv(raw, columns, [])
    with pytest.raises(FileExistsError, match="ya tiene una entrega publicada"):
        simulate("2017-08-15", raw, inbox)

    assert original.read_bytes() == before
    assert list(inbox.rglob("data.csv")) == [original]
    assert len(ingest(inbox, _at(16))) == 1
