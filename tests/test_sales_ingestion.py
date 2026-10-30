import csv
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from inventory.sales_ingestion import ingest_sales
from inventory.train_schema import SaleRecord, TRAIN_COLUMNS


def cutoff(year: int, month: int, day: int, hour: int) -> datetime:
    return datetime(year, month, day, hour, tzinfo=timezone.utc)


def delivery(
    inbox: Path,
    observation_date: str,
    available_at: str,
    rows: list[list[str]],
    revision: str = "0001",
    header: tuple[str, ...] = TRAIN_COLUMNS,
    source: str = "train",
) -> Path:
    path = (
        inbox
        / f"source={source}"
        / f"observation_date={observation_date}"
        / f"available_at={available_at}"
        / f"revision={revision}"
        / f"delivery_id={source}-{observation_date}-r{revision}"
        / "data.csv"
    )
    path.parent.mkdir(parents=True)
    with path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.writer(output)
        writer.writerow(header)
        writer.writerows(rows)
    return path


def test_multiple_dates_are_typed_sorted_and_repeatable(tmp_path: Path) -> None:
    inbox = tmp_path / "inbox"
    delivery(
        inbox,
        "2017-08-15",
        "2017-08-16T060000Z",
        [
            ["3", "2017-08-15", "2", "GROCERY, I", "4.0000", "0"],
            ["2", "2017-08-15", "1", "AUTOMOTIVE", "3.0", "1"],
        ],
    )
    delivery(
        inbox,
        "2017-08-14",
        "2017-08-15T060000Z",
        [["1", "2017-08-14", "1", "AUTOMOTIVE", "2.0", "0"]],
    )
    delivery(inbox, "2017-08-15", "2017-08-16T060000Z", [], header=("dcoilwtico",), source="oil")
    before = {path: path.read_bytes() for path in inbox.rglob("data.csv")}

    expected = [
        SaleRecord(1, date(2017, 8, 14), 1, "AUTOMOTIVE", Decimal("2.0"), 0),
        SaleRecord(2, date(2017, 8, 15), 1, "AUTOMOTIVE", Decimal("3.0"), 1),
        SaleRecord(3, date(2017, 8, 15), 2, "GROCERY, I", Decimal("4.0000"), 0),
    ]
    assert ingest_sales(inbox, cutoff(2017, 8, 16, 6)) == expected
    assert ingest_sales(inbox, cutoff(2017, 8, 16, 6)) == expected
    assert {path: path.read_bytes() for path in inbox.rglob("data.csv")} == before
    assert not (tmp_path / "processed").exists()


def test_correction_replaces_complete_batch_at_its_cutoff(tmp_path: Path) -> None:
    inbox = tmp_path / "inbox"
    delivery(
        inbox,
        "2017-08-15",
        "2017-08-16T060000Z",
        [
            ["1", "2017-08-15", "1", "AUTOMOTIVE", "2.0", "0"],
            ["2", "2017-08-15", "2", "GROCERY I", "3.0", "0"],
        ],
    )
    delivery(
        inbox,
        "2017-08-15",
        "2017-08-20T060000Z",
        [["1", "2017-08-15", "1", "AUTOMOTIVE", "9.0", "0"]],
        revision="0002",
    )

    assert ingest_sales(inbox, cutoff(2017, 8, 16, 5)) == []
    assert [sale.id for sale in ingest_sales(inbox, cutoff(2017, 8, 20, 5))] == [1, 2]
    corrected = ingest_sales(inbox, cutoff(2017, 8, 20, 6))
    assert corrected == [SaleRecord(1, date(2017, 8, 15), 1, "AUTOMOTIVE", Decimal("9.0"), 0)]


def test_empty_correction_removes_all_rows_for_its_date(tmp_path: Path) -> None:
    inbox = tmp_path / "inbox"
    delivery(inbox, "2017-08-15", "2017-08-16T060000Z", [["1", "2017-08-15", "1", "A", "2", "0"]])
    delivery(inbox, "2017-08-15", "2017-08-20T060000Z", [], revision="0002")

    assert len(ingest_sales(inbox, cutoff(2017, 8, 20, 5))) == 1
    assert ingest_sales(inbox, cutoff(2017, 8, 20, 6)) == []


@pytest.mark.parametrize(
    "header,rows",
    [
        (("date", "id", "store_nbr", "family", "sales", "onpromotion"), []),
        (TRAIN_COLUMNS, [["x", "2017-08-15", "1", "A", "2.0", "0"]]),
        (TRAIN_COLUMNS, [["1", "2017-02-30", "1", "A", "2.0", "0"]]),
        (TRAIN_COLUMNS, [["1", "2017-08-15", "0", "A", "2.0", "0"]]),
        (TRAIN_COLUMNS, [["1", "2017-08-15", "1", " ", "2.0", "0"]]),
        (TRAIN_COLUMNS, [["1", "2017-08-15", "1", "A", "NaN", "0"]]),
        (TRAIN_COLUMNS, [["1", "2017-08-15", "1", "A", "2.0", "-1"]]),
        (TRAIN_COLUMNS, [["1", "2017-08-15", "1", "A", "2.0"]]),
    ],
)
def test_invalid_selected_csv_is_rejected(
    tmp_path: Path, header: tuple[str, ...], rows: list[list[str]]
) -> None:
    inbox = tmp_path / "inbox"
    delivery(inbox, "2017-08-15", "2017-08-16T060000Z", rows, header=header)

    with pytest.raises(ValueError):
        ingest_sales(inbox, cutoff(2017, 8, 16, 6))


def test_row_date_must_match_delivery_date(tmp_path: Path) -> None:
    inbox = tmp_path / "inbox"
    delivery(inbox, "2017-08-15", "2017-08-16T060000Z", [["1", "2017-08-14", "1", "A", "2", "0"]])

    with pytest.raises(ValueError, match="no coincide con la ruta"):
        ingest_sales(inbox, cutoff(2017, 8, 16, 6))


def test_duplicate_sales_key_is_rejected(tmp_path: Path) -> None:
    inbox = tmp_path / "inbox"
    delivery(
        inbox,
        "2017-08-15",
        "2017-08-16T060000Z",
        [
            ["1", "2017-08-15", "1", "A", "2", "0"],
            ["2", "2017-08-15", "01", "A", "3", "0"],
        ],
    )

    with pytest.raises(ValueError, match="duplicada"):
        ingest_sales(inbox, cutoff(2017, 8, 16, 6))


def test_unselected_correction_is_not_read(tmp_path: Path) -> None:
    inbox = tmp_path / "inbox"
    delivery(inbox, "2017-08-15", "2017-08-16T060000Z", [["1", "2017-08-15", "1", "A", "2", "0"]])
    delivery(
        inbox,
        "2017-08-15",
        "2017-08-20T060000Z",
        [["bad", "2017-08-15", "1", "A", "2", "0"]],
        revision="0002",
    )

    assert len(ingest_sales(inbox, cutoff(2017, 8, 20, 5))) == 1
    with pytest.raises(ValueError):
        ingest_sales(inbox, cutoff(2017, 8, 20, 6))


@pytest.mark.parametrize(
    "historical_cutoff",
    [cutoff(2017, 8, 15, 12), cutoff(2017, 8, 16, 6)],
)
def test_premature_original_is_rejected_even_before_its_claimed_arrival(
    tmp_path: Path, historical_cutoff: datetime
) -> None:
    inbox = tmp_path / "inbox"
    delivery(
        inbox,
        "2017-08-15",
        "2017-08-16T055959Z",
        [["1", "2017-08-15", "1", "A", "2", "0"]],
    )

    with pytest.raises(ValueError, match="Entrega anticipada de train"):
        ingest_sales(inbox, historical_cutoff)


def test_premature_correction_is_rejected_when_original_would_win(tmp_path: Path) -> None:
    inbox = tmp_path / "inbox"
    delivery(inbox, "2017-08-15", "2017-08-16T060000Z", [["1", "2017-08-15", "1", "A", "2", "0"]])
    delivery(
        inbox,
        "2017-08-15",
        "2017-08-16T055959Z",
        [["1", "2017-08-15", "1", "A", "9", "0"]],
        revision="0002",
    )

    with pytest.raises(ValueError, match="Entrega anticipada de train"):
        ingest_sales(inbox, cutoff(2017, 8, 16, 6))


def test_valid_boundary_and_later_correction_preserve_historical_cuts(tmp_path: Path) -> None:
    inbox = tmp_path / "inbox"
    delivery(inbox, "2017-08-15", "2017-08-16T060000Z", [["1", "2017-08-15", "1", "A", "2", "0"]])
    delivery(
        inbox,
        "2017-08-15",
        "2017-08-17T060000Z",
        [["1", "2017-08-15", "1", "A", "9", "0"]],
        revision="0002",
    )

    assert ingest_sales(inbox, cutoff(2017, 8, 16, 5)) == []
    assert ingest_sales(inbox, cutoff(2017, 8, 16, 6))[0].sales == Decimal("2")
    assert ingest_sales(inbox, cutoff(2017, 8, 17, 6))[0].sales == Decimal("9")
    assert ingest_sales(inbox, cutoff(2017, 8, 16, 6))[0].sales == Decimal("2")
