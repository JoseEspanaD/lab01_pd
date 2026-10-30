import csv
from datetime import datetime
from pathlib import Path

import pytest

import inventory.train_delivery as train_delivery
from inventory.train_delivery import simulate_train_delivery


def write_train(path: Path, rows: list[list[str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["id", "date", "store_nbr", "family", "sales", "onpromotion"])
        writer.writerows(rows)


def test_creates_only_requested_date_and_repeating_is_idempotent(tmp_path: Path) -> None:
    raw = tmp_path / "train.csv"
    inbox = tmp_path / "inbox"
    write_train(
        raw,
        [
            ["1", "2017-08-14", "1", "AUTOMOTIVE", "2.0", "0"],
            ["2", "2017-08-15", "1", "AUTOMOTIVE", "3.0", "1"],
            ["3", "2017-08-15", "2", "GROCERY, I", "4.0000", "0"],
        ],
    )
    original = raw.read_bytes()

    delivery = simulate_train_delivery("2017-08-15", raw, inbox)

    assert delivery == (
        inbox
        / "source=train/observation_date=2017-08-15"
        / "available_at=2017-08-16T060000Z/revision=0001"
        / "delivery_id=train-2017-08-15-r0001/data.csv"
    )
    with delivery.open(newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        assert reader.fieldnames == ["id", "date", "store_nbr", "family", "sales", "onpromotion"]
        rows = list(reader)
    assert [row["id"] for row in rows] == ["2", "3"]
    assert rows[1] == {
        "id": "3",
        "date": "2017-08-15",
        "store_nbr": "2",
        "family": "GROCERY, I",
        "sales": "4.0000",
        "onpromotion": "0",
    }
    assert raw.read_bytes() == original

    assert simulate_train_delivery("2017-08-15", raw, inbox) == delivery
    assert list(inbox.rglob("data.csv")) == [delivery]


def test_date_without_rows_creates_no_delivery(tmp_path: Path) -> None:
    raw = tmp_path / "train.csv"
    inbox = tmp_path / "inbox"
    write_train(raw, [["1", "2017-08-14", "1", "AUTOMOTIVE", "2.0", "0"]])

    assert simulate_train_delivery("2017-08-15", raw, inbox) is None
    assert not inbox.exists()


def test_conflicting_existing_delivery_is_not_overwritten(tmp_path: Path) -> None:
    raw = tmp_path / "train.csv"
    inbox = tmp_path / "inbox"
    write_train(raw, [["1", "2017-08-15", "1", "AUTOMOTIVE", "2.0", "0"]])
    delivery = simulate_train_delivery("2017-08-15", raw, inbox)
    original = delivery.read_bytes()
    write_train(raw, [["1", "2017-08-15", "1", "AUTOMOTIVE", "9.0", "0"]])

    with pytest.raises(FileExistsError, match="difiere"):
        simulate_train_delivery("2017-08-15", raw, inbox)
    assert delivery.read_bytes() == original


def test_duplicate_delivery_id_is_rejected(tmp_path: Path) -> None:
    raw = tmp_path / "train.csv"
    inbox = tmp_path / "inbox"
    write_train(raw, [["1", "2017-08-15", "1", "AUTOMOTIVE", "2.0", "0"]])
    duplicate = inbox / "source=other/observation_date=2017-08-15/available_at=x/revision=0001"
    duplicate = duplicate / "delivery_id=train-2017-08-15-r0001"
    duplicate.mkdir(parents=True)

    with pytest.raises(FileExistsError, match="duplicado"):
        simulate_train_delivery("2017-08-15", raw, inbox)
    assert not list((inbox / "source=train").rglob("data.csv"))


@pytest.mark.parametrize("invalid", ["2017-02-30", "2017-8-15", datetime(2017, 8, 15)])
def test_invalid_observation_date_is_rejected(tmp_path: Path, invalid: object) -> None:
    raw = tmp_path / "train.csv"
    inbox = tmp_path / "inbox"
    write_train(raw, [["1", "2017-08-15", "1", "AUTOMOTIVE", "2.0", "0"]])

    with pytest.raises(ValueError, match="observation_date"):
        simulate_train_delivery(invalid, raw, inbox)
    assert not inbox.exists()


def test_wrong_schema_is_rejected(tmp_path: Path) -> None:
    raw = tmp_path / "train.csv"
    inbox = tmp_path / "inbox"
    raw.write_text("date,id,store_nbr,family,sales,onpromotion\n", encoding="utf-8")

    with pytest.raises(ValueError, match="seis columnas"):
        simulate_train_delivery("2017-08-15", raw, inbox)
    assert not inbox.exists()


@pytest.mark.parametrize(
    "row",
    [
        ["-1", "2017-08-15", "1", "AUTOMOTIVE", "2.0", "0"],
        ["1", "2017-02-30", "1", "AUTOMOTIVE", "2.0", "0"],
        ["1", "2017-08-15", "0", "AUTOMOTIVE", "2.0", "0"],
        ["1", "2017-08-15", "1", " ", "2.0", "0"],
        ["1", "2017-08-15", "1", "AUTOMOTIVE", "NaN", "0"],
        ["1", "2017-08-15", "1", "AUTOMOTIVE", "-2.0", "0"],
        ["1", "2017-08-15", "1", "AUTOMOTIVE", "2.0", "-1"],
        ["1", "2017-08-15", "1", "AUTOMOTIVE", "2.0"],
    ],
)
def test_invalid_record_is_rejected(tmp_path: Path, row: list[str]) -> None:
    raw = tmp_path / "train.csv"
    inbox = tmp_path / "inbox"
    write_train(raw, [row])

    with pytest.raises(ValueError, match="Fila"):
        simulate_train_delivery("2017-08-15", raw, inbox)
    assert not inbox.exists()


def test_duplicate_observation_key_is_rejected(tmp_path: Path) -> None:
    raw = tmp_path / "train.csv"
    inbox = tmp_path / "inbox"
    write_train(
        raw,
        [
            ["1", "2017-08-15", "1", "AUTOMOTIVE", "2.0", "0"],
            ["2", "2017-08-15", "01", "AUTOMOTIVE", "3.0", "0"],
        ],
    )

    with pytest.raises(ValueError, match="clave duplicada"):
        simulate_train_delivery("2017-08-15", raw, inbox)
    assert not inbox.exists()


def test_preparation_failure_cleans_stage_and_retry_is_idempotent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    raw = tmp_path / "train.csv"
    inbox = tmp_path / "inbox"
    write_train(raw, [["1", "2017-08-15", "1", "AUTOMOTIVE", "2.0", "0"]])

    def fail_preparation(path: Path, rows: list[list[str]]) -> None:
        path.write_text("partial", encoding="utf-8")
        raise OSError("preparation failed")

    with monkeypatch.context() as patcher:
        patcher.setattr(train_delivery, "_write_batch", fail_preparation)
        with pytest.raises(OSError, match="preparation failed"):
            simulate_train_delivery("2017-08-15", raw, inbox)

    assert not list(inbox.rglob(".staging-*"))
    assert not list(inbox.rglob("delivery_id=*"))
    delivery = simulate_train_delivery("2017-08-15", raw, inbox)
    assert simulate_train_delivery("2017-08-15", raw, inbox) == delivery
    assert list(inbox.rglob("data.csv")) == [delivery]


def test_publication_failure_cleans_stage_and_retry_succeeds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    raw = tmp_path / "train.csv"
    inbox = tmp_path / "inbox"
    write_train(raw, [["1", "2017-08-15", "1", "AUTOMOTIVE", "2.0", "0"]])

    def fail_publication(source: str, target: Path) -> None:
        raise OSError("publication failed")

    with monkeypatch.context() as patcher:
        patcher.setattr(train_delivery.os, "rename", fail_publication)
        with pytest.raises(OSError, match="publication failed"):
            simulate_train_delivery("2017-08-15", raw, inbox)

    assert not list(inbox.rglob(".staging-*"))
    assert not list(inbox.rglob("delivery_id=*"))
    assert simulate_train_delivery("2017-08-15", raw, inbox).is_file()
