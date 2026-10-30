from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from inventory.delivery_selector import select_deliveries
from inventory.train_delivery import simulate_train_delivery


def utc(year: int, month: int, day: int, hour: int, minute: int = 0) -> datetime:
    return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)


def make_delivery(
    inbox: Path,
    source: str = "train",
    observation_date: str = "2017-08-15",
    available_at: str = "2017-08-16T060000Z",
    revision: str = "0001",
    delivery_id: str | None = None,
) -> Path:
    delivery_id = delivery_id or f"{source}-{observation_date}-r{revision}"
    path = (
        inbox
        / f"source={source}"
        / f"observation_date={observation_date}"
        / f"available_at={available_at}"
        / f"revision={revision}"
        / f"delivery_id={delivery_id}"
        / "data.csv"
    )
    path.parent.mkdir(parents=True)
    path.write_text("contenido sin formato CSV", encoding="utf-8")
    return path


def test_cutoff_before_and_at_delivery(tmp_path: Path) -> None:
    inbox = tmp_path / "inbox"
    inbox.mkdir()
    (inbox / ".gitkeep").touch()
    delivery = make_delivery(inbox)

    assert select_deliveries(inbox, utc(2017, 8, 16, 5, 59)) == []
    assert select_deliveries(inbox, utc(2017, 8, 16, 6)) == [delivery]


def test_corrections_and_order_are_deterministic(tmp_path: Path) -> None:
    inbox = tmp_path / "inbox"
    original = make_delivery(inbox)
    corrected = make_delivery(inbox, available_at="2017-08-20T060000Z", revision="0002")
    oil = make_delivery(inbox, source="oil")
    earlier = make_delivery(inbox, observation_date="2017-08-14", available_at="2017-08-15T060000Z")

    assert select_deliveries(inbox, utc(2017, 8, 19, 0)) == [oil, earlier, original]
    expected = [oil, earlier, corrected]
    assert select_deliveries(inbox, utc(2017, 8, 20, 6)) == expected
    assert select_deliveries(inbox, utc(2017, 8, 20, 6)) == expected


def test_higher_revision_wins_when_available_at_ties(tmp_path: Path) -> None:
    inbox = tmp_path / "inbox"
    make_delivery(inbox)
    corrected = make_delivery(inbox, revision="0002")

    assert select_deliveries(inbox, utc(2017, 8, 16, 6)) == [corrected]


def test_selector_accepts_producer_output_without_reading_csv(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    raw = tmp_path / "train.csv"
    raw.write_text(
        "id,date,store_nbr,family,sales,onpromotion\n"
        "1,2017-08-15,1,AUTOMOTIVE,2.0,0\n",
        encoding="utf-8",
    )
    inbox = tmp_path / "inbox"
    delivery = simulate_train_delivery("2017-08-15", raw, inbox)

    def reject_open(*args: object, **kwargs: object) -> None:
        raise AssertionError("El selector no debe abrir archivos")

    with monkeypatch.context() as patcher:
        patcher.setattr(Path, "open", reject_open)
        assert select_deliveries(inbox, utc(2017, 8, 16, 6)) == [delivery]


@pytest.mark.parametrize(
    "relative_path",
    [
        "source=unknown/observation_date=2017-08-15/available_at=2017-08-16T060000Z/revision=0001/delivery_id=x/data.csv",
        "source=train/observation_date=2017-02-30/available_at=2017-08-16T060000Z/revision=0001/delivery_id=x/data.csv",
        "source=train/observation_date=2017-08-15/available_at=2017-02-30T060000Z/revision=0001/delivery_id=x/data.csv",
        "source=train/observation_date=2017-08-15/available_at=2017-08-16T060000/revision=0001/delivery_id=x/data.csv",
        "source=train/observation_date=2017-08-15/available_at=2017-08-16T060000Z/revision=0000/delivery_id=x/data.csv",
        "source=train/observation_date=2017-08-15/available_at=2017-08-16T060000Z/revision=abc/delivery_id=x/data.csv",
        "source=train/observation_date=2017-08-15/available_at=2017-08-16T060000Z/revision=0001/delivery_id=/data.csv",
        "source=train/observation_date=2017-08-15/available_at=2017-08-16T060000Z/revision=0001/delivery_id=x/other.csv",
    ],
)
def test_invalid_metadata_is_rejected(tmp_path: Path, relative_path: str) -> None:
    inbox = tmp_path / "inbox"
    path = inbox / relative_path
    path.parent.mkdir(parents=True)
    path.touch()

    with pytest.raises(ValueError):
        select_deliveries(inbox, utc(2017, 8, 30, 0))


def test_duplicate_id_is_rejected_even_when_unavailable(tmp_path: Path) -> None:
    inbox = tmp_path / "inbox"
    make_delivery(inbox, delivery_id="repeated")
    make_delivery(inbox, source="oil", available_at="2017-09-01T060000Z", delivery_id="repeated")

    with pytest.raises(ValueError, match="duplicado"):
        select_deliveries(inbox, utc(2017, 8, 16, 6))


def test_duplicate_revision_is_rejected(tmp_path: Path) -> None:
    inbox = tmp_path / "inbox"
    make_delivery(inbox, delivery_id="first")
    make_delivery(inbox, available_at="2017-08-17T060000Z", delivery_id="second")

    with pytest.raises(ValueError, match="Revisión duplicada"):
        select_deliveries(inbox, utc(2017, 8, 20, 0))


def test_incomplete_delivery_is_rejected_and_staging_is_ignored(tmp_path: Path) -> None:
    inbox = tmp_path / "inbox"
    revision = inbox / "source=train/observation_date=2017-08-15/available_at=2017-08-16T060000Z/revision=0001"
    stage = revision / ".staging-in-progress"
    stage.mkdir(parents=True)
    (stage / "data.csv").touch()
    assert select_deliveries(inbox, utc(2017, 8, 16, 6)) == []

    (revision / "delivery_id=incomplete").mkdir()
    with pytest.raises(ValueError, match="incompleta"):
        select_deliveries(inbox, utc(2017, 8, 16, 6))


@pytest.mark.parametrize(
    "cutoff",
    [datetime(2017, 8, 16, 6), datetime(2017, 8, 16, 7, tzinfo=timezone(timedelta(hours=1)))],
)
def test_cutoff_must_be_utc(tmp_path: Path, cutoff: datetime) -> None:
    with pytest.raises(ValueError, match="cutoff_at"):
        select_deliveries(tmp_path / "inbox", cutoff)
