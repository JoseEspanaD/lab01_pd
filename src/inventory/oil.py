"""Simulate and ingest dated oil price batches."""

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

from .dated_source import DatedSource, ingest_deliveries, parse_date, simulate_delivery


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_RAW = PROJECT_ROOT / "data/raw/oil.csv"
DEFAULT_INBOX = PROJECT_ROOT / "data/inbox"
OIL_COLUMNS = ("date", "dcoilwtico")


@dataclass(frozen=True)
class OilRecord:
    date: date
    dcoilwtico: Decimal | None


def _parse_row(row: list[str], line: int) -> OilRecord:
    if len(row) != len(OIL_COLUMNS):
        raise ValueError(f"Fila {line}: se esperaban dos columnas")
    observed = parse_date(row[0], line)
    if row[1] == "":
        return OilRecord(observed, None)
    try:
        price = Decimal(row[1])
    except InvalidOperation as exc:
        raise ValueError(f"Fila {line}: dcoilwtico debe ser un número o estar vacío") from exc
    if not price.is_finite() or price < 0:
        raise ValueError(f"Fila {line}: dcoilwtico debe ser finito y no negativo")
    return OilRecord(observed, price)


SOURCE = DatedSource("oil", OIL_COLUMNS, _parse_row, lambda row: (row.date,))


def simulate_oil_delivery(
    observation_date: str | date,
    raw_path: Path = DEFAULT_RAW,
    inbox_dir: Path = DEFAULT_INBOX,
    *,
    revision: int = 1,
    available_at: datetime | None = None,
) -> Path | None:
    return simulate_delivery(SOURCE, observation_date, raw_path, inbox_dir, revision=revision, available_at=available_at)


def ingest_oil(inbox_dir: Path, cutoff_at: datetime) -> list[OilRecord]:
    return ingest_deliveries(SOURCE, inbox_dir, cutoff_at)
