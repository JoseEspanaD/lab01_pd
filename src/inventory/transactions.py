"""Simulate and ingest dated transaction batches."""

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

from .dated_source import DatedSource, ingest_deliveries, parse_date, parse_integer, simulate_delivery


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_RAW = PROJECT_ROOT / "data/raw/transactions.csv"
DEFAULT_INBOX = PROJECT_ROOT / "data/inbox"
TRANSACTIONS_COLUMNS = ("date", "store_nbr", "transactions")


@dataclass(frozen=True)
class TransactionRecord:
    date: date
    store_nbr: int
    transactions: int


def _parse_row(row: list[str], line: int) -> TransactionRecord:
    if len(row) != len(TRANSACTIONS_COLUMNS):
        raise ValueError(f"Fila {line}: se esperaban tres columnas")
    return TransactionRecord(
        parse_date(row[0], line),
        parse_integer(row[1], "store_nbr", line, minimum=1),
        parse_integer(row[2], "transactions", line),
    )


SOURCE = DatedSource("transactions", TRANSACTIONS_COLUMNS, _parse_row, lambda row: (row.date, row.store_nbr))


def simulate_transactions_delivery(
    observation_date: str | date,
    raw_path: Path = DEFAULT_RAW,
    inbox_dir: Path = DEFAULT_INBOX,
    *,
    revision: int = 1,
    available_at: datetime | None = None,
) -> Path | None:
    return simulate_delivery(SOURCE, observation_date, raw_path, inbox_dir, revision=revision, available_at=available_at)


def ingest_transactions(inbox_dir: Path, cutoff_at: datetime) -> list[TransactionRecord]:
    return ingest_deliveries(SOURCE, inbox_dir, cutoff_at)
