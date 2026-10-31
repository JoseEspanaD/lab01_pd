"""Simulate and ingest complete daily holiday and event batches."""

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

from .dated_source import DatedSource, ingest_deliveries, parse_date, simulate_delivery


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_RAW = PROJECT_ROOT / "data/raw/holidays_events.csv"
DEFAULT_INBOX = PROJECT_ROOT / "data/inbox"
HOLIDAYS_COLUMNS = ("date", "type", "locale", "locale_name", "description", "transferred")
EVENT_TYPES = frozenset({"Holiday", "Event", "Additional", "Transfer", "Bridge", "Work Day"})
LOCALES = frozenset({"National", "Regional", "Local"})


@dataclass(frozen=True)
class HolidayEventRecord:
    date: date
    type: str
    locale: str
    locale_name: str
    description: str
    transferred: bool


def _parse_row(row: list[str], line: int) -> HolidayEventRecord:
    if len(row) != len(HOLIDAYS_COLUMNS):
        raise ValueError(f"Fila {line}: se esperaban seis columnas")
    observed = parse_date(row[0], line)
    if row[1] not in EVENT_TYPES:
        raise ValueError(f"Fila {line}: type inválido")
    if row[2] not in LOCALES:
        raise ValueError(f"Fila {line}: locale inválido")
    if not row[3].strip() or not row[4].strip():
        raise ValueError(f"Fila {line}: locale_name y description no pueden estar vacíos")
    if row[5] not in {"True", "False"}:
        raise ValueError(f"Fila {line}: transferred debe ser True o False")
    return HolidayEventRecord(observed, row[1], row[2], row[3], row[4], row[5] == "True")


def _key(row: HolidayEventRecord) -> tuple:
    return (row.date, row.type, row.locale, row.locale_name, row.description, row.transferred)


SOURCE = DatedSource("holidays_events", HOLIDAYS_COLUMNS, _parse_row, _key)


def simulate_holidays_delivery(
    observation_date: str | date,
    raw_path: Path = DEFAULT_RAW,
    inbox_dir: Path = DEFAULT_INBOX,
    *,
    revision: int = 1,
    available_at: datetime | None = None,
) -> Path | None:
    return simulate_delivery(SOURCE, observation_date, raw_path, inbox_dir, revision=revision, available_at=available_at)


def ingest_holidays(inbox_dir: Path, cutoff_at: datetime) -> list[HolidayEventRecord]:
    return ingest_deliveries(SOURCE, inbox_dir, cutoff_at)
