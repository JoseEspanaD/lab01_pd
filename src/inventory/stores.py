"""Simulate and ingest the initial store catalog at DEC-001's T0."""

import csv
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from .dated_source import parse_integer, publish_delivery
from .delivery_selector import AVAILABLE_AT_FORMAT, select_deliveries


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_RAW = PROJECT_ROOT / "data/raw/stores.csv"
DEFAULT_INBOX = PROJECT_ROOT / "data/inbox"
STORES_COLUMNS = ("store_nbr", "city", "state", "type", "cluster")
CATALOG_DATE = date(2013, 1, 1)
T0 = datetime(2013, 1, 1, tzinfo=timezone.utc)


@dataclass(frozen=True)
class StoreRecord:
    store_nbr: int
    city: str
    state: str
    type: str
    cluster: int


def _validate_header(header: list[str]) -> None:
    if tuple(header) != STORES_COLUMNS:
        raise ValueError("stores.csv debe tener las cinco columnas esperadas en orden")


def _parse_row(row: list[str], line: int) -> StoreRecord:
    if len(row) != len(STORES_COLUMNS):
        raise ValueError(f"Fila {line}: se esperaban cinco columnas")
    store_nbr = parse_integer(row[0], "store_nbr", line, minimum=1)
    if not row[1].strip() or not row[2].strip():
        raise ValueError(f"Fila {line}: city y state no pueden estar vacíos")
    if row[3] not in {"A", "B", "C", "D", "E"}:
        raise ValueError(f"Fila {line}: type inválido")
    cluster = parse_integer(row[4], "cluster", line, minimum=1)
    return StoreRecord(store_nbr, row[1], row[2], row[3], cluster)


def _availability(revision: int, available_at: datetime | None) -> datetime:
    if available_at is None:
        if revision != 1:
            raise ValueError("Una corrección de stores requiere available_at explícito")
        return T0
    if not isinstance(available_at, datetime) or available_at.tzinfo is None:
        raise ValueError("available_at debe ser un datetime UTC")
    if available_at.utcoffset() != timedelta(0) or available_at.microsecond:
        raise ValueError("available_at debe ser UTC y tener precisión de segundos")
    if available_at < T0 or (revision == 1 and available_at != T0):
        raise ValueError("La entrega original de stores debe estar disponible en T0")
    return available_at


def simulate_stores_delivery(
    raw_path: Path = DEFAULT_RAW,
    inbox_dir: Path = DEFAULT_INBOX,
    *,
    revision: int = 1,
    available_at: datetime | None = None,
) -> Path:
    """Publish the complete store catalog, or an immutable later correction."""
    if isinstance(revision, bool) or not isinstance(revision, int) or not 1 <= revision <= 9999:
        raise ValueError("revision debe ser un entero entre 1 y 9999")
    available = _availability(revision, available_at)
    rows: list[list[str]] = []
    seen_stores: set[int] = set()
    with Path(raw_path).open(newline="", encoding="utf-8") as raw:
        reader = csv.reader(raw)
        _validate_header(next(reader, []))
        for row in reader:
            store = _parse_row(row, reader.line_num)
            if store.store_nbr in seen_stores:
                raise ValueError(f"Fila {reader.line_num}: store_nbr duplicado")
            seen_stores.add(store.store_nbr)
            rows.append(row)
    if not rows and revision == 1:
        raise ValueError("El catálogo inicial de stores no puede estar vacío")
    return publish_delivery("stores", CATALOG_DATE, available, revision, STORES_COLUMNS, rows, Path(inbox_dir))


def _validate_catalog_metadata(inbox_dir: Path) -> None:
    pattern = "observation_date=*/available_at=*/revision=*/delivery_id=*/data.csv"
    for path in sorted((inbox_dir / "source=stores").glob(pattern)):
        parts = path.relative_to(inbox_dir).parts
        if parts[1] != f"observation_date={CATALOG_DATE.isoformat()}":
            raise ValueError(f"observation_date inválida para stores: {path}")
        available = datetime.strptime(parts[2].removeprefix("available_at="), AVAILABLE_AT_FORMAT)
        available = available.replace(tzinfo=timezone.utc)
        if available < T0 or (parts[3] == "revision=0001" and available != T0):
            raise ValueError(f"Disponibilidad inválida para stores: {path}")


def ingest_stores(inbox_dir: Path, cutoff_at: datetime) -> list[StoreRecord]:
    """Read only the store catalog selected at the UTC historical cutoff."""
    inbox_dir = Path(inbox_dir)
    selected_paths = select_deliveries(inbox_dir, cutoff_at)
    _validate_catalog_metadata(inbox_dir)
    stores: list[StoreRecord] = []
    seen_stores: set[int] = set()
    for path in selected_paths:
        if path.relative_to(inbox_dir).parts[0] != "source=stores":
            continue
        with path.open(newline="", encoding="utf-8") as batch:
            reader = csv.reader(batch)
            _validate_header(next(reader, []))
            for row in reader:
                store = _parse_row(row, reader.line_num)
                if store.store_nbr in seen_stores:
                    raise ValueError(f"Fila {reader.line_num} de {path}: store_nbr duplicado")
                seen_stores.add(store.store_nbr)
                stores.append(store)
    stores.sort(key=lambda store: store.store_nbr)
    return stores
