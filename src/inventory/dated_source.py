"""Shared delivery and read-only ingestion for dated, complete CSV batches."""

import csv
import os
import tempfile
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Callable, Generic, Protocol, TypeVar

from .delivery_selector import AVAILABLE_AT_FORMAT, select_deliveries


class DatedRecord(Protocol):
    date: date


Record = TypeVar("Record", bound=DatedRecord)


@dataclass(frozen=True)
class DatedSource(Generic[Record]):
    name: str
    columns: tuple[str, ...]
    parse_row: Callable[[list[str], int], Record]
    key: Callable[[Record], tuple]


def parse_date(value: str, line: int) -> date:
    try:
        observed = date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"Fila {line}: date inválida") from exc
    if observed.isoformat() != value:
        raise ValueError(f"Fila {line}: date debe tener formato AAAA-MM-DD")
    return observed


def parse_integer(value: str, name: str, line: int, minimum: int = 0) -> int:
    if not value.isascii() or not value.isdecimal() or int(value) < minimum:
        raise ValueError(f"Fila {line}: {name} debe ser un entero >= {minimum}")
    return int(value)


def _observation_date(value: str | date) -> date:
    if isinstance(value, datetime):
        raise ValueError("observation_date debe ser una fecha sin hora")
    if isinstance(value, date):
        return value
    if not isinstance(value, str):
        raise ValueError("observation_date debe tener formato AAAA-MM-DD")
    return parse_date(value, 0)


def _minimum_availability(observed: date) -> datetime:
    try:
        return datetime.combine(observed, time(6), tzinfo=timezone.utc) + timedelta(days=1)
    except OverflowError as exc:
        raise ValueError("No se puede calcular D+1 para observation_date") from exc


def _availability(observed: date, revision: int, available_at: datetime | None) -> datetime:
    minimum = _minimum_availability(observed)
    if available_at is None:
        if revision != 1:
            raise ValueError("Una corrección requiere available_at explícito")
        return minimum
    if not isinstance(available_at, datetime) or available_at.tzinfo is None:
        raise ValueError("available_at debe ser un datetime UTC")
    if available_at.utcoffset() != timedelta(0) or available_at.microsecond:
        raise ValueError("available_at debe ser UTC y tener precisión de segundos")
    if available_at < minimum or (revision == 1 and available_at != minimum):
        raise ValueError("available_at anterior o distinto al mínimo D+1 de la entrega original")
    return available_at


def _validate_header(header: list[str], source: DatedSource) -> None:
    if tuple(header) != source.columns:
        raise ValueError(f"{source.name}.csv debe tener columnas {source.columns} en orden")


def _last_published_revision(date_dir: Path) -> tuple[int, datetime] | None:
    latest = None
    pattern = "available_at=*/revision=*/delivery_id=*/data.csv"
    for path in date_dir.glob(pattern):
        revision_text = path.parent.parent.name.removeprefix("revision=")
        available_text = path.parent.parent.parent.name.removeprefix("available_at=")
        try:
            revision = int(revision_text)
            available = datetime.strptime(available_text, AVAILABLE_AT_FORMAT).replace(tzinfo=timezone.utc)
        except ValueError as exc:
            raise ValueError(f"Metadatos de entrega inválidos: {path}") from exc
        if latest is None or revision > latest[0]:
            latest = (revision, available)
    return latest


def publish_delivery(
    source_name: str,
    observed: date,
    available: datetime,
    revision: int,
    columns: tuple[str, ...],
    rows: list[list[str]],
    inbox_dir: Path,
) -> Path:
    """Atomically publish a complete batch without replacing an existing delivery."""
    day = observed.isoformat()
    revision_text = f"{revision:04d}"
    delivery_id = f"{source_name}-{day}-r{revision_text}"
    date_dir = inbox_dir / f"source={source_name}" / f"observation_date={day}"
    revision_dir = date_dir / f"available_at={available.strftime(AVAILABLE_AT_FORMAT)}" / f"revision={revision_text}"
    delivery_dir = revision_dir / f"delivery_id={delivery_id}"
    destination = delivery_dir / "data.csv"

    for existing_revision in date_dir.glob(f"available_at=*/revision={revision_text}"):
        if existing_revision != revision_dir:
            raise FileExistsError(f"La revisión {revision_text} ya existe para esta fecha")
        if any(path != delivery_dir for path in existing_revision.glob("delivery_id=*")):
            raise FileExistsError(f"La revisión {revision_text} tiene otra entrega")
    for existing_id in inbox_dir.rglob(f"delivery_id={delivery_id}"):
        if existing_id != delivery_dir:
            raise FileExistsError(f"Identificador de entrega duplicado: {delivery_id}")

    if not delivery_dir.exists():
        latest = _last_published_revision(date_dir)
        if latest is not None:
            last_revision, last_available = latest
            if revision <= last_revision:
                raise FileExistsError(f"La revisión debe ser mayor que {last_revision:04d}")
            if available <= last_available:
                raise ValueError("available_at de la nueva revisión debe ser posterior al de la última revisión")

    revision_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=revision_dir, prefix=".staging-") as temporary:
        prepared = Path(temporary) / "data.csv"
        with prepared.open("w", newline="", encoding="utf-8") as output:
            writer = csv.writer(output)
            writer.writerow(columns)
            writer.writerows(rows)
        if delivery_dir.exists() or delivery_dir.is_symlink():
            if destination.is_file() and destination.read_bytes() == prepared.read_bytes():
                return destination
            raise FileExistsError(f"La entrega existente difiere o está incompleta: {delivery_dir}")
        os.rename(temporary, delivery_dir)
    return destination


def simulate_delivery(
    source: DatedSource[Record],
    observation_date: str | date,
    raw_path: Path,
    inbox_dir: Path,
    *,
    revision: int = 1,
    available_at: datetime | None = None,
) -> Path | None:
    """Publish one immutable batch; revisions above one are complete corrections."""
    observed = _observation_date(observation_date)
    if isinstance(revision, bool) or not isinstance(revision, int) or not 1 <= revision <= 9999:
        raise ValueError("revision debe ser un entero entre 1 y 9999")
    available = _availability(observed, revision, available_at)
    raw_path = Path(raw_path)
    inbox_dir = Path(inbox_dir)
    day = observed.isoformat()
    date_dir = inbox_dir / f"source={source.name}" / f"observation_date={day}"

    selected: list[list[str]] = []
    keys: set[tuple] = set()
    with raw_path.open(newline="", encoding="utf-8") as raw:
        reader = csv.reader(raw)
        _validate_header(next(reader, []), source)
        for row in reader:
            record = source.parse_row(row, reader.line_num)
            if record.date != observed:
                continue
            key = source.key(record)
            if key in keys:
                raise ValueError(f"Fila {reader.line_num}: clave duplicada en {source.name}")
            keys.add(key)
            selected.append(row)

    if not selected and revision == 1:
        if _last_published_revision(date_dir) is not None:
            raise FileExistsError(f"La fecha {day} ya tiene una entrega publicada de {source.name}")
        return None
    return publish_delivery(source.name, observed, available, revision, source.columns, selected, inbox_dir)


def ingest_deliveries(source: DatedSource[Record], inbox_dir: Path, cutoff_at: datetime) -> list[Record]:
    """Read selected batches and reject premature deliveries of this source."""
    inbox_dir = Path(inbox_dir)
    selected_paths = select_deliveries(inbox_dir, cutoff_at)
    pattern = "observation_date=*/available_at=*/revision=*/delivery_id=*/data.csv"
    for path in sorted((inbox_dir / f"source={source.name}").glob(pattern)):
        parts = path.relative_to(inbox_dir).parts
        observed = date.fromisoformat(parts[1].removeprefix("observation_date="))
        available = datetime.strptime(parts[2].removeprefix("available_at="), AVAILABLE_AT_FORMAT)
        available = available.replace(tzinfo=timezone.utc)
        if available < _minimum_availability(observed):
            raise ValueError(f"Entrega anticipada de {source.name}: {path}")

    records: list[Record] = []
    keys: set[tuple] = set()
    for path in selected_paths:
        parts = path.relative_to(inbox_dir).parts
        if parts[0] != f"source={source.name}":
            continue
        expected_date = date.fromisoformat(parts[1].removeprefix("observation_date="))
        with path.open(newline="", encoding="utf-8") as batch:
            reader = csv.reader(batch)
            _validate_header(next(reader, []), source)
            for row in reader:
                record = source.parse_row(row, reader.line_num)
                if record.date != expected_date:
                    raise ValueError(f"Fila {reader.line_num} de {path}: date no coincide con la ruta")
                key = source.key(record)
                if key in keys:
                    raise ValueError(f"Fila {reader.line_num} de {path}: clave duplicada en {source.name}")
                keys.add(key)
                records.append(record)
    records.sort(key=source.key)
    return records
