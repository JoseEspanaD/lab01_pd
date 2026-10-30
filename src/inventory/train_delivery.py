"""Prepare one immutable, simulated delivery from the historical train CSV."""

import argparse
import csv
import os
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path

from .train_schema import TRAIN_COLUMNS, parse_train_row, validate_train_header


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_RAW = PROJECT_ROOT / "data/raw/train.csv"
DEFAULT_INBOX = PROJECT_ROOT / "data/inbox"


def _parse_observation_date(value: str | date) -> date:
    if isinstance(value, datetime):
        raise ValueError("observation_date debe ser una fecha sin hora")
    if isinstance(value, date):
        return value
    try:
        parsed = date.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("observation_date debe tener formato AAAA-MM-DD") from exc
    if parsed.isoformat() != value:
        raise ValueError("observation_date debe tener formato AAAA-MM-DD")
    return parsed


def _write_batch(path: Path, rows: list[list[str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.writer(output)
        writer.writerow(TRAIN_COLUMNS)
        writer.writerows(rows)


def simulate_train_delivery(
    observation_date: str | date,
    raw_path: Path = DEFAULT_RAW,
    inbox_dir: Path = DEFAULT_INBOX,
) -> Path | None:
    """Publish revision 0001 for one date, or return None if it has no rows.

    An identical delivery can be requested again. Conflicting deliveries are
    rejected rather than replaced. This is a producer of inbox files, separate
    from the product's read-only ingestion of the inbox.
    """
    observed = _parse_observation_date(observation_date)
    raw_path = Path(raw_path)
    inbox_dir = Path(inbox_dir)
    day = observed.isoformat()
    available_at = f"{(observed + timedelta(days=1)).isoformat()}T060000Z"
    delivery_id = f"train-{day}-r0001"
    date_dir = inbox_dir / "source=train" / f"observation_date={day}"
    revision_dir = date_dir / f"available_at={available_at}" / "revision=0001"
    delivery_dir = revision_dir / f"delivery_id={delivery_id}"
    destination = delivery_dir / "data.csv"

    selected: list[list[str]] = []
    keys: set[tuple[int, str]] = set()
    with raw_path.open(newline="", encoding="utf-8") as source:
        reader = csv.reader(source)
        validate_train_header(next(reader, ()))
        for row in reader:
            sale = parse_train_row(row, reader.line_num)
            if sale.date != observed:
                continue
            key = (sale.store_nbr, sale.family)
            if key in keys:
                raise ValueError(f"Fila {reader.line_num}: clave duplicada (date, store_nbr, family)")
            keys.add(key)
            selected.append(row)

    if not selected:
        return None

    # Revision numbers and delivery IDs must identify a single delivery.
    for existing_revision in date_dir.glob("available_at=*/revision=0001"):
        if existing_revision != revision_dir:
            raise FileExistsError("La revisión 0001 ya existe para esta fecha")
        if any(path != delivery_dir for path in existing_revision.glob("delivery_id=*")):
            raise FileExistsError("La revisión 0001 tiene otra entrega")
    for existing_id in inbox_dir.rglob(f"delivery_id={delivery_id}"):
        if existing_id != delivery_dir:
            raise FileExistsError(f"Identificador de entrega duplicado: {delivery_id}")

    revision_dir.mkdir(parents=True, exist_ok=True)
    # The staging directory is a sibling of the final delivery, so rename is
    # atomic on the same filesystem. TemporaryDirectory removes it on failure.
    with tempfile.TemporaryDirectory(dir=revision_dir, prefix=".staging-") as temporary:
        prepared = Path(temporary) / "data.csv"
        _write_batch(prepared, selected)
        if delivery_dir.exists() or delivery_dir.is_symlink():
            if destination.is_file() and destination.read_bytes() == prepared.read_bytes():
                return destination
            raise FileExistsError(f"La entrega existente difiere o está incompleta: {delivery_dir}")
        os.rename(temporary, delivery_dir)
    return destination


def main() -> None:
    parser = argparse.ArgumentParser(description="Simula una entrega diaria de train.csv")
    parser.add_argument("observation_date", help="Fecha de observación AAAA-MM-DD")
    parser.add_argument("--raw-path", type=Path, default=DEFAULT_RAW)
    parser.add_argument("--inbox-dir", type=Path, default=DEFAULT_INBOX)
    args = parser.parse_args()
    delivery = simulate_train_delivery(args.observation_date, args.raw_path, args.inbox_dir)
    print(delivery if delivery is not None else "No hay registros para esa fecha")


if __name__ == "__main__":
    main()
