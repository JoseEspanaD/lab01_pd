"""Read the train deliveries visible at a historical cutoff."""

import csv
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from .delivery_selector import AVAILABLE_AT_FORMAT, select_deliveries
from .train_schema import SaleRecord, parse_train_row, validate_train_header


def _validate_train_availability(inbox_dir: Path) -> None:
    """Reject premature train deliveries, including unselected corrections."""
    pattern = "observation_date=*/available_at=*/revision=*/delivery_id=*/data.csv"
    for path in sorted((inbox_dir / "source=train").glob(pattern)):
        parts = path.relative_to(inbox_dir).parts
        observed = date.fromisoformat(parts[1].removeprefix("observation_date="))
        available = datetime.strptime(
            parts[2].removeprefix("available_at="), AVAILABLE_AT_FORMAT
        ).replace(tzinfo=timezone.utc)
        try:
            minimum = datetime(observed.year, observed.month, observed.day, 6, tzinfo=timezone.utc)
            minimum += timedelta(days=1)
        except OverflowError as exc:
            raise ValueError(f"No se puede calcular D+1 para {path}") from exc
        if available < minimum:
            raise ValueError(f"Entrega anticipada de train: {path}; mínimo {minimum.isoformat()}")


def ingest_sales(inbox_dir: Path, cutoff_at: datetime) -> list[SaleRecord]:
    """Return validated sales from the selected full batches, without writing files."""
    inbox_dir = Path(inbox_dir)
    records: list[SaleRecord] = []
    keys: set[tuple[date, int, str]] = set()

    selected_paths = select_deliveries(inbox_dir, cutoff_at)
    _validate_train_availability(inbox_dir)
    for path in selected_paths:
        parts = path.relative_to(inbox_dir).parts
        if parts[0] != "source=train":
            continue
        expected_date = date.fromisoformat(parts[1].removeprefix("observation_date="))
        with path.open(newline="", encoding="utf-8") as source:
            reader = csv.reader(source)
            validate_train_header(next(reader, ()))
            for row in reader:
                sale = parse_train_row(row, reader.line_num)
                if sale.date != expected_date:
                    raise ValueError(f"Fila {reader.line_num} de {path}: date no coincide con la ruta")
                key = (sale.date, sale.store_nbr, sale.family)
                if key in keys:
                    raise ValueError(f"Fila {reader.line_num} de {path}: clave de venta duplicada")
                keys.add(key)
                records.append(sale)

    records.sort(key=lambda sale: (sale.date, sale.store_nbr, sale.family))
    return records
