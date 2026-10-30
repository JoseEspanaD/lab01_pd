"""Schema and typed records shared by train delivery and ingestion."""

from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation


TRAIN_COLUMNS = ("id", "date", "store_nbr", "family", "sales", "onpromotion")


@dataclass(frozen=True)
class SaleRecord:
    id: int
    date: date
    store_nbr: int
    family: str
    sales: Decimal
    onpromotion: int


def validate_train_header(header: list[str] | tuple[str, ...]) -> None:
    if tuple(header) != TRAIN_COLUMNS:
        raise ValueError("train.csv debe tener las seis columnas esperadas en orden")


def _nonnegative_integer(value: str, name: str, line: int, minimum: int = 0) -> int:
    if not value.isascii() or not value.isdecimal() or int(value) < minimum:
        raise ValueError(f"Fila {line}: {name} debe ser un entero >= {minimum}")
    return int(value)


def parse_train_row(row: list[str], line: int) -> SaleRecord:
    """Validate and convert all six CSV fields to a typed sale."""
    if len(row) != len(TRAIN_COLUMNS):
        raise ValueError(f"Fila {line}: se esperaban seis columnas")
    identifier = _nonnegative_integer(row[0], "id", line)
    try:
        observed = date.fromisoformat(row[1])
    except ValueError as exc:
        raise ValueError(f"Fila {line}: date inválida") from exc
    if observed.isoformat() != row[1]:
        raise ValueError(f"Fila {line}: date debe tener formato AAAA-MM-DD")
    store = _nonnegative_integer(row[2], "store_nbr", line, minimum=1)
    if not row[3].strip():
        raise ValueError(f"Fila {line}: family no puede estar vacía")
    try:
        sales = Decimal(row[4])
    except InvalidOperation as exc:
        raise ValueError(f"Fila {line}: sales debe ser un número") from exc
    if not sales.is_finite() or sales < 0:
        raise ValueError(f"Fila {line}: sales debe ser finito y no negativo")
    onpromotion = _nonnegative_integer(row[5], "onpromotion", line)
    return SaleRecord(identifier, observed, store, row[3], sales, onpromotion)
