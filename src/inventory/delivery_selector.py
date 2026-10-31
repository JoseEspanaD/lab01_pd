"""Select inbox deliveries available at a historical UTC cutoff."""

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import re


SOURCES = frozenset({"train", "transactions", "oil", "holidays_events", "stores"})
AVAILABLE_AT_FORMAT = "%Y-%m-%dT%H%M%SZ"


@dataclass(frozen=True)
class _Delivery:
    source: str
    observation_date: date
    available_at: datetime
    revision: int
    delivery_id: str
    path: Path


def _value(part: str, prefix: str, path: Path) -> str:
    if not part.startswith(prefix) or not part[len(prefix) :]:
        raise ValueError(f"Ruta de entrega inválida: {path}")
    return part[len(prefix) :]


def _parse_path(path: Path, inbox_dir: Path) -> _Delivery | None:
    parts = path.relative_to(inbox_dir).parts
    if not 1 <= len(parts) <= 6:
        raise ValueError(f"Ruta de entrega inválida: {path}")

    source = _value(parts[0], "source=", path)
    if source not in SOURCES:
        raise ValueError(f"Fuente inválida en la ruta: {path}")
    if len(parts) == 1:
        return None

    observed_text = _value(parts[1], "observation_date=", path)
    try:
        observed = date.fromisoformat(observed_text)
    except ValueError as exc:
        raise ValueError(f"observation_date inválida en la ruta: {path}") from exc
    if observed.isoformat() != observed_text:
        raise ValueError(f"observation_date inválida en la ruta: {path}")
    if len(parts) == 2:
        return None

    available_text = _value(parts[2], "available_at=", path)
    if not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{6}Z", available_text):
        raise ValueError(f"available_at inválido en la ruta: {path}")
    try:
        available = datetime.strptime(available_text, AVAILABLE_AT_FORMAT).replace(tzinfo=timezone.utc)
    except ValueError as exc:
        raise ValueError(f"available_at inválido en la ruta: {path}") from exc
    if available.strftime(AVAILABLE_AT_FORMAT) != available_text:
        raise ValueError(f"available_at inválido en la ruta: {path}")
    if len(parts) == 3:
        return None

    revision_text = _value(parts[3], "revision=", path)
    if not re.fullmatch(r"[0-9]{4}", revision_text) or int(revision_text) == 0:
        raise ValueError(f"revision inválida en la ruta: {path}")
    revision = int(revision_text)
    if len(parts) == 4:
        return None

    delivery_id = _value(parts[4], "delivery_id=", path)
    if not delivery_id.strip():
        raise ValueError(f"delivery_id inválido en la ruta: {path}")
    if len(parts) == 5:
        return None
    if parts[5] != "data.csv":
        raise ValueError(f"Ruta de entrega inválida: {path}")
    return _Delivery(source, observed, available, revision, delivery_id, path)


def list_deliveries(inbox_dir: Path, cutoff_at: datetime) -> list[_Delivery]:
    """Validate inbox metadata once and list deliveries through cutoff_at.

    Staging directories are ignored because the producer has not published them.
    """
    if not isinstance(cutoff_at, datetime) or cutoff_at.tzinfo is None:
        raise ValueError("cutoff_at debe ser un datetime UTC con zona horaria")
    if cutoff_at.utcoffset() != timedelta(0):
        raise ValueError("cutoff_at debe estar en UTC")
    inbox_dir = Path(inbox_dir)
    if not inbox_dir.exists():
        return []
    if not inbox_dir.is_dir():
        raise ValueError(f"inbox no es un directorio: {inbox_dir}")

    seen_ids: set[str] = set()
    seen_revisions: set[tuple[str, date, int]] = set()
    deliveries: list[_Delivery] = []
    for path in sorted(inbox_dir.rglob("*")):
        parts = path.relative_to(inbox_dir).parts
        if any(part.startswith(".staging-") for part in parts):
            continue
        if parts == (".gitkeep",) and path.is_file():
            continue
        if path.is_symlink():
            raise ValueError(f"Ruta de entrega inválida: {path}")
        delivery = _parse_path(path, inbox_dir)
        if path.is_dir():
            if len(parts) == 6:
                raise ValueError(f"data.csv debe ser un archivo: {path}")
            if len(parts) == 5 and not (path / "data.csv").is_file():
                raise ValueError(f"Entrega incompleta: {path}")
            continue
        if not path.is_file() or delivery is None:
            raise ValueError(f"Ruta de entrega inválida: {path}")

        if delivery.delivery_id in seen_ids:
            raise ValueError(f"Identificador de entrega duplicado: {delivery.delivery_id}")
        seen_ids.add(delivery.delivery_id)
        revision_key = (delivery.source, delivery.observation_date, delivery.revision)
        if revision_key in seen_revisions:
            raise ValueError(f"Revisión duplicada: {revision_key}")
        seen_revisions.add(revision_key)

        if delivery.available_at <= cutoff_at:
            deliveries.append(delivery)

    return deliveries


def select_deliveries(inbox_dir: Path, cutoff_at: datetime) -> list[Path]:
    """Return one immutable CSV path per source and date, ordered by those keys."""
    selected: dict[tuple[str, date], _Delivery] = {}
    for delivery in list_deliveries(inbox_dir, cutoff_at):
        key = (delivery.source, delivery.observation_date)
        previous = selected.get(key)
        if previous is None or (delivery.available_at, delivery.revision) > (
            previous.available_at, previous.revision
        ):
            selected[key] = delivery

    return [selected[key].path for key in sorted(selected)]
