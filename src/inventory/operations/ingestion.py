"""Validate an immutable inbox at one explicit UTC cutoff and record its inputs."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
import re

from inventory.holidays_events import ingest_holidays
from inventory.oil import ingest_oil
from inventory.published_forecasts import _input_references, _require_utc, _write_immutable
from inventory.sales_ingestion import ingest_sales
from inventory.stores import ingest_stores
from inventory.transactions import ingest_transactions


READERS = (
    ("train", ingest_sales),
    ("transactions", ingest_transactions),
    ("oil", ingest_oil),
    ("holidays_events", ingest_holidays),
    ("stores", ingest_stores),
)


def parse_cutoff_at(value: str) -> datetime:
    """Require an explicit UTC timestamp with second precision."""
    if not isinstance(value, str) or not re.fullmatch(
        r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(Z|\+00:00)", value
    ):
        raise ValueError("cutoff_at debe ser AAAA-MM-DDTHH:MM:SSZ en UTC")
    try:
        cutoff_at = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("cutoff_at debe ser una fecha y hora UTC válidas") from exc
    _require_utc(cutoff_at, "cutoff_at")
    return cutoff_at


def validate_and_record(
    inbox_dir: Path, cutoff_text: str, processed_dir: Path
) -> dict:
    """Read selected batches, validate all sources, and write one immutable report.

    A repeat at the same cutoff accepts only the identical selected inputs and
    row counts. The inbox is never written; later corrections need a later cut.
    """
    cutoff_at = parse_cutoff_at(cutoff_text)
    inbox_dir = Path(inbox_dir)
    if inbox_dir.is_symlink() or not inbox_dir.is_dir():
        raise ValueError(f"inbox debe ser un directorio existente: {inbox_dir}")

    before = _input_references(inbox_dir, cutoff_at)
    counts = {name: len(reader(inbox_dir, cutoff_at)) for name, reader in READERS}
    after = _input_references(inbox_dir, cutoff_at)
    if before != after:
        raise ValueError("Las entregas visibles cambiaron durante la ingesta")
    if not before:
        raise ValueError(f"No hay entregas visibles al corte {cutoff_at.isoformat()}")

    report = {
        "schema_version": 1,
        "cutoff_at": cutoff_at.isoformat(),
        "selected_deliveries": [
            {"path": reference.path, "sha256": reference.sha256} for reference in before
        ],
        "record_counts": counts,
    }
    instant = cutoff_at.strftime("%Y-%m-%dT%H%M%SZ")
    destination = Path(processed_dir) / "ingestion" / f"{instant}.json"
    _write_immutable(destination, report)
    return report
