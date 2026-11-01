"""Persist point-in-time training rows after a matching ingestion report."""

from __future__ import annotations

from dataclasses import asdict
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from inventory.data_preparation import prepare_training_data
from inventory.delivery_selector import list_deliveries
from inventory.operations.ingestion import parse_cutoff_at
from inventory.published_forecasts import (
    _canonical, _input_references, _read_envelope, _sha256, _write_immutable,
)


def _date(value: str, name: str) -> date:
    try:
        parsed = date.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} debe ser AAAA-MM-DD") from exc
    if parsed.isoformat() != value:
        raise ValueError(f"{name} debe ser AAAA-MM-DD")
    return parsed


def _history_references(inbox_dir: Path, cutoff_at: datetime) -> list[dict[str, str]]:
    """Include older revisions needed by per-row historical feature cutoffs."""
    return [
        {"path": item.path.relative_to(inbox_dir).as_posix(),
         "sha256": _sha256(item.path.read_bytes())}
        for item in list_deliveries(inbox_dir, cutoff_at)
    ]


def _selected_references(inbox_dir: Path, cutoff_at: datetime) -> list[dict[str, str]]:
    return [asdict(item) for item in _input_references(inbox_dir, cutoff_at)]


def _row_payload(row) -> dict:
    features = asdict(row.features)
    for name, value in features.items():
        if isinstance(value, Decimal):
            features[name] = str(value)
    return {
        "target_date": row.target_date.isoformat(),
        "store_nbr": row.store_nbr,
        "family": row.family,
        "prediction_cutoff_at": row.prediction_cutoff_at.isoformat(),
        "features": features,
        "label_sales": str(row.label_sales),
    }


def prepare_and_record(
    inbox_dir: Path, processed_dir: Path, start_text: str, end_text: str, cutoff_text: str,
) -> dict:
    """Write one immutable range for a cutoff, reusing an identical result."""
    start_date = _date(start_text, "start_date")
    end_date = _date(end_text, "end_date")
    if end_date < start_date:
        raise ValueError("end_date debe ser igual o posterior a start_date")
    cutoff_at = parse_cutoff_at(cutoff_text)
    inbox_dir, processed_dir = Path(inbox_dir), Path(processed_dir)
    instant = cutoff_at.strftime("%Y-%m-%dT%H%M%SZ")
    ingestion_path = processed_dir / "ingestion" / f"{instant}.json"
    if not ingestion_path.is_file():
        raise FileNotFoundError(f"Falta el reporte de ingest_deliveries: {ingestion_path}")
    ingestion = _read_envelope(ingestion_path)
    if (ingestion.get("schema_version") != 1
            or ingestion.get("cutoff_at") != cutoff_at.isoformat()):
        raise ValueError("El reporte de ingesta no corresponde al corte solicitado")
    selected = _selected_references(inbox_dir, cutoff_at)
    if ingestion.get("selected_deliveries") != selected:
        raise ValueError("Conflicto: las entregas seleccionadas difieren del reporte de ingesta")
    history = _history_references(inbox_dir, cutoff_at)
    provenance = {
        "schema_version": 1,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "cutoff_at": cutoff_at.isoformat(),
        "ingestion_report_sha256": _sha256(_canonical(ingestion)),
        "history_deliveries": history,
    }
    destination = (processed_dir / "preparation" / instant
                   / f"{start_date.isoformat()}_{end_date.isoformat()}.json")
    if destination.exists():
        existing = _read_envelope(destination)
        if any(existing.get(key) != value for key, value in provenance.items()):
            raise ValueError(f"Conflicto: la preparación existente difiere: {destination}")
        if (not isinstance(existing.get("rows"), list)
                or existing.get("row_count") != len(existing["rows"])):
            raise ValueError("El conteo del artefacto preparado es inválido")
        return {"path": str(destination), "row_count": existing["row_count"],
                "cutoff_at": provenance["cutoff_at"], "reused": True}

    rows = prepare_training_data(inbox_dir, start_date, end_date, cutoff_at)
    if (_selected_references(inbox_dir, cutoff_at) != selected
            or _history_references(inbox_dir, cutoff_at) != history):
        raise ValueError("Las entregas cambiaron durante la preparación")
    payload = {**provenance, "row_count": len(rows),
               "rows": [_row_payload(row) for row in rows]}
    _write_immutable(destination, payload)
    return {"path": str(destination), "row_count": len(rows),
            "cutoff_at": provenance["cutoff_at"], "reused": False}
