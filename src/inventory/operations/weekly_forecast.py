"""Publish a weekly forecast and gross replenishment recommendation by cut."""

from __future__ import annotations

from dataclasses import asdict
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from inventory.data_preparation import _prediction_cutoff, prepare_forecast_examples
from inventory.forecasting import evaluate_models
from inventory.operations.ingestion import parse_cutoff_at
from inventory.operations.preparation import _date, _history_references, _selected_references
from inventory.ordering import suggest_weekly_orders
from inventory.published_forecasts import (
    _canonical, _read_envelope, _sha256, _write_immutable,
    publish_forecasts,
)


SPLIT_NAMES = ("train_start", "validation_start", "selection_start",
               "test_start", "last_test_start", "calibration_first_start",
               "calibration_last_start")


def _jsonable(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {key: _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    return value


def _prepared_labels(payload: dict) -> dict[tuple[str, int, str], Decimal]:
    if (payload.get("schema_version") != 1 or not isinstance(payload.get("rows"), list)
            or payload.get("row_count") != len(payload["rows"])):
        raise ValueError("Artefacto de preparación inválido")
    labels = {}
    for row in payload["rows"]:
        try:
            key = (row["target_date"], row["store_nbr"], row["family"])
            value = Decimal(row["label_sales"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("Fila preparada inválida") from exc
        if key in labels or not value.is_finite() or value < 0:
            raise ValueError("Filas preparadas duplicadas o inválidas")
        labels[key] = value
    return labels


def _require_preparation(inbox_dir: Path, processed_dir: Path,
                         train_start: date, target_start: date, cutoff_at: datetime) -> tuple[dict, Path]:
    instant = cutoff_at.strftime("%Y-%m-%dT%H%M%SZ")
    ingestion_path = processed_dir / "ingestion" / f"{instant}.json"
    preparation_path = (processed_dir / "preparation" / instant
                        / f"{train_start.isoformat()}_{(target_start - timedelta(days=1)).isoformat()}.json")
    if not ingestion_path.is_file() or not preparation_path.is_file():
        raise FileNotFoundError("Se requieren ingesta y preparación del mismo corte antes de pronosticar")
    ingestion = _read_envelope(ingestion_path)
    prepared = _read_envelope(preparation_path)
    if (ingestion.get("schema_version") != 1
            or ingestion.get("cutoff_at") != cutoff_at.isoformat()
            or ingestion.get("selected_deliveries") != _selected_references(inbox_dir, cutoff_at)
            or prepared.get("start_date") != train_start.isoformat()
            or prepared.get("end_date") != (target_start - timedelta(days=1)).isoformat()
            or prepared.get("cutoff_at") != cutoff_at.isoformat()
            or prepared.get("ingestion_report_sha256") != _sha256(_canonical(ingestion))
            or prepared.get("history_deliveries") != _history_references(inbox_dir, cutoff_at)):
        raise ValueError("Conflicto: la preparación o ingesta no coincide con las entregas del corte")
    return prepared, preparation_path


def publish_weekly_plan(
    inbox_dir: Path, processed_dir: Path, published_dir: Path, costs_path: Path,
    config: dict,
) -> dict:
    """Evaluate, publish seven daily forecasts, then persist gross weekly orders."""
    if not isinstance(config, dict):
        raise ValueError("Se requiere configuración explícita del DAG")
    target = _date(config.get("forecast_start_date"), "forecast_start_date")
    cutoff = parse_cutoff_at(config.get("cutoff_at"))
    if cutoff != _prediction_cutoff(target):
        raise ValueError("cutoff_at debe ser D-1 06:00 UTC de la semana objetivo")
    splits = {name: _date(config.get(name), name) for name in SPLIT_NAMES}
    report_cutoff = parse_cutoff_at(config.get("report_cutoff_at"))
    if report_cutoff > cutoff:
        raise ValueError("El corte de evaluación no puede superar el corte objetivo")
    kind = config.get("kind")
    if kind not in ("real", "historical_simulation"):
        raise ValueError("kind debe ser real o historical_simulation")
    if splits["calibration_last_start"] + timedelta(days=6) >= target:
        raise ValueError("La calibración semanal debe terminar antes de la fecha objetivo")

    inbox_dir, processed_dir = Path(inbox_dir), Path(processed_dir)
    published_dir, costs_path = Path(published_dir), Path(costs_path)
    prepared, preparation_path = _require_preparation(
        inbox_dir, processed_dir, splits["train_start"], target, cutoff)
    labels = _prepared_labels(prepared)
    costs_before = costs_path.read_bytes()
    evaluation = evaluate_models(
        inbox_dir,
        *(splits[name] for name in SPLIT_NAMES[:5]),
        report_cutoff, costs_path,
    )
    if evaluation.partitions is None:
        raise ValueError("La evaluación debe conservar sus particiones")
    # Weekly calibration uses labels at the decision cut. Earlier evaluation
    # partitions may correctly retain labels from their older partition cuts.
    calibration_rows = prepare_forecast_examples(
        inbox_dir, splits["calibration_first_start"], splits["calibration_last_start"],
        cutoff, require_labels=True,
    )
    for row in calibration_rows:
        if labels.get((row.target_date.isoformat(), row.store_nbr, row.family)) != row.label_sales:
            raise ValueError("Las etiquetas semanales difieren de la preparación del corte")

    orders = suggest_weekly_orders(
        inbox_dir, target, cutoff, evaluation,
        splits["calibration_first_start"], splits["calibration_last_start"], costs_path,
    )
    if not orders:
        raise ValueError("No hay series para pronosticar en el corte")
    if costs_path.read_bytes() != costs_before:
        raise ValueError("Los costos cambiaron durante la ejecución")
    _require_preparation(inbox_dir, processed_dir, splits["train_start"], target, cutoff)

    instant = cutoff.strftime("%Y%m%dT%H%M%SZ")
    publication_id = f"weekly-{target.strftime('%Y%m%d')}-{instant}"
    publication = publish_forecasts(
        inbox_dir, target, evaluation, costs_path, publication_id,
        published_dir=published_dir, kind=kind,
    )
    groups = {(order.store_nbr, order.family) for order in orders}
    if (groups != {(row.store_nbr, row.family) for row in publication.forecasts}
            or len(publication.forecasts) != 7 * len(orders)):
        raise ValueError("Los pedidos no coinciden con las series publicadas")
    if costs_path.read_bytes() != costs_before:
        raise ValueError("Los costos cambiaron durante la publicación")
    _require_preparation(inbox_dir, processed_dir, splits["train_start"], target, cutoff)

    forecast_path = published_dir / "forecasts" / f"{publication_id}.json"
    payload = {
        "schema_version": 1,
        "publication_id": publication_id,
        "forecast_start_date": target.isoformat(),
        "cutoff_at": cutoff.isoformat(),
        "kind": kind,
        "preparation_sha256": _sha256(preparation_path.read_bytes()),
        "forecast_sha256": _sha256(forecast_path.read_bytes()),
        "costs_sha256": _sha256(costs_before),
        "evaluation_splits": {name: day.isoformat() for name, day in splits.items()},
        "report_cutoff_at": report_cutoff.isoformat(),
        "selected_model": evaluation.selected_model,
        "model_state_sha256": evaluation.selected_forecast.model_state_sha256,
        "daily_calibration_sha256": publication.daily_calibration_sha256,
        "order_count": len(orders),
        "orders": [_jsonable(asdict(order)) for order in orders],
    }
    order_path = published_dir / "orders" / f"{publication_id}.json"
    _write_immutable(order_path, payload)
    return {"publication_id": publication_id, "cutoff_at": cutoff.isoformat(),
            "forecast_rows": len(publication.forecasts), "order_count": len(orders),
            "orders_path": str(order_path)}
