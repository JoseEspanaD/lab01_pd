"""Immutable, auditable snapshots of forecasts actually written to disk."""

import hashlib
import json
import math
import os
import re
import tempfile
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timezone
from pathlib import Path

from .data_preparation import _prediction_cutoff
from .delivery_selector import select_deliveries
from .forecasting import CostPolicy, EvaluationResult, Forecast, forecast_week, load_costs
from .ordering import _group_forecasts, require_evaluation_provenance


DEFAULT_PUBLISHED_DIR = Path("data/published")


@dataclass(frozen=True)
class InputReference:
    path: str
    sha256: str


@dataclass(frozen=True)
class PublishedForecasts:
    publication_id: str
    kind: str  # "real" or "historical_simulation"
    recorded_at: datetime
    published_at: datetime | None
    forecast_start_date: date
    prediction_cutoff_at: datetime
    model_name: str
    model_state_sha256: str
    model_available_at: datetime
    daily_calibration_available_at: datetime
    daily_calibration_sha256: str
    costs_path: str
    costs_sha256: str
    costs: CostPolicy
    input_deliveries: tuple[InputReference, ...]
    forecasts: tuple[Forecast, ...]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _require_utc(value: datetime, name: str) -> None:
    if (not isinstance(value, datetime) or value.tzinfo is None
            or value.utcoffset() != timezone.utc.utcoffset(value)):
        raise ValueError(f"{name} debe ser un datetime UTC")


def _sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def _validate_id(publication_id: str) -> None:
    if not isinstance(publication_id, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}", publication_id):
        raise ValueError("publication_id debe ser un identificador simple y estable")


def _input_references(inbox_dir: Path, cutoff_at: datetime) -> tuple[InputReference, ...]:
    return tuple(
        InputReference(path.relative_to(inbox_dir).as_posix(), _sha256(path.read_bytes()))
        for path in select_deliveries(inbox_dir, cutoff_at)
    )


def _validate_publication(item: PublishedForecasts) -> None:
    _validate_id(item.publication_id)
    _require_utc(item.recorded_at, "recorded_at")
    _require_utc(item.prediction_cutoff_at, "prediction_cutoff_at")
    _require_utc(item.model_available_at, "model_available_at")
    _require_utc(item.daily_calibration_available_at, "daily_calibration_available_at")
    if item.kind not in ("real", "historical_simulation"):
        raise ValueError("Tipo de publicación inválido")
    if item.kind == "real":
        _require_utc(item.published_at, "published_at")
        if (item.published_at != item.recorded_at
                or not item.prediction_cutoff_at <= item.published_at
                < datetime.combine(item.forecast_start_date, time.min, tzinfo=timezone.utc)):
            raise ValueError("La publicación real debe ocurrir entre el corte y el inicio de la semana")
    elif item.published_at is not None:
        raise ValueError("Una simulación histórica no tiene fecha de publicación real")
    if (item.prediction_cutoff_at != _prediction_cutoff(item.forecast_start_date)
            or item.model_available_at > item.prediction_cutoff_at
            or item.daily_calibration_available_at > item.prediction_cutoff_at
            or item.recorded_at < item.prediction_cutoff_at):
        raise ValueError("Cortes o procedencia temporal inválidos")
    if (item.model_name not in ("baseline", "ml")
            or any(not re.fullmatch(r"[0-9a-f]{64}", value) for value in (
                item.model_state_sha256, item.daily_calibration_sha256, item.costs_sha256))
            or not item.costs_path):
        raise ValueError("Procedencia del modelo o costos inválida")
    if (not item.input_deliveries or len({ref.path for ref in item.input_deliveries})
            != len(item.input_deliveries) or any(
                not ref.path or Path(ref.path).is_absolute() or ".." in Path(ref.path).parts
                or not re.fullmatch(r"[0-9a-f]{64}", ref.sha256)
                for ref in item.input_deliveries)):
        raise ValueError("Referencias de entregas inválidas")
    if (not item.forecasts or any(forecast.forecast_start_date != item.forecast_start_date
                                  for forecast in item.forecasts)):
        raise ValueError("La publicación requiere pronósticos de un único corte")
    _group_forecasts(list(item.forecasts))
    costs = item.costs
    if (not all(math.isfinite(value) for value in (
            costs.underforecast_per_unit, costs.overforecast_per_unit,
            costs.min_relative_improvement))
            or costs.underforecast_per_unit < 0 or costs.overforecast_per_unit < 0
            or not math.isfinite(costs.underforecast_per_unit + costs.overforecast_per_unit)
            or costs.underforecast_per_unit + costs.overforecast_per_unit <= 0
            or not 0 <= costs.min_relative_improvement < 1):
        raise ValueError("Costos de la publicación inválidos")


def _payload(item: PublishedForecasts) -> dict:
    payload = asdict(item)
    for name in ("recorded_at", "published_at", "prediction_cutoff_at",
                 "model_available_at", "daily_calibration_available_at"):
        if payload[name] is not None:
            payload[name] = payload[name].isoformat()
    payload["forecast_start_date"] = item.forecast_start_date.isoformat()
    for row in payload["forecasts"]:
        row["forecast_start_date"] = row["forecast_start_date"].isoformat()
        row["target_date"] = row["target_date"].isoformat()
        row["prediction_cutoff_at"] = row["prediction_cutoff_at"].isoformat()
    payload["schema_version"] = 1
    return payload


def _parse_payload(payload: dict) -> PublishedForecasts:
    if payload.get("schema_version") != 1:
        raise ValueError("Versión de publicación desconocida")
    try:
        item = PublishedForecasts(
            publication_id=payload["publication_id"], kind=payload["kind"],
            recorded_at=datetime.fromisoformat(payload["recorded_at"]),
            published_at=(datetime.fromisoformat(payload["published_at"])
                          if payload["published_at"] is not None else None),
            forecast_start_date=date.fromisoformat(payload["forecast_start_date"]),
            prediction_cutoff_at=datetime.fromisoformat(payload["prediction_cutoff_at"]),
            model_name=payload["model_name"],
            model_state_sha256=payload["model_state_sha256"],
            model_available_at=datetime.fromisoformat(payload["model_available_at"]),
            daily_calibration_available_at=datetime.fromisoformat(
                payload["daily_calibration_available_at"]),
            daily_calibration_sha256=payload["daily_calibration_sha256"],
            costs_path=payload["costs_path"], costs_sha256=payload["costs_sha256"],
            costs=CostPolicy(**payload["costs"]),
            input_deliveries=tuple(InputReference(**ref) for ref in payload["input_deliveries"]),
            forecasts=tuple(Forecast(
                date.fromisoformat(row["forecast_start_date"]),
                date.fromisoformat(row["target_date"]), row["store_nbr"], row["family"],
                datetime.fromisoformat(row["prediction_cutoff_at"]),
                row["q10"], row["q50"], row["q90"], row["business_target"],
            ) for row in payload["forecasts"]),
        )
    except (KeyError, TypeError, ValueError, AttributeError) as exc:
        raise ValueError("Publicación mal formada") from exc
    _validate_publication(item)
    return item


def _read_envelope(path: Path) -> dict:
    try:
        envelope = json.loads(path.read_text(encoding="utf-8"))
        if (set(envelope) != {"payload", "sha256"}
                or _sha256(_canonical(envelope["payload"])) != envelope["sha256"]):
            raise ValueError("Huella del artefacto no coincide")
        return envelope["payload"]
    except (json.JSONDecodeError, TypeError, AttributeError) as exc:
        raise ValueError(f"Artefacto inválido: {path}") from exc


def _write_immutable(path: Path, payload: dict, *, retry_fields: tuple[str, ...] = ()) -> None:
    """Publish one complete JSON file atomically; never replace an existing file."""
    envelope = {"payload": payload, "sha256": _sha256(_canonical(payload))}
    content = _canonical(envelope) + b"\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, prefix=".staging-", delete=False) as staging:
        staging_path = Path(staging.name)
        try:
            staging.write(content)
            staging.flush()
            os.fsync(staging.fileno())
            try:
                os.link(staging_path, path)
            except FileExistsError:
                existing = _read_envelope(path)
                before = {key: value for key, value in existing.items() if key not in retry_fields}
                after = {key: value for key, value in payload.items() if key not in retry_fields}
                if _canonical(before) != _canonical(after):
                    raise ValueError(f"Conflicto: el artefacto existente difiere: {path}")
        finally:
            staging_path.unlink(missing_ok=True)


def read_publication(publication_id: str, *,
                     published_dir: Path = DEFAULT_PUBLISHED_DIR) -> PublishedForecasts:
    """Read and verify an immutable forecast publication by its stable ID."""
    _validate_id(publication_id)
    path = Path(published_dir) / "forecasts" / f"{publication_id}.json"
    item = _parse_payload(_read_envelope(path))
    if item.publication_id != publication_id:
        raise ValueError("El identificador de la publicación no coincide con su ruta")
    return item


def publish_forecasts(
    inbox_dir: Path, start_date: date, evaluation: EvaluationResult,
    costs_path: Path, publication_id: str, *,
    published_dir: Path = DEFAULT_PUBLISHED_DIR, kind: str = "real",
) -> PublishedForecasts:
    """Generate and atomically record one real publication or explicit simulation.

    The real publication timestamp is taken from the UTC clock at this write;
    callers cannot supply a historical publication timestamp.
    """
    _validate_id(publication_id)
    if not isinstance(start_date, date) or isinstance(start_date, datetime):
        raise ValueError("start_date debe ser una fecha sin hora")
    if kind not in ("real", "historical_simulation"):
        raise ValueError("Tipo de publicación inválido")
    cutoff_at = _prediction_cutoff(start_date)
    selected = require_evaluation_provenance(evaluation, cutoff_at)
    inbox_dir = Path(inbox_dir)
    costs_path = Path(costs_path)
    costs_before = costs_path.read_bytes()
    costs = load_costs(costs_path)
    inputs_before = _input_references(inbox_dir, cutoff_at)
    forecasts = tuple(forecast_week(
        inbox_dir, start_date, cutoff_at, selected.model, selected.daily_calibration, costs,
    ))
    if (costs_path.read_bytes() != costs_before
            or _input_references(inbox_dir, cutoff_at) != inputs_before):
        raise ValueError("Las entradas cambiaron durante la publicación")
    require_evaluation_provenance(evaluation, cutoff_at)
    recorded_at = _utc_now()
    _require_utc(recorded_at, "recorded_at")
    path = Path(published_dir) / "forecasts" / f"{publication_id}.json"
    # A retry preserves the original write time, including after the week begins.
    existing = read_publication(publication_id, published_dir=published_dir) if path.exists() else None
    if existing is not None:
        recorded_at = existing.recorded_at
    elif recorded_at < cutoff_at:
        raise ValueError("La fecha de corte aún no ha ocurrido")
    item = PublishedForecasts(
        publication_id, kind, recorded_at, recorded_at if kind == "real" else None,
        start_date, cutoff_at, selected.name, selected.model_state_sha256,
        selected.model_available_at, selected.daily_calibration_available_at,
        _sha256(_canonical(selected.daily_calibration.residuals)),
        costs_path.as_posix(), _sha256(costs_before), costs, inputs_before,
        tuple(sorted(forecasts, key=lambda row: (row.store_nbr, row.family, row.target_date))),
    )
    _validate_publication(item)
    _write_immutable(path, _payload(item), retry_fields=("recorded_at", "published_at"))
    return read_publication(publication_id, published_dir=published_dir)
