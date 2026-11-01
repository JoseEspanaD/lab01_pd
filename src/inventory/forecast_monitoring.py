"""Score stored forecasts against sales visible at a chosen UTC instant."""

import math
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path

from .published_forecasts import (
    DEFAULT_PUBLISHED_DIR, InputReference, _canonical, _input_references,
    _payload as _publication_payload,
    _read_envelope, _require_utc, _sha256, _utc_now, _validate_id, _write_immutable,
    read_publication,
)
from .sales_ingestion import ingest_sales


@dataclass(frozen=True)
class MonitoredForecast:
    target_date: date
    store_nbr: int
    family: str
    status: str
    actual_sales: float | None
    error: float | None  # actual minus the published q50


@dataclass(frozen=True)
class MonitoringMetrics:
    evaluated_pairs: int
    pending_pairs: int
    mae: float | None
    rmse: float | None
    bias: float | None
    coverage_80: float | None


@dataclass(frozen=True)
class MonitoringReport:
    publication_id: str
    publication_kind: str
    publication_sha256: str
    evaluation_at: datetime
    recorded_at: datetime
    sales_deliveries: tuple[InputReference, ...]
    rows: tuple[MonitoredForecast, ...]
    metrics: MonitoringMetrics


def _report_path(published_dir: Path, publication_id: str, evaluation_at: datetime) -> Path:
    _validate_id(publication_id)
    instant = evaluation_at.strftime("%Y-%m-%dT%H%M%S.%fZ")
    return Path(published_dir) / "monitoring" / publication_id / f"{instant}.json"


def _payload(report: MonitoringReport) -> dict:
    result = asdict(report)
    result["evaluation_at"] = report.evaluation_at.isoformat()
    result["recorded_at"] = report.recorded_at.isoformat()
    for row in result["rows"]:
        row["target_date"] = row["target_date"].isoformat()
    result["schema_version"] = 1
    return result


def read_report(publication_id: str, evaluation_at: datetime, *,
                published_dir: Path = DEFAULT_PUBLISHED_DIR) -> MonitoringReport:
    """Read one previously recorded as-of result without consulting current sales."""
    _require_utc(evaluation_at, "evaluation_at")
    payload = _read_envelope(_report_path(published_dir, publication_id, evaluation_at))
    if payload.get("schema_version") != 1:
        raise ValueError("Versión de reporte desconocida")
    try:
        report = MonitoringReport(
            publication_id=payload["publication_id"],
            publication_kind=payload["publication_kind"],
            publication_sha256=payload["publication_sha256"],
            evaluation_at=datetime.fromisoformat(payload["evaluation_at"]),
            recorded_at=datetime.fromisoformat(payload["recorded_at"]),
            sales_deliveries=tuple(InputReference(**ref) for ref in payload["sales_deliveries"]),
            rows=tuple(MonitoredForecast(
                date.fromisoformat(row["target_date"]), row["store_nbr"], row["family"],
                row["status"], row["actual_sales"], row["error"],
            ) for row in payload["rows"]),
            metrics=MonitoringMetrics(**payload["metrics"]),
        )
    except (KeyError, TypeError, ValueError, AttributeError) as exc:
        raise ValueError("Reporte mal formado") from exc
    if report.publication_id != publication_id or report.evaluation_at != evaluation_at:
        raise ValueError("La identidad del reporte no coincide con su ruta")
    return report


def evaluate_publication(inbox_dir: Path, publication_id: str, evaluation_at: datetime, *,
                         published_dir: Path = DEFAULT_PUBLISHED_DIR) -> MonitoringReport:
    """Persist one immutable as-of score of the original published predictions."""
    _require_utc(evaluation_at, "evaluation_at")
    publication = read_publication(publication_id, published_dir=published_dir)
    recorded_at = _utc_now()
    _require_utc(recorded_at, "recorded_at")
    earliest = (publication.published_at if publication.kind == "real"
                else publication.prediction_cutoff_at)
    if not earliest <= evaluation_at <= recorded_at:
        raise ValueError("La fecha de evaluación debe seguir a la publicación y no ser futura")
    inbox_dir = Path(inbox_dir)
    targets = {forecast.target_date for forecast in publication.forecasts}
    before = tuple(ref for ref in _input_references(inbox_dir, evaluation_at)
                   if ref.path.startswith("source=train/")
                   and any(f"observation_date={day.isoformat()}/" in ref.path for day in targets))
    actuals = {
        (sale.date, sale.store_nbr, sale.family): float(sale.sales)
        for sale in ingest_sales(inbox_dir, evaluation_at)
        if sale.date in targets
    }
    after = tuple(ref for ref in _input_references(inbox_dir, evaluation_at)
                  if ref.path.startswith("source=train/")
                  and any(f"observation_date={day.isoformat()}/" in ref.path for day in targets))
    if before != after:
        raise ValueError("Las ventas seleccionadas cambiaron durante la evaluación")
    if any(not math.isfinite(value) for value in actuals.values()):
        raise ValueError("Las ventas observadas exceden el rango numérico evaluable")
    rows = []
    errors = []
    covered = 0
    for forecast in publication.forecasts:
        actual = actuals.get((forecast.target_date, forecast.store_nbr, forecast.family))
        if actual is None:
            rows.append(MonitoredForecast(
                forecast.target_date, forecast.store_nbr, forecast.family,
                "pending", None, None,
            ))
        else:
            error = actual - forecast.q50
            errors.append(error)
            covered += forecast.q10 <= actual <= forecast.q90
            rows.append(MonitoredForecast(
                forecast.target_date, forecast.store_nbr, forecast.family,
                "observed", actual, error,
            ))
    count = len(errors)
    metrics = MonitoringMetrics(
        count, len(rows) - count,
        math.fsum(abs(error) for error in errors) / count if count else None,
        math.sqrt(math.fsum(error * error for error in errors) / count) if count else None,
        math.fsum(errors) / count if count else None,
        covered / count if count else None,
    )
    # The publication hash binds the report to exactly the predictions scored.
    publication_sha256 = _sha256(_canonical(_publication_payload(publication)))
    report = MonitoringReport(
        publication_id, publication.kind, publication_sha256,
        evaluation_at, recorded_at, before, tuple(rows), metrics,
    )
    path = _report_path(published_dir, publication_id, evaluation_at)
    _write_immutable(path, _payload(report), retry_fields=("recorded_at",))
    return read_report(publication_id, evaluation_at, published_dir=published_dir)
