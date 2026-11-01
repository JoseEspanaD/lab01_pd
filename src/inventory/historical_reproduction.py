"""Build and verify one DVC-ready, explicitly simulated historical forecast."""

import argparse
import csv
import hashlib
import json
import math
import re
import subprocess
from dataclasses import fields, is_dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from .data_preparation import Features, ForecastExample, _prediction_cutoff
from .dated_source import publish_delivery
from .delivery_selector import list_deliveries
from .forecasting import (
    MeanBaseline, ResidualCalibration, RidgeForecaster,
    evaluate_models, forecast_week, load_costs, model_state_payload,
    model_state_sha256,
)
from .holidays_events import simulate_holidays_delivery
from .oil import simulate_oil_delivery
from .published_forecasts import (
    _canonical, _input_references, _sha256, _write_immutable,
    publish_forecasts, read_publication,
)
from .stores import simulate_stores_delivery
from .train_schema import TRAIN_COLUMNS, parse_train_row, validate_train_header
from .transactions import simulate_transactions_delivery


ROOT = Path(__file__).resolve().parents[2]
RUN_ID = "historical-20170801-store1-grocery-i"
RUN_DIR = Path("data/reproductions") / RUN_ID
INBOX = RUN_DIR / "inbox"
PROCESSED = RUN_DIR / "processed.json"
MODEL = Path("models") / f"{RUN_ID}.json"
PUBLICATION = Path("data/published/forecasts") / f"{RUN_ID}.json"
MANIFEST = RUN_DIR / "manifest.json"
ARTIFACTS = (INBOX, PROCESSED, MODEL, PUBLICATION)
RAW_NAMES = ("train", "transactions", "oil", "holidays_events", "stores")
START = date(2017, 8, 1)
CUTOFF = _prediction_cutoff(START)
FIRST_OBSERVED = date(2017, 5, 27)
LAST_OBSERVED = date(2017, 7, 30)
SPLITS = {
    "train_start": date(2017, 6, 4),
    "validation_start": date(2017, 6, 18),
    "selection_start": date(2017, 7, 2),
    "test_start": date(2017, 7, 16),
    "last_test_start": date(2017, 7, 16),
}
REPORT_CUTOFF = datetime(2017, 7, 24, 6, tzinfo=timezone.utc)
COSTS = Path("config/forecast_costs.json")


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _jsonable(value):
    if is_dataclass(value):
        return {field.name: _jsonable(getattr(value, field.name)) for field in fields(value)}
    if isinstance(value, (datetime, date, Decimal)):
        return str(value) if isinstance(value, Decimal) else value.isoformat()
    if isinstance(value, (tuple, list)):
        return [_jsonable(item) for item in value]
    if isinstance(value, dict):
        return {key: _jsonable(item) for key, item in value.items()}
    return value


def _read_payload(path: Path) -> dict:
    try:
        envelope = json.loads(path.read_text(encoding="utf-8"))
        if (set(envelope) != {"payload", "sha256"}
                or _sha256(_canonical(envelope["payload"])) != envelope["sha256"]):
            raise ValueError(f"Huella inválida: {path}")
        return envelope["payload"]
    except (json.JSONDecodeError, TypeError, KeyError) as exc:
        raise ValueError(f"Artefacto inválido: {path}") from exc


def _pointer(path: Path) -> Path:
    return path.with_name(path.name + ".dvc")


def _dvc_output(path: Path) -> tuple[str, int]:
    """Read the simple single-output DVC pointer without adding a YAML dependency."""
    text = _pointer(path).read_text(encoding="utf-8")
    md5 = re.findall(r"(?m)^\s*-?\s*md5:\s+([0-9a-f]{32}(?:\.dir)?)\s*$", text)
    size = re.findall(r"(?m)^\s+size:\s+([0-9]+)\s*$", text)
    out = re.findall(r"(?m)^\s+path:\s+([^\s]+)\s*$", text)
    if len(md5) != 1 or len(size) != 1 or out != [path.name]:
        raise ValueError(f"Puntero DVC inválido: {_pointer(path)}")
    return md5[0], int(size[0])


def _raw_pointers(root: Path) -> dict:
    result = {}
    for name in RAW_NAMES:
        path = Path("data/raw") / f"{name}.csv"
        md5, size = _dvc_output(root / path)
        raw = root / path
        if not raw.is_file() or raw.stat().st_size != size:
            raise ValueError(f"Falta o cambió el CSV original: {path}")
        actual_md5 = hashlib.md5(raw.read_bytes()).hexdigest()
        if actual_md5 != md5:
            raise ValueError(f"MD5 original inválido: {path}")
        result[path.as_posix()] = {
            "md5": md5, "size": size, "pointer_sha256": _digest(_pointer(root / path)),
        }
    return result


def _simulate_train_range(raw: Path, inbox: Path) -> None:
    """Select the documented demo series in one pass through the large train CSV."""
    batches: dict[date, list[list[str]]] = {}
    with raw.open(newline="", encoding="utf-8") as source:
        reader = csv.reader(source)
        validate_train_header(next(reader, ()))
        for row in reader:
            sale = parse_train_row(row, reader.line_num)
            if (FIRST_OBSERVED <= sale.date <= LAST_OBSERVED
                    and sale.store_nbr == 1 and sale.family == "GROCERY I"):
                batch = batches.setdefault(sale.date, [])
                if batch:
                    raise ValueError(f"Venta duplicada para {sale.date}")
                batch.append(row)
    for observed, rows in sorted(batches.items()):
        publish_delivery(
            "train", observed,
            datetime.combine(observed + timedelta(days=1), time(6), tzinfo=timezone.utc),
            1, TRAIN_COLUMNS, rows, inbox,
        )


def build_snapshot(root: Path = ROOT) -> None:
    """Create immutable D+1/T0 deliveries, never writing to the product inbox."""
    root = Path(root)
    _raw_pointers(root)
    inbox = root / INBOX
    _simulate_train_range(root / "data/raw/train.csv", inbox)
    simulate_stores_delivery(root / "data/raw/stores.csv", inbox)
    observed = FIRST_OBSERVED
    while observed <= LAST_OBSERVED:
        for name, simulate in (
            ("transactions", simulate_transactions_delivery),
            ("oil", simulate_oil_delivery),
            ("holidays_events", simulate_holidays_delivery),
        ):
            simulate(observed, root / f"data/raw/{name}.csv", inbox)
        observed += timedelta(days=1)
    _snapshot_files(root)


def _prepared_row(payload: dict) -> ForecastExample:
    feature_values = dict(payload["features"])
    for name in ("last_sales", "mean_sales_7d", "last_oil_price"):
        if feature_values[name] is not None:
            feature_values[name] = Decimal(feature_values[name])
    return ForecastExample(
        date.fromisoformat(payload["forecast_start_date"]),
        date.fromisoformat(payload["target_date"]), payload["store_nbr"],
        payload["family"], datetime.fromisoformat(payload["prediction_cutoff_at"]),
        Features(**feature_values),
        Decimal(payload["label_sales"]) if payload["label_sales"] is not None else None,
    )


def _evaluation(root: Path):
    return evaluate_models(
        root / INBOX, **SPLITS, report_cutoff_at=REPORT_CUTOFF,
        costs_path=root / COSTS,
    )


def build_artifacts(root: Path = ROOT) -> None:
    """Persist the exact partitions, selected model/calibration and simulation."""
    root = Path(root)
    _snapshot_files(root)
    evaluation = _evaluation(root)
    if evaluation.partitions is None:
        raise ValueError("La evaluación no conserva particiones")
    if evaluation.selected_forecast.model_available_at > CUTOFF:
        raise ValueError("Modelo posterior al corte histórico")
    if evaluation.selected_forecast.daily_calibration_available_at > CUTOFF:
        raise ValueError("Calibración posterior al corte histórico")
    _write_immutable(root / PROCESSED, {
        "schema_version": 1,
        "split_dates": {key: day.isoformat() for key, day in SPLITS.items()},
        "report_cutoff_at": REPORT_CUTOFF.isoformat(),
        "partitions": _jsonable(evaluation.partitions),
    })
    selected = evaluation.selected_forecast
    _write_immutable(root / MODEL, {
        "schema_version": 1, "name": selected.name,
        "state": model_state_payload(selected.model),
        "state_sha256": selected.model_state_sha256,
        "daily_residuals": list(selected.daily_calibration.residuals),
        "model_available_at": selected.model_available_at.isoformat(),
        "daily_calibration_available_at": selected.daily_calibration_available_at.isoformat(),
        "selected_penalty": evaluation.ridge_penalty,
    })
    publish_forecasts(
        root / INBOX, START, evaluation, root / COSTS, RUN_ID,
        published_dir=root / "data/published", kind="historical_simulation",
    )


def _snapshot_files(root: Path) -> dict[str, str]:
    inbox = root / INBOX
    deliveries = list_deliveries(inbox, CUTOFF)
    paths = sorted(inbox.rglob("data.csv"))
    if len(paths) != len(deliveries):
        raise ValueError("La instantánea contiene entregas no visibles al corte")
    return {path.relative_to(inbox).as_posix(): _digest(path) for path in paths}


def _code_files(root: Path) -> dict[str, str]:
    paths = sorted((root / "src/inventory").glob("*.py"))
    paths += [root / "pyproject.toml", root / COSTS]
    return {path.relative_to(root).as_posix(): _digest(path) for path in paths}


def write_manifest(root: Path = ROOT, *, source_git_commit: str | None = None) -> None:
    """Link DVC pointers to content; a pending draft may be finalized once."""
    root = Path(root)
    if source_git_commit is not None and not re.fullmatch(r"[0-9a-f]{40}", source_git_commit):
        raise ValueError("source_git_commit inválido")
    raw = _raw_pointers(root)
    pointers = {}
    for artifact in ARTIFACTS:
        md5, size = _dvc_output(root / artifact)
        pointers[artifact.as_posix()] = {
            "md5": md5, "size": size,
            "pointer_sha256": _digest(_pointer(root / artifact)),
        }
    payload = {
        "schema_version": 1, "kind": "historical_simulation", "run_id": RUN_ID,
        "source_git_commit": source_git_commit,
        "forecast_start_date": START.isoformat(), "prediction_cutoff_at": CUTOFF.isoformat(),
        "report_cutoff_at": REPORT_CUTOFF.isoformat(),
        "split_dates": {key: day.isoformat() for key, day in SPLITS.items()},
        "series": {"store_nbr": 1, "family": "GROCERY I"},
        "availability_assumptions": {"dated_sources": "D+1 06:00 UTC", "stores": "T0 2013-01-01 UTC"},
        "raw_dvc": raw, "artifact_dvc": pointers,
        "snapshot_files_sha256": _snapshot_files(root),
        "processed_sha256": _digest(root / PROCESSED),
        "model_sha256": _digest(root / MODEL),
        "publication_sha256": _digest(root / PUBLICATION),
        "code_files_sha256": _code_files(root),
    }
    path = root / MANIFEST
    if path.exists():
        existing = _read_payload(path)
        if existing.get("source_git_commit") is None:
            unchanged = {key: value for key, value in existing.items()
                         if key != "source_git_commit"}
            current = {key: value for key, value in payload.items()
                       if key != "source_git_commit"}
            if source_git_commit is not None and unchanged != current:
                raise ValueError("El borrador de linaje cambió antes de fijar la revisión Git")
            if existing != payload:
                path.unlink()  # Only the uncommitted, Git-unfixed draft is replaceable.
    _write_immutable(path, payload)


def _load_model(payload: dict):
    state = payload["state"]
    if payload["name"] == "baseline" and state == {"type": "mean_baseline"}:
        model = MeanBaseline()
    elif payload["name"] == "ml" and set(state) == {
        "type", "penalty", "means", "scales", "coefficients"
    } and state["type"] == "ridge":
        model = RidgeForecaster(state["penalty"])
        model.means = state["means"]
        model.scales = state["scales"]
        model.coefficients = state["coefficients"]
        if (not all(isinstance(values, list) for values in (
                model.means, model.scales, model.coefficients))
                or len(model.means) != 45 or len(model.scales) != 45
                or len(model.coefficients) != 46
                or any(type(value) not in (int, float) or not math.isfinite(value)
                       for values in (model.means, model.scales, model.coefficients)
                       for value in values)
                or any(value <= 0 for value in model.scales)):
            raise ValueError("Dimensiones del modelo inválidas")
    else:
        raise ValueError("Estado del modelo inválido")
    if model_state_sha256(model) != payload["state_sha256"]:
        raise ValueError("Huella del modelo inválida")
    residuals = payload["daily_residuals"]
    if (not isinstance(residuals, list) or not residuals
            or any(type(item) not in (int, float) or not math.isfinite(item)
                   for item in residuals)
            or residuals != sorted(residuals)):
        raise ValueError("Calibración inválida")
    return model, ResidualCalibration(tuple(residuals))


def verify(root: Path = ROOT, *, require_git_revision: bool = True) -> dict:
    """Replay evaluation and predictions from archived inputs, checking lineage."""
    root = Path(root)
    manifest = _read_payload(root / MANIFEST)
    if (manifest["schema_version"] != 1 or manifest["kind"] != "historical_simulation"
            or manifest["run_id"] != RUN_ID
            or manifest["forecast_start_date"] != START.isoformat()
            or manifest["prediction_cutoff_at"] != CUTOFF.isoformat()
            or manifest["report_cutoff_at"] != REPORT_CUTOFF.isoformat()
            or manifest["series"] != {"store_nbr": 1, "family": "GROCERY I"}):
        raise ValueError("Escenario histórico inválido")
    if require_git_revision:
        commit = manifest["source_git_commit"]
        if not commit:
            raise ValueError("Revisión Git del código pendiente de fijar")
        ancestor = subprocess.run(["git", "merge-base", "--is-ancestor", commit, "HEAD"],
                                  cwd=root, capture_output=True, check=False)
        if ancestor.returncode != 0:
            raise ValueError("La revisión Git del código no está en esta rama")
        for path, digest in manifest["code_files_sha256"].items():
            committed = subprocess.run(["git", "show", f"{commit}:{path}"],
                                       cwd=root, capture_output=True, check=False)
            if committed.returncode != 0 or _sha256(committed.stdout) != digest:
                raise ValueError(f"Código no coincide con la revisión Git: {path}")
    if _raw_pointers(root) != manifest["raw_dvc"]:
        raise ValueError("Linaje de CSV originales inválido")
    if _code_files(root) != manifest["code_files_sha256"]:
        raise ValueError("Código o configuración distintos del manifiesto")
    for artifact in ARTIFACTS:
        md5, size = _dvc_output(root / artifact)
        expected = manifest["artifact_dvc"][artifact.as_posix()]
        if (expected != {"md5": md5, "size": size,
                         "pointer_sha256": _digest(_pointer(root / artifact))}):
            raise ValueError(f"Puntero DVC distinto: {artifact}")
    if _snapshot_files(root) != manifest["snapshot_files_sha256"]:
        raise ValueError("Instantánea distinta del manifiesto")
    for path, name in ((PROCESSED, "processed"), (MODEL, "model"),
                       (PUBLICATION, "publication")):
        if _digest(root / path) != manifest[f"{name}_sha256"]:
            raise ValueError(f"Artefacto distinto: {path}")
    processed = _read_payload(root / PROCESSED)
    model_data = _read_payload(root / MODEL)
    if (processed["schema_version"] != 1 or model_data["schema_version"] != 1
            or processed["split_dates"] != manifest["split_dates"]
            or processed["report_cutoff_at"] != REPORT_CUTOFF.isoformat()):
        raise ValueError("Particiones o cortes distintos del manifiesto")
    evaluation = _evaluation(root)
    if evaluation.partitions is None or _jsonable(evaluation.partitions) != processed["partitions"]:
        raise ValueError("Las particiones preparadas no se reproducen")
    for name in ("train", "calibration", "selection", "test"):
        decoded = tuple(_prepared_row(row) for row in processed["partitions"][name])
        if decoded != getattr(evaluation.partitions, name):
            raise ValueError(f"Partición serializada inválida: {name}")
    selected = evaluation.selected_forecast
    archived_model, archived_calibration = _load_model(model_data)
    if (selected.name != model_data["name"]
            or selected.model_state_sha256 != model_data["state_sha256"]
            or evaluation.ridge_penalty != model_data["selected_penalty"]
            or list(selected.daily_calibration.residuals) != list(archived_calibration.residuals)
            or selected.model_available_at.isoformat() != model_data["model_available_at"]
            or selected.daily_calibration_available_at.isoformat()
            != model_data["daily_calibration_available_at"]):
        raise ValueError("La selección del modelo no se reproduce")
    publication = read_publication(RUN_ID, published_dir=root / "data/published")
    if publication.kind != "historical_simulation" or publication.published_at is not None:
        raise ValueError("La publicación no es una simulación histórica")
    costs = load_costs(root / COSTS)
    replayed = forecast_week(root / INBOX, START, CUTOFF,
                             archived_model, archived_calibration, costs)
    replayed.sort(key=lambda row: (row.store_nbr, row.family, row.target_date))
    if tuple(replayed) != publication.forecasts:
        raise ValueError("Las predicciones históricas no se reproducen")
    if (publication.model_name != model_data["name"]
            or publication.model_state_sha256 != model_data["state_sha256"]
            or publication.prediction_cutoff_at != CUTOFF
            or publication.costs_sha256 != _digest(root / COSTS)
            or publication.costs != costs
            or publication.daily_calibration_sha256
            != _sha256(_canonical(archived_calibration.residuals))
            or publication.input_deliveries != _input_references(root / INBOX, CUTOFF)
            or publication.model_available_at.isoformat() != model_data["model_available_at"]
            or publication.daily_calibration_available_at.isoformat()
            != model_data["daily_calibration_available_at"]):
        raise ValueError("Procedencia temporal inválida")
    return {"publication_id": RUN_ID, "forecast_rows": len(replayed),
            "selected_model": selected.name, "cutoff_at": CUTOFF.isoformat()}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("build", "manifest", "verify"))
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--source-git-commit", default=None)
    parser.add_argument("--allow-pending-git", action="store_true")
    args = parser.parse_args()
    if args.action == "build":
        build_snapshot(args.root)
        build_artifacts(args.root)
    elif args.action == "manifest":
        write_manifest(args.root, source_git_commit=args.source_git_commit)
    else:
        print(json.dumps(verify(args.root, require_git_revision=not args.allow_pending_git),
                         sort_keys=True))


if __name__ == "__main__":
    main()
