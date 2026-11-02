"""Freeze and verify one immutable Airflow publication for host-side DVC tracking."""

from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import date, timedelta
import hashlib
import math
import os
from pathlib import Path
import shutil
import tempfile

from inventory.delivery_selector import list_deliveries
from inventory.forecast_monitoring import _report_path, evaluate_publication, read_report
from inventory.forecasting import evaluate_models, forecast_week, model_state_payload
from inventory.historical_reproduction import _load_model
from inventory.operations.ingestion import parse_cutoff_at
from inventory.operations.model_record import weekly_residuals
from inventory.operations.preparation import _history_references, _selected_references
from inventory.operations.weekly_forecast import _jsonable, _require_preparation
from inventory.ordering import suggest_weekly_orders
from inventory.published_forecasts import (
    _canonical, _read_envelope, _sha256, _validate_id, _write_immutable,
    read_publication,
)


def _digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def _source_files(root: Path, publication_id: str, evaluation_at) -> tuple[dict[str, Path], dict]:
    """Select only immutable outputs and deliveries visible by evaluation time."""
    _validate_id(publication_id)
    published = root / "data/published"
    publication = read_publication(publication_id, published_dir=published)
    if evaluation_at < publication.prediction_cutoff_at:
        raise ValueError("La evaluación precede al corte de pronóstico")
    cutoff = publication.prediction_cutoff_at
    token = cutoff.strftime("%Y-%m-%dT%H%M%SZ")
    order = _read_envelope(published / "orders" / f"{publication_id}.json")
    if (order.get("publication_id") != publication_id
            or order.get("cutoff_at") != cutoff.isoformat()
            or order.get("forecast_start_date") != publication.forecast_start_date.isoformat()):
        raise ValueError("Los pedidos no corresponden a la publicación")
    train_start = date.fromisoformat(order["evaluation_splits"]["train_start"])
    preparation = (f"data/processed/preparation/{token}/"
                   f"{train_start.isoformat()}_{(publication.forecast_start_date - timedelta(days=1)).isoformat()}.json")
    monitor = _report_path(published, publication_id, evaluation_at)
    files = {
        f"processed/ingestion/{token}.json": root / "data/processed/ingestion" / f"{token}.json",
        preparation.removeprefix("data/"): root / preparation,
        "model/model.json": published / "model_states" / f"{publication_id}.json",
        f"published/forecasts/{publication_id}.json": published / "forecasts" / f"{publication_id}.json",
        f"published/orders/{publication_id}.json": published / "orders" / f"{publication_id}.json",
        f"published/monitoring/{publication_id}/{monitor.name}": monitor,
    }
    inbox = root / "data/inbox"
    evaluation_deliveries = list_deliveries(inbox, evaluation_at)
    for delivery in evaluation_deliveries:
        relative = delivery.path.relative_to(inbox).as_posix()
        files[f"inbox/{relative}"] = delivery.path
    if not evaluation_deliveries:
        raise ValueError("No existen entregas hasta la evaluación")
    details = {
        "schema_version": 1,
        "publication_id": publication_id,
        "forecast_start_date": publication.forecast_start_date.isoformat(),
        "prediction_cutoff_at": cutoff.isoformat(),
        "evaluation_at": evaluation_at.isoformat(),
        "decision_deliveries": [
            item.path.relative_to(inbox).as_posix()
            for item in list_deliveries(inbox, cutoff)
        ],
        "evaluation_deliveries": [
            item.path.relative_to(inbox).as_posix() for item in evaluation_deliveries
        ],
        "preparation_start_date": train_start.isoformat(),
        "preparation_end_date": (publication.forecast_start_date - timedelta(days=1)).isoformat(),
    }
    return files, details


def _code_hashes(root: Path) -> dict[str, str]:
    paths = sorted((root / "src/inventory").rglob("*.py"))
    paths += [root / "airflow/dags/inventory_operational_flow.py",
              root / "config/forecast_costs.json", root / "pyproject.toml"]
    return {path.relative_to(root).as_posix(): _digest(path) for path in paths}


def _raw_pointers(root: Path) -> dict[str, str]:
    paths = [root / "data/raw" / f"{source}.csv.dvc" for source in
             ("train", "transactions", "oil", "holidays_events", "stores")]
    return {path.relative_to(root).as_posix(): _digest(path) for path in paths}


def freeze(root: Path, archive_root: Path, publication_id: str, evaluation_text: str,
           *, repo_root: Path | None = None) -> Path:
    """Make one complete, immutable copy; an identical repeat reuses it."""
    root, archive_root = Path(root).resolve(), Path(archive_root).resolve()
    repo_root = Path(repo_root).resolve() if repo_root is not None else root
    evaluation_at = parse_cutoff_at(evaluation_text)
    files, details = _source_files(root, publication_id, evaluation_at)
    source_hashes = {}
    for name, source in files.items():
        if source.is_symlink() or not source.is_file():
            raise ValueError(f"Archivo de origen inválido: {source}")
        source_hashes[name] = _digest(source)
    payload = {
        **details,
        "files_sha256": dict(sorted(source_hashes.items())),
        "raw_dvc_sha256": _raw_pointers(repo_root),
        "code_sha256": _code_hashes(repo_root),
        "availability_assumptions": {"dated_sources": "D+1 06:00 UTC", "stores": "T0"},
    }
    token = evaluation_at.strftime("%Y%m%dT%H%M%SZ")
    destination = archive_root / f"{publication_id}__eval-{token}"
    if destination.exists():
        if _read_envelope(destination / "manifest.json") != payload:
            raise ValueError(f"Conflicto: la congelación existente difiere: {destination}")
        verify(destination, repo_root)
        return destination
    archive_root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".staging-", dir=archive_root) as staging:
        staged = Path(staging)
        for name, source in files.items():
            target = staged / name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, target)
            if _digest(target) != source_hashes[name] or _digest(source) != source_hashes[name]:
                raise ValueError(f"El origen cambió durante la congelación: {source}")
        current_files, current_details = _source_files(root, publication_id, evaluation_at)
        if (current_details != details or set(current_files) != set(files)
                or any(_digest(source) != source_hashes[name]
                       for name, source in current_files.items())):
            raise ValueError("Las entregas o salidas cambiaron durante la congelación")
        _write_immutable(staged / "manifest.json", payload)
        verify(staged, repo_root)
        try:
            os.rename(staged, destination)
        except FileExistsError:
            if _read_envelope(destination / "manifest.json") != payload:
                raise ValueError(f"Conflicto: la congelación existente difiere: {destination}")
            verify(destination, repo_root)
    return destination


def verify(archive: Path, root: Path) -> dict:
    """Verify hashes, UTC lineage and outputs without writing into the archive."""
    archive, root = Path(archive), Path(root)
    manifest = _read_envelope(archive / "manifest.json")
    if manifest.get("schema_version") != 1:
        raise ValueError("Manifiesto operativo inválido")
    publication_id = manifest["publication_id"]
    _validate_id(publication_id)
    cutoff = parse_cutoff_at(manifest["prediction_cutoff_at"])
    evaluation_at = parse_cutoff_at(manifest["evaluation_at"])
    if (evaluation_at < cutoff
            or manifest["forecast_start_date"] != (cutoff.date() + timedelta(days=1)).isoformat()):
        raise ValueError("Identificador o cortes del manifiesto inválidos")
    if _raw_pointers(root) != manifest["raw_dvc_sha256"]:
        raise ValueError("Los punteros DVC de los CSV originales difieren")
    if _code_hashes(root) != manifest["code_sha256"]:
        raise ValueError("El código o los costos difieren de la congelación")
    actual = sorted(path for path in archive.rglob("*") if path.is_file())
    if any(path.is_symlink() for path in archive.rglob("*")):
        raise ValueError("La congelación contiene enlaces simbólicos")
    names = {path.relative_to(archive).as_posix() for path in actual}
    if names != set(manifest["files_sha256"]) | {"manifest.json"}:
        raise ValueError("La lista de archivos congelados difiere")
    if any(_digest(archive / name) != digest
           for name, digest in manifest["files_sha256"].items()):
        raise ValueError("La huella de un archivo congelado difiere")
    inbox = archive / "inbox"
    all_deliveries = list_deliveries(inbox, evaluation_at)
    all_paths = [item.path.relative_to(inbox).as_posix() for item in all_deliveries]
    if (all_paths != manifest["evaluation_deliveries"]
            or sorted(name.removeprefix("inbox/") for name in names if name.startswith("inbox/"))
            != sorted(all_paths)
            or [item.path.relative_to(inbox).as_posix()
                for item in list_deliveries(inbox, cutoff)] != manifest["decision_deliveries"]):
        raise ValueError("Las entregas no respetan los cortes declarados")
    processed = archive / "processed"
    target = date.fromisoformat(manifest["forecast_start_date"])
    train_start = date.fromisoformat(manifest["preparation_start_date"])
    prepared, _ = _require_preparation(inbox, processed, train_start, target, cutoff)
    if prepared["end_date"] != manifest["preparation_end_date"]:
        raise ValueError("La preparación no corresponde al período")
    published = archive / "published"
    publication = read_publication(publication_id, published_dir=published)
    order = _read_envelope(published / "orders" / f"{publication_id}.json")
    model_data = _read_envelope(archive / "model/model.json")
    if (publication.prediction_cutoff_at != cutoff
            or publication.forecast_start_date != target
            or order["publication_id"] != publication_id
            or order["cutoff_at"] != cutoff.isoformat()
            or order["forecast_sha256"] != _digest(published / "forecasts" / f"{publication_id}.json")
            or order["preparation_sha256"] != _digest(
                processed / "preparation" / cutoff.strftime("%Y-%m-%dT%H%M%SZ")
                / f"{train_start.isoformat()}_{(target - timedelta(days=1)).isoformat()}.json")
            or model_data["publication_id"] != publication_id
            or model_data["cutoff_at"] != cutoff.isoformat()
            or model_data["name"] != publication.model_name
            or model_data["state_sha256"] != publication.model_state_sha256
            or _sha256(_canonical(model_data["daily_residuals"]))
            != publication.daily_calibration_sha256
            or model_data["costs_sha256"] != publication.costs_sha256):
        raise ValueError("Modelo, preparación, pronósticos o pedidos no coinciden")
    archived_model, archived_calibration = _load_model(model_data)
    if (not isinstance(model_data["weekly_residuals"], list)
            or not model_data["weekly_residuals"]
            or any(type(value) not in (int, float) or not math.isfinite(value)
                   for value in model_data["weekly_residuals"])):
        raise ValueError("Calibración semanal inválida")
    splits = {key: date.fromisoformat(value) for key, value in order["evaluation_splits"].items()}
    costs_path = root / "config/forecast_costs.json"
    evaluation = evaluate_models(
        inbox, *(splits[key] for key in
                 ("train_start", "validation_start", "selection_start", "test_start", "last_test_start")),
        parse_cutoff_at(order["report_cutoff_at"]), costs_path,
    )
    selected = evaluation.selected_forecast
    if (selected.name != model_data["name"]
            or model_state_payload(selected.model) != model_data["state"]
            or list(selected.daily_calibration.residuals) != model_data["daily_residuals"]
            or selected.model_available_at.isoformat() != model_data["model_available_at"]
            or selected.daily_calibration_available_at.isoformat()
            != model_data["daily_calibration_available_at"]
            or evaluation.ridge_penalty != model_data["ridge_penalty"]
            or asdict(evaluation.validation_baseline) != model_data["selection_baseline"]
            or asdict(evaluation.validation_ml) != model_data["selection_ml"]
            or asdict(evaluation.test_baseline) != model_data["test_baseline"]
            or asdict(evaluation.test_ml) != model_data["test_ml"]):
        raise ValueError("La evaluación no reproduce el modelo seleccionado")
    forecasts = forecast_week(inbox, target, cutoff, archived_model, archived_calibration,
                              evaluation.costs)
    if tuple(sorted(forecasts, key=lambda row: (row.store_nbr, row.family, row.target_date))) != publication.forecasts:
        raise ValueError("Los pronósticos no se reproducen")
    orders = suggest_weekly_orders(
        inbox, target, cutoff, evaluation,
        splits["calibration_first_start"], splits["calibration_last_start"], costs_path,
    )
    if ([_jsonable(asdict(item)) for item in orders] != order["orders"]
            or len(orders) != order["order_count"]
            or weekly_residuals(inbox, target, cutoff, evaluation,
                                splits["calibration_first_start"],
                                splits["calibration_last_start"], costs_path, orders)
            != model_data["weekly_residuals"]):
        raise ValueError("Los pedidos o su calibración semanal no se reproducen")
    report = read_report(publication_id, evaluation_at, published_dir=published)
    with tempfile.TemporaryDirectory(prefix="lab1-archive-check-") as temp:
        temp_published = Path(temp) / "published"
        (temp_published / "forecasts").mkdir(parents=True)
        shutil.copyfile(published / "forecasts" / f"{publication_id}.json",
                        temp_published / "forecasts" / f"{publication_id}.json")
        repeated = evaluate_publication(
            inbox, publication_id, evaluation_at, published_dir=temp_published)
    if (report.publication_sha256 != repeated.publication_sha256
            or report.sales_deliveries != repeated.sales_deliveries
            or report.rows != repeated.rows or report.metrics != repeated.metrics):
        raise ValueError("El monitoreo no se reproduce al corte de evaluación")
    return {"publication_id": publication_id, "evaluation_at": evaluation_at.isoformat(),
            "forecast_rows": len(publication.forecasts), "order_count": len(orders),
            "evaluated_pairs": report.metrics.evaluated_pairs}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    freezing = sub.add_parser("freeze")
    freezing.add_argument("--source-root", type=Path, default=Path("."))
    freezing.add_argument("--repo-root", type=Path, default=Path("."))
    freezing.add_argument("--archive-root", type=Path,
                          default=Path("data/reproductions/operational"))
    freezing.add_argument("--publication-id", required=True)
    freezing.add_argument("--evaluation-at", required=True)
    checking = sub.add_parser("verify")
    checking.add_argument("--archive", type=Path, required=True)
    checking.add_argument("--repo-root", type=Path, default=Path("."))
    args = parser.parse_args()
    if args.action == "freeze":
        path = freeze(args.source_root, args.archive_root,
                      args.publication_id, args.evaluation_at, repo_root=args.repo_root)
        print(path)
    else:
        print(verify(args.archive, args.repo_root))


if __name__ == "__main__":
    main()
