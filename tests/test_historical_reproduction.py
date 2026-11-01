"""Historical replay from immutable inputs and DVC-linked artifacts."""

import csv
import hashlib
import json
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

import inventory.historical_reproduction as replay
from inventory.dated_source import publish_delivery
from inventory.forecasting import MeanBaseline, ResidualCalibration, model_state_sha256
from inventory.holidays_events import HOLIDAYS_COLUMNS
from inventory.oil import OIL_COLUMNS
from inventory.stores import STORES_COLUMNS
from inventory.train_schema import TRAIN_COLUMNS
from inventory.transactions import TRANSACTIONS_COLUMNS


def _csv(path: Path, columns: tuple[str, ...], rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.writer(output)
        writer.writerow(columns)
        writer.writerows(rows)


def _pointer(path: Path, md5: str, size: int) -> None:
    pointer = path.with_name(path.name + ".dvc")
    pointer.parent.mkdir(parents=True, exist_ok=True)
    pointer.write_text(f"outs:\n- md5: {md5}\n  size: {size}\n  path: {path.name}\n",
                       encoding="utf-8")


@pytest.fixture
def scenario(tmp_path: Path) -> Path:
    raw = tmp_path / "data/raw"
    days = [replay.FIRST_OBSERVED + timedelta(days=offset) for offset in range(65)]
    _csv(raw / "train.csv", TRAIN_COLUMNS, [
        [str(i + 1), day.isoformat(), "1", "GROCERY I", str(10 + i % 7), "0"]
        for i, day in enumerate(days)
    ])
    _csv(raw / "transactions.csv", TRANSACTIONS_COLUMNS,
         [[day.isoformat(), "1", "50"] for day in days])
    _csv(raw / "oil.csv", OIL_COLUMNS,
         [[day.isoformat(), "50"] for day in days])
    _csv(raw / "holidays_events.csv", HOLIDAYS_COLUMNS, [])
    _csv(raw / "stores.csv", STORES_COLUMNS, [["1", "Quito", "Pichincha", "D", "13"]])
    for name in replay.RAW_NAMES:
        path = raw / f"{name}.csv"
        _pointer(path, hashlib.md5(path.read_bytes()).hexdigest(), path.stat().st_size)
    config = tmp_path / replay.COSTS
    config.parent.mkdir(parents=True)
    config.write_text(json.dumps({
        "underforecast_per_unit": 5, "overforecast_per_unit": 1,
        "min_relative_improvement": 0.05,
    }), encoding="utf-8")
    source = tmp_path / "src/inventory"
    source.mkdir(parents=True)
    (source / "historical_reproduction.py").write_text("demo code\n", encoding="utf-8")
    (tmp_path / "pyproject.toml").write_text("[project]\n", encoding="utf-8")
    return tmp_path


def _materialize_pointers(root: Path) -> None:
    for artifact in replay.ARTIFACTS:
        path = root / artifact
        if path.is_dir():
            size = sum(item.stat().st_size for item in path.rglob("data.csv"))
            digest = hashlib.md5(str(size).encode()).hexdigest() + ".dir"
        else:
            size = path.stat().st_size
            digest = hashlib.md5(path.read_bytes()).hexdigest()
        _pointer(path, digest, size)


def test_build_is_idempotent_and_replays_exact_predictions(scenario: Path) -> None:
    replay.build_snapshot(scenario)
    replay.build_artifacts(scenario)
    original = (scenario / replay.PUBLICATION).read_bytes()
    replay.build_snapshot(scenario)
    replay.build_artifacts(scenario)
    assert (scenario / replay.PUBLICATION).read_bytes() == original
    _materialize_pointers(scenario)
    replay.write_manifest(scenario)
    result = replay.verify(scenario, require_git_revision=False)
    assert result["forecast_rows"] == 7
    assert result["cutoff_at"] == replay.CUTOFF.isoformat()
    assert replay._read_payload(scenario / replay.MANIFEST)["source_git_commit"] is None
    with pytest.raises(ValueError, match="Revisión Git"):
        replay.verify(scenario)


def test_integrity_rejects_changed_partitions_model_and_code(scenario: Path) -> None:
    replay.build_snapshot(scenario)
    replay.build_artifacts(scenario)
    _materialize_pointers(scenario)
    replay.write_manifest(scenario)
    processed = scenario / replay.PROCESSED
    before = processed.read_bytes()
    processed.write_bytes(before + b" ")
    with pytest.raises(ValueError, match="Artefacto distinto"):
        replay.verify(scenario, require_git_revision=False)
    processed.write_bytes(before)
    model = scenario / replay.MODEL
    before = model.read_bytes()
    model.write_bytes(before + b" ")
    with pytest.raises(ValueError, match="Artefacto distinto"):
        replay.verify(scenario, require_git_revision=False)
    model.write_bytes(before)
    delivery = next((scenario / replay.INBOX).rglob("data.csv"))
    before = delivery.read_bytes()
    delivery.write_bytes(before + b" ")
    with pytest.raises(ValueError, match="Instantánea distinta"):
        replay.verify(scenario, require_git_revision=False)
    delivery.write_bytes(before)
    publication = scenario / replay.PUBLICATION
    before = publication.read_bytes()
    publication.write_bytes(before + b" ")
    with pytest.raises(ValueError, match="Artefacto distinto"):
        replay.verify(scenario, require_git_revision=False)
    publication.write_bytes(before)
    code = scenario / "src/inventory/historical_reproduction.py"
    code.write_text("changed\n", encoding="utf-8")
    with pytest.raises(ValueError, match="Código o configuración"):
        replay.verify(scenario, require_git_revision=False)


def test_pending_manifest_can_be_finalized_once_without_changing_lineage(scenario: Path) -> None:
    replay.build_snapshot(scenario)
    replay.build_artifacts(scenario)
    _materialize_pointers(scenario)
    replay.write_manifest(scenario)
    draft = replay._read_payload(scenario / replay.MANIFEST)
    commit = "a" * 40
    replay.write_manifest(scenario, source_git_commit=commit)
    final = replay._read_payload(scenario / replay.MANIFEST)
    assert final["source_git_commit"] == commit
    assert {key: value for key, value in final.items() if key != "source_git_commit"} == {
        key: value for key, value in draft.items() if key != "source_git_commit"
    }
    before = (scenario / replay.MANIFEST).read_bytes()
    replay.write_manifest(scenario, source_git_commit=commit)
    assert (scenario / replay.MANIFEST).read_bytes() == before
    with pytest.raises(ValueError, match="Conflicto"):
        replay.write_manifest(scenario, source_git_commit="b" * 40)


def test_future_delivery_and_late_correction_are_not_silently_included(scenario: Path) -> None:
    replay.build_snapshot(scenario)
    day = replay.LAST_OBSERVED
    late = datetime(2017, 8, 2, 6, tzinfo=timezone.utc)
    publish_delivery("train", day, late, 2, TRAIN_COLUMNS,
                     [["999", day.isoformat(), "1", "GROCERY I", "999", "0"]],
                     scenario / replay.INBOX)
    with pytest.raises(ValueError, match="no visibles"):
        replay._snapshot_files(scenario)
    with pytest.raises(ValueError, match="no visibles"):
        replay.build_snapshot(scenario)
    with pytest.raises(ValueError, match="no visibles"):
        replay.build_artifacts(scenario)
    assert not (scenario / replay.PROCESSED).exists()


def test_corrections_respect_each_label_cutoff(scenario: Path) -> None:
    replay.build_snapshot(scenario)
    day = date(2017, 6, 5)
    inbox = scenario / replay.INBOX
    publish_delivery("train", day, datetime(2017, 6, 15, 6, tzinfo=timezone.utc),
                     2, TRAIN_COLUMNS,
                     [["111", day.isoformat(), "1", "GROCERY I", "111", "0"]], inbox)
    publish_delivery("train", day, datetime(2017, 6, 25, 6, tzinfo=timezone.utc),
                     3, TRAIN_COLUMNS,
                     [["999", day.isoformat(), "1", "GROCERY I", "999", "0"]], inbox)
    result = replay._evaluation(scenario)
    assert result.partitions is not None
    observed = [row.label_sales for row in result.partitions.train if row.target_date == day]
    assert observed == [111]
    replay.build_artifacts(scenario)
    _materialize_pointers(scenario)
    replay.write_manifest(scenario)
    assert replay.verify(scenario, require_git_revision=False)["forecast_rows"] == 7


def test_future_model_or_calibration_is_rejected_before_persisting(scenario: Path,
                                                                   monkeypatch) -> None:
    replay.build_snapshot(scenario)
    actual = replay._evaluation(scenario)
    for name in ("model_available_at", "daily_calibration_available_at"):
        future = replace(actual.selected_forecast,
                         **{name: replay.CUTOFF + timedelta(seconds=1)})
        monkeypatch.setattr(replay, "_evaluation", lambda root, selected=future:
                            replace(actual, selected_forecast=selected))
        with pytest.raises(ValueError, match="posterior"):
            replay.build_artifacts(scenario)
    assert not (scenario / replay.PROCESSED).exists()


def test_model_loader_rejects_tampered_state_and_calibration() -> None:
    base = {"name": "baseline", "state": {"type": "mean_baseline"},
            "state_sha256": model_state_sha256(MeanBaseline()),
            "daily_residuals": [0.0, 1.0]}
    model, calibration = replay._load_model(base)
    assert isinstance(model, MeanBaseline)
    assert calibration == ResidualCalibration((0.0, 1.0))
    with pytest.raises(ValueError, match="Calibración"):
        replay._load_model({**base, "daily_residuals": [1.0, 0.0]})
    with pytest.raises(ValueError, match="Huella"):
        replay._load_model({**base, "state_sha256": "0" * 64})
