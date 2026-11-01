"""A squash merge must retain a verified Git anchor for old DVC manifests."""

import json
from pathlib import Path
from subprocess import CompletedProcess

import pytest

import scripts.verify_integrated_lineage as lineage_module
from inventory.historical_reproduction import MANIFEST, RUN_ID, _code_files, _digest
from inventory.published_forecasts import _write_immutable


SOURCE = "a" * 40
INTEGRATED = "b" * 40


@pytest.fixture
def scenario(tmp_path: Path, monkeypatch) -> Path:
    code = tmp_path / "src/inventory/demo.py"
    code.parent.mkdir(parents=True)
    code.write_text("model code\n", encoding="utf-8")
    (tmp_path / "pyproject.toml").write_text("[project]\n", encoding="utf-8")
    costs = tmp_path / "config/forecast_costs.json"
    costs.parent.mkdir(parents=True)
    costs.write_text("{}\n", encoding="utf-8")
    manifest_path = tmp_path / MANIFEST
    _write_immutable(manifest_path, {
        "source_git_commit": SOURCE,
        "code_files_sha256": _code_files(tmp_path),
    })
    lineage_path = tmp_path / lineage_module.LINEAGE
    lineage_path.parent.mkdir(parents=True)
    lineage_path.write_text(json.dumps({
        "schema_version": 1, "run_id": RUN_ID,
        "manifest_sha256": _digest(manifest_path),
        "source_git_commit": SOURCE,
        "integrated_git_commit": INTEGRATED,
    }), encoding="utf-8")
    monkeypatch.setattr(lineage_module, "_commit_is_ancestor", lambda root, sha: sha == INTEGRATED)
    monkeypatch.setattr(lineage_module, "_commit_exists", lambda root, sha: False)
    monkeypatch.setattr(lineage_module, "_check_committed_code", lambda root, sha, hashes: None)
    monkeypatch.setattr(lineage_module, "verify", lambda root, require_git_revision: {
        "forecast_rows": 7,
    })
    return tmp_path


def test_integrated_anchor_preserves_original_manifest(scenario: Path) -> None:
    before = (scenario / MANIFEST).read_bytes()
    result = lineage_module.verify_integrated_lineage(scenario)
    assert result == {"forecast_rows": 7, "integrated_git_commit": INTEGRATED}
    assert (scenario / MANIFEST).read_bytes() == before


def test_rejects_wrong_manifest_hash_and_unknown_git_anchor(scenario: Path) -> None:
    path = scenario / lineage_module.LINEAGE
    reference = json.loads(path.read_text(encoding="utf-8"))
    path.write_text(json.dumps({**reference, "manifest_sha256": "0" * 64}), encoding="utf-8")
    with pytest.raises(ValueError, match="no coincide"):
        lineage_module.verify_integrated_lineage(scenario)
    path.write_text(json.dumps({**reference, "integrated_git_commit": "c" * 40}),
                    encoding="utf-8")
    with pytest.raises(ValueError, match="antecesor"):
        lineage_module.verify_integrated_lineage(scenario)


def test_rejects_changed_code_or_mismatched_committed_blob(scenario: Path,
                                                           monkeypatch) -> None:
    code = scenario / "src/inventory/demo.py"
    code.write_text("changed\n", encoding="utf-8")
    with pytest.raises(ValueError, match="código actual"):
        lineage_module.verify_integrated_lineage(scenario)
    code.write_text("model code\n", encoding="utf-8")
    def reject_blob(root, sha, hashes):
        raise ValueError("El commit no contiene el código esperado")
    monkeypatch.setattr(lineage_module, "_check_committed_code", reject_blob)
    with pytest.raises(ValueError, match="commit no contiene"):
        lineage_module.verify_integrated_lineage(scenario)


def test_original_commit_is_checked_when_available(scenario: Path, monkeypatch) -> None:
    checked = []
    monkeypatch.setattr(lineage_module, "_commit_exists", lambda root, sha: True)
    monkeypatch.setattr(lineage_module, "_check_committed_code",
                        lambda root, sha, hashes: checked.append(sha))
    lineage_module.verify_integrated_lineage(scenario)
    assert checked == [INTEGRATED, SOURCE]


def test_git_blob_check_requires_exact_bytes(tmp_path: Path, monkeypatch) -> None:
    path = "src/inventory/demo.py"
    digest = lineage_module._sha256(b"expected")
    monkeypatch.setattr(lineage_module, "_git", lambda root, *args:
                        CompletedProcess(args, 0, stdout=b"changed"))
    with pytest.raises(ValueError, match="no contiene el código esperado"):
        lineage_module._check_committed_code(tmp_path, INTEGRATED, {path: digest})
