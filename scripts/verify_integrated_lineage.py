"""Verify a historical forecast against code integrated by Squash and merge."""

import argparse
import json
import re
import subprocess
from pathlib import Path

from inventory.historical_reproduction import (
    MANIFEST, ROOT, RUN_ID, _code_files, _digest, _read_payload, _sha256,
    verify,
)


LINEAGE = Path("lineage") / f"{RUN_ID}.json"


def _git(root: Path, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["git", *args], cwd=root, capture_output=True, check=False)


def _commit_exists(root: Path, commit: str) -> bool:
    return _git(root, "cat-file", "-e", f"{commit}^{{commit}}").returncode == 0


def _commit_is_ancestor(root: Path, commit: str) -> bool:
    return _git(root, "merge-base", "--is-ancestor", commit, "HEAD").returncode == 0


def _check_committed_code(root: Path, commit: str, digests: dict[str, str]) -> None:
    for path, expected in digests.items():
        blob = _git(root, "show", f"{commit}:{path}")
        if blob.returncode != 0 or _sha256(blob.stdout) != expected:
            raise ValueError(f"El commit {commit} no contiene el código esperado: {path}")


def verify_integrated_lineage(root: Path = ROOT) -> dict:
    """Keep the old manifest immutable and bind it to an ancestor with exact code."""
    root = Path(root)
    manifest_path = root / MANIFEST
    manifest = _read_payload(manifest_path)
    try:
        lineage = json.loads((root / LINEAGE).read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ValueError("Referencia de linaje integrado inválida") from exc
    if (not isinstance(lineage, dict)
            or set(lineage) != {"schema_version", "run_id", "manifest_sha256",
                                "source_git_commit", "integrated_git_commit"}
            or lineage["schema_version"] != 1 or lineage["run_id"] != RUN_ID
            or lineage["source_git_commit"] != manifest["source_git_commit"]
            or lineage["manifest_sha256"] != _digest(manifest_path)):
        raise ValueError("La referencia integrada no coincide con el manifiesto original")
    commit = lineage["integrated_git_commit"]
    if not isinstance(commit, str) or not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise ValueError("SHA Git integrado inválido")
    digests = manifest["code_files_sha256"]
    if _code_files(root) != digests:
        raise ValueError("El código actual no coincide con el manifiesto original")
    if not _commit_is_ancestor(root, commit):
        raise ValueError("El commit integrado no es antecesor de HEAD")
    _check_committed_code(root, commit, digests)
    source_commit = manifest["source_git_commit"]
    if _commit_exists(root, source_commit):
        _check_committed_code(root, source_commit, digests)
    result = verify(root, require_git_revision=False)
    return {**result, "integrated_git_commit": commit}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    args = parser.parse_args()
    print(json.dumps(verify_integrated_lineage(args.root), sort_keys=True))


if __name__ == "__main__":
    main()
