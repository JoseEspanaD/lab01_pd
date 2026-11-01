"""Run immutable, as-of monitoring for one stored forecast publication."""

from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

from inventory.forecast_monitoring import _report_path, evaluate_publication
from inventory.operations.ingestion import parse_cutoff_at
from inventory.forecast_monitoring import read_report
from inventory.published_forecasts import (
    _canonical, _input_references, _payload, _sha256, read_publication,
)


def monitor_and_record(
    inbox_dir: Path, published_dir: Path, publication_id: str, evaluation_text: str,
) -> dict:
    """Score published predictions only with sales visible at an explicit UTC cut."""
    evaluation_at = parse_cutoff_at(evaluation_text)
    report = evaluate_publication(
        Path(inbox_dir), publication_id, evaluation_at,
        published_dir=Path(published_dir),
    )
    return {
        "publication_id": report.publication_id,
        "publication_kind": report.publication_kind,
        "evaluation_at": report.evaluation_at.isoformat(),
        "report_path": str(_report_path(Path(published_dir), publication_id, evaluation_at)),
        "metrics": asdict(report.metrics),
    }


def monitor_visible_publications(inbox_dir: Path, published_dir: Path,
                                 evaluation_text: str) -> dict:
    """Reevaluate stored publications only when target sales revisions change."""
    evaluation_at = parse_cutoff_at(evaluation_text)
    inbox_dir, published_dir = Path(inbox_dir), Path(published_dir)
    forecasts_dir = published_dir / "forecasts"
    selected = _input_references(inbox_dir, evaluation_at)
    reports = []
    skipped = []
    for path in sorted(forecasts_dir.glob("*.json")):
        publication_id = path.stem
        publication = read_publication(publication_id, published_dir=published_dir)
        earliest = (publication.published_at if publication.kind == "real"
                    else publication.prediction_cutoff_at)
        if evaluation_at < earliest:
            skipped.append(publication_id)
            continue
        dates = {row.target_date.isoformat() for row in publication.forecasts}
        relevant = tuple(ref for ref in selected
                         if ref.path.startswith("source=train/")
                         and any(f"observation_date={day}/" in ref.path for day in dates))
        report_dir = published_dir / "monitoring" / publication_id
        earlier = sorted(path for path in report_dir.glob("*.json")
                         if path.name <= _report_path(published_dir, publication_id,
                                                       evaluation_at).name)
        if earlier:
            previous_at = datetime.strptime(
                earlier[-1].stem, "%Y-%m-%dT%H%M%S.%fZ").replace(tzinfo=timezone.utc)
            previous = read_report(publication_id, previous_at, published_dir=published_dir)
            if previous.publication_sha256 != _sha256(_canonical(_payload(publication))):
                raise ValueError(f"Conflicto: cambió la publicación {publication_id}")
            if previous.sales_deliveries == relevant:
                skipped.append(publication_id)
                continue
        reports.append(monitor_and_record(
            inbox_dir, published_dir, publication_id, evaluation_text))
    return {"evaluation_at": evaluation_at.isoformat(), "reports": reports,
            "skipped_publications": skipped}
