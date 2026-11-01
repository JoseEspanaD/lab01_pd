"""Publish one simulated observation date from the original CSVs."""

from __future__ import annotations

import argparse
import json
import os
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

from inventory.holidays_events import simulate_holidays_delivery
from inventory.oil import simulate_oil_delivery
from inventory.operations.ingestion import parse_cutoff_at
from inventory.stores import T0, simulate_stores_delivery
from inventory.train_delivery import simulate_train_delivery
from inventory.transactions import simulate_transactions_delivery


DATED_SOURCES = {
    "train": simulate_train_delivery,
    "transactions": simulate_transactions_delivery,
    "oil": simulate_oil_delivery,
    "holidays_events": simulate_holidays_delivery,
}
SOURCES = (*DATED_SOURCES, "stores")


def _date(value: str) -> date:
    try:
        observed = date.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("observation_date debe ser AAAA-MM-DD") from exc
    if observed.isoformat() != value:
        raise ValueError("observation_date debe ser AAAA-MM-DD")
    return observed


def produce_for_date(
    observation_date: str,
    cutoff_text: str,
    raw_dir: Path,
    inbox_dir: Path,
    *,
    source: str = "all",
    revision: int = 1,
    available_at_text: str | None = None,
    correction_path: Path | None = None,
) -> dict:
    """Publish only the requested date's batches visible at the explicit UTC cut.

    Initial batches use D+1 06:00 UTC (stores use T0). Corrections are complete
    replacements for one source and date, with an explicit later availability.
    Existing delivery functions reject changed content and duplicate revisions.
    """
    observed = _date(observation_date)
    cutoff = parse_cutoff_at(cutoff_text)
    if source not in (*SOURCES, "all"):
        raise ValueError(f"Fuente desconocida: {source}")
    if isinstance(revision, bool) or not isinstance(revision, int) or not 1 <= revision <= 9999:
        raise ValueError("revision debe estar entre 1 y 9999")
    if revision == 1 and (available_at_text is not None or correction_path is not None):
        raise ValueError("La entrega inicial usa el CSV original y su disponibilidad fija")
    if revision > 1:
        if source in ("all", "train") or available_at_text is None or correction_path is None:
            raise ValueError("La corrección requiere una fuente compatible, available_at y correction_path")
    available = parse_cutoff_at(available_at_text) if available_at_text else None
    if available is not None and available > cutoff:
        raise ValueError("available_at de la corrección es posterior al corte")
    if source != "stores":
        try:
            minimum = datetime.combine(observed + timedelta(days=1), time(6), timezone.utc)
        except OverflowError as exc:
            raise ValueError("No se puede calcular D+1 para observation_date") from exc
        if available is not None and available <= minimum:
            raise ValueError("La corrección debe estar disponible después de D+1 06:00 UTC")
    if available is not None and source == "stores" and available <= T0:
        raise ValueError("La corrección de stores debe ser posterior a T0")

    raw_dir = Path(raw_dir)
    inbox_dir = Path(inbox_dir)
    correction_path = Path(correction_path) if correction_path is not None else None
    if correction_path is not None and not correction_path.is_file():
        raise ValueError(f"No existe el CSV de corrección: {correction_path}")

    published = []
    names = SOURCES if source == "all" else (source,)
    for name in names:
        if name == "stores":
            if cutoff < T0:
                continue
            path = simulate_stores_delivery(
                correction_path or raw_dir / "stores.csv", inbox_dir,
                revision=revision, available_at=available,
            )
        else:
            if revision == 1 and cutoff < minimum:
                continue
            simulator = DATED_SOURCES[name]
            if name == "train":
                path = simulator(observed, raw_dir / "train.csv", inbox_dir)
            else:
                path = simulator(
                    observed, correction_path or raw_dir / f"{name}.csv", inbox_dir,
                    revision=revision, available_at=available,
                )
        if path is not None:
            published.append(path.relative_to(inbox_dir).as_posix())
    return {"cutoff_at": cutoff.isoformat(), "observation_date": observed.isoformat(),
            "deliveries": published}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--observation-date", required=True)
    parser.add_argument("--cutoff-at", required=True)
    parser.add_argument("--raw-dir", type=Path,
                        default=Path(os.environ.get("INVENTORY_RAW_DIR", "data/raw")))
    parser.add_argument("--inbox-dir", type=Path,
                        default=Path(os.environ.get("INVENTORY_INBOX_DIR", "data/inbox")))
    parser.add_argument("--source", choices=(*SOURCES, "all"), default="all")
    parser.add_argument("--revision", type=int, default=1)
    parser.add_argument("--available-at")
    parser.add_argument("--correction-path", type=Path)
    args = parser.parse_args()
    result = produce_for_date(
        args.observation_date, args.cutoff_at, args.raw_dir, args.inbox_dir,
        source=args.source, revision=args.revision,
        available_at_text=args.available_at, correction_path=args.correction_path,
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
