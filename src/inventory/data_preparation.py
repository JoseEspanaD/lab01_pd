"""Build one-day-ahead training rows from point-in-time inbox deliveries."""

import csv
from bisect import bisect_left, insort
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from .dated_source import _minimum_availability
from .delivery_selector import _Delivery, list_deliveries
from .holidays_events import HolidayEventRecord, SOURCE as HOLIDAYS_SOURCE
from .oil import OilRecord, SOURCE as OIL_SOURCE
from .sales_ingestion import ingest_sales
from .stores import CATALOG_DATE, STORES_COLUMNS, T0, StoreRecord
from .stores import _parse_row as parse_store_row
from .train_schema import TRAIN_COLUMNS, SaleRecord, parse_train_row
from .transactions import TransactionRecord, SOURCE as TRANSACTIONS_SOURCE


@dataclass(frozen=True)
class Features:
    """Predictors known at prediction_cutoff_at; missing observations stay None."""

    day_of_week: int
    month: int
    city: str | None
    state: str | None
    store_type: str | None
    cluster: int | None
    last_sales: Decimal | None
    sales_age_days: int | None
    mean_sales_7d: Decimal | None
    last_onpromotion: int | None
    last_transactions: int | None
    transactions_age_days: int | None
    last_oil_price: Decimal | None
    oil_age_days: int | None
    known_events_7d: int


@dataclass(frozen=True)
class TrainingExample:
    target_date: date
    store_nbr: int
    family: str
    prediction_cutoff_at: datetime
    features: Features
    label_sales: Decimal


def _prediction_cutoff(target_date: date) -> datetime:
    try:
        return datetime.combine(target_date - timedelta(days=1), time(6), tzinfo=timezone.utc)
    except OverflowError as exc:
        raise ValueError("No se puede calcular el corte de predicción") from exc


def _applies(event: HolidayEventRecord, store: StoreRecord | None) -> bool:
    if event.locale == "National":
        return True
    if store is None:
        return False
    if event.locale == "Regional":
        return event.locale_name == store.state
    return event.locale_name == store.city


def _read_batch(delivery: _Delivery) -> list:
    """Parse one selected complete batch with the source ingestion parsers."""
    source = delivery.source
    if source == "train":
        columns, parse_row = TRAIN_COLUMNS, parse_train_row
        key = lambda record: (record.date, record.store_nbr, record.family)
    elif source == "stores":
        columns, parse_row = STORES_COLUMNS, parse_store_row
        key = lambda record: record.store_nbr
    else:
        schema = {
            "transactions": TRANSACTIONS_SOURCE,
            "oil": OIL_SOURCE,
            "holidays_events": HOLIDAYS_SOURCE,
        }[source]
        columns, parse_row, key = schema.columns, schema.parse_row, schema.key

    records = []
    seen = set()
    with delivery.path.open(newline="", encoding="utf-8") as batch:
        reader = csv.reader(batch)
        if tuple(next(reader, [])) != columns:
            raise ValueError(f"Columnas inválidas en {delivery.path}")
        for row in reader:
            record = parse_row(row, reader.line_num)
            if source != "stores" and record.date != delivery.observation_date:
                raise ValueError(f"Fila {reader.line_num} de {delivery.path}: date no coincide con la ruta")
            record_key = key(record)
            if record_key in seen:
                raise ValueError(f"Fila {reader.line_num} de {delivery.path}: clave duplicada")
            seen.add(record_key)
            records.append(record)
    return records


def _remove_date(dates: list[date], observed: date) -> None:
    position = bisect_left(dates, observed)
    dates.pop(position)


class _History:
    """Advance selected complete batches once, keeping compact point-in-time indexes."""

    def __init__(self, inbox_dir: Path) -> None:
        versions = list_deliveries(inbox_dir, datetime.max.replace(tzinfo=timezone.utc))
        for delivery in versions:
            if delivery.source == "stores":
                if (delivery.observation_date != CATALOG_DATE or delivery.available_at < T0
                        or (delivery.revision == 1 and delivery.available_at != T0)):
                    raise ValueError(f"Disponibilidad o fecha inválida para stores: {delivery.path}")
            elif delivery.available_at < _minimum_availability(delivery.observation_date):
                raise ValueError(f"Entrega anticipada de {delivery.source}: {delivery.path}")
        self.versions = sorted(versions, key=lambda item: (item.available_at, item.revision))
        self.position = 0
        self.active: dict[tuple[str, date], list] = {}
        self.selected_rank: dict[tuple[str, date], tuple[datetime, int]] = {}
        self.sales: dict[tuple[int, str], dict[date, SaleRecord]] = defaultdict(dict)
        self.sales_dates: dict[tuple[int, str], list[date]] = defaultdict(list)
        self.transactions: dict[int, dict[date, TransactionRecord]] = defaultdict(dict)
        self.transaction_dates: dict[int, list[date]] = defaultdict(list)
        self.oil: dict[date, OilRecord] = {}
        self.oil_dates: list[date] = []
        self.events: dict[date, list[HolidayEventRecord]] = {}
        self.stores: dict[int, StoreRecord] = {}

    def advance(self, cutoff_at: datetime) -> None:
        pending: dict[tuple[str, date], _Delivery] = {}
        while self.position < len(self.versions) and self.versions[self.position].available_at <= cutoff_at:
            delivery = self.versions[self.position]
            key = (delivery.source, delivery.observation_date)
            previous = pending.get(key)
            if previous is None or (delivery.available_at, delivery.revision) > (
                previous.available_at, previous.revision
            ):
                pending[key] = delivery
            self.position += 1
        for key in sorted(pending):
            delivery = pending[key]
            rank = (delivery.available_at, delivery.revision)
            if rank <= self.selected_rank.get(key, (datetime.min.replace(tzinfo=timezone.utc), 0)):
                continue
            new = _read_batch(delivery)
            self._replace(delivery, self.active.get(key, []), new)
            self.active[key] = new
            self.selected_rank[key] = rank

    def _replace(self, delivery: _Delivery, old: list, new: list) -> None:
        observed = delivery.observation_date
        if delivery.source == "train":
            for record in old:
                pair = (record.store_nbr, record.family)
                del self.sales[pair][observed]
                _remove_date(self.sales_dates[pair], observed)
            for record in new:
                pair = (record.store_nbr, record.family)
                self.sales[pair][observed] = record
                insort(self.sales_dates[pair], observed)
        elif delivery.source == "transactions":
            for record in old:
                del self.transactions[record.store_nbr][observed]
                _remove_date(self.transaction_dates[record.store_nbr], observed)
            for record in new:
                self.transactions[record.store_nbr][observed] = record
                insort(self.transaction_dates[record.store_nbr], observed)
        elif delivery.source == "oil":
            if old:
                del self.oil[observed]
                _remove_date(self.oil_dates, observed)
            if new:
                self.oil[observed] = new[0]
                insort(self.oil_dates, observed)
        elif delivery.source == "holidays_events":
            self.events[observed] = new
        else:
            self.stores = {record.store_nbr: record for record in new}


def prepare_training_data(
    inbox_dir: Path,
    start_date: date,
    end_date: date,
    cutoff_at: datetime,
) -> list[TrainingExample]:
    """Return labeled rows for inclusive target dates, using each row's own cutoff.

    The training cutoff limits labels. Predictors use deliveries available at
    06:00 UTC on the day before each target, so later revisions cannot rewrite them.
    """
    if (not isinstance(start_date, date) or isinstance(start_date, datetime)
            or not isinstance(end_date, date) or isinstance(end_date, datetime)):
        raise ValueError("start_date y end_date deben ser fechas sin hora")
    if end_date < start_date:
        raise ValueError("end_date debe ser igual o posterior a start_date")
    if not isinstance(cutoff_at, datetime) or cutoff_at.tzinfo is None or cutoff_at.utcoffset() != timedelta(0):
        raise ValueError("cutoff_at debe ser un datetime UTC")

    inbox_dir = Path(inbox_dir)
    labels_by_date: dict[date, list] = defaultdict(list)
    for sale in ingest_sales(inbox_dir, cutoff_at):
        if start_date <= sale.date <= end_date:
            labels_by_date[sale.date].append(sale)

    history = _History(inbox_dir) if labels_by_date else None
    examples: list[TrainingExample] = []
    for target_date in sorted(labels_by_date):
        prediction_cutoff_at = _prediction_cutoff(target_date)
        if prediction_cutoff_at > cutoff_at:
            continue
        history.advance(prediction_cutoff_at)
        week_start = target_date - timedelta(days=8)

        for label in labels_by_date[target_date]:
            pair = (label.store_nbr, label.family)
            dates = history.sales_dates.get(pair)
            if not dates:
                continue  # No historical evidence that this store-family series existed.
            last_sale = history.sales[pair][dates[-1]]
            week_sales = [
                history.sales[pair][observed].sales
                for observed in dates[bisect_left(dates, week_start):]
            ]
            store = history.stores.get(label.store_nbr)
            transaction_dates = history.transaction_dates.get(label.store_nbr, [])
            last_transaction = (
                history.transactions[label.store_nbr][transaction_dates[-1]] if transaction_dates else None
            )
            last_oil = history.oil[history.oil_dates[-1]] if history.oil_dates else None
            known_events = sum(
                1 for offset in range(2, 9)
                for event in history.events.get(target_date - timedelta(days=offset), [])
                if _applies(event, store)
            )
            features = Features(
                day_of_week=target_date.weekday(),
                month=target_date.month,
                city=store.city if store else None,
                state=store.state if store else None,
                store_type=store.type if store else None,
                cluster=store.cluster if store else None,
                last_sales=last_sale.sales,
                sales_age_days=(target_date - last_sale.date).days,
                mean_sales_7d=sum(week_sales, Decimal(0)) / len(week_sales) if week_sales else None,
                last_onpromotion=last_sale.onpromotion,
                last_transactions=last_transaction.transactions if last_transaction else None,
                transactions_age_days=(target_date - last_transaction.date).days if last_transaction else None,
                last_oil_price=last_oil.dcoilwtico if last_oil else None,
                oil_age_days=(target_date - last_oil.date).days if last_oil else None,
                known_events_7d=known_events,
            )
            examples.append(TrainingExample(
                target_date, label.store_nbr, label.family, prediction_cutoff_at, features, label.sales
            ))
    return examples
