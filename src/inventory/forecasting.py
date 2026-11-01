"""Seven daily forecasts from one weekly historical cutoff, with temporal evaluation."""

import json
import hashlib
import math
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

from .data_preparation import ForecastExample, _prediction_cutoff, prepare_forecast_examples


RIDGE_PENALTIES = (0.01, 0.1, 1.0)


@dataclass(frozen=True)
class CostPolicy:
    underforecast_per_unit: float
    overforecast_per_unit: float
    min_relative_improvement: float

    @property
    def decision_quantile(self) -> float:
        return self.underforecast_per_unit / (
            self.underforecast_per_unit + self.overforecast_per_unit
        )


def load_costs(path: Path) -> CostPolicy:
    """Read versioned asymmetric demand-planning costs."""
    values = json.loads(Path(path).read_text(encoding="utf-8"))
    keys = ("underforecast_per_unit", "overforecast_per_unit", "min_relative_improvement")
    if not isinstance(values, dict) or set(values) != set(keys):
        raise ValueError("La configuración de costos tiene claves inválidas")
    if any(type(values[key]) not in (int, float) or not math.isfinite(values[key]) for key in keys):
        raise ValueError("Los costos deben ser números finitos")
    policy = CostPolicy(*(float(values[key]) for key in keys))
    if (policy.underforecast_per_unit < 0 or policy.overforecast_per_unit < 0
            or policy.underforecast_per_unit + policy.overforecast_per_unit == 0
            or not 0 <= policy.min_relative_improvement < 1):
        raise ValueError("Los costos o el umbral de mejora son inválidos")
    return policy


class MeanBaseline:
    """Forecast the observed seven-day historical mean for every future day."""

    def predict_point(self, row: ForecastExample) -> float:
        value = row.features.mean_sales_7d
        if value is None:
            value = row.features.last_sales
        return max(0.0, float(value)) if value is not None else 0.0


def _raw_features(row: ForecastExample) -> list[float]:
    f = row.features
    optional = (
        f.last_sales, f.mean_sales_7d, f.last_onpromotion, f.sales_age_days,
        f.last_transactions, f.transactions_age_days, f.last_oil_price,
        f.oil_age_days, f.cluster,
    )
    # Compress skewed nonnegative volumes before scaling a log-sales target.
    numbers = [
        math.log1p(float(value)) if index in (0, 1, 2, 4) and value is not None
        else float(value) if value is not None else 0.0
        for index, value in enumerate(optional)
    ]
    missing = [float(value is None) for value in optional]
    horizon = (row.target_date - row.forecast_start_date).days
    return (
        numbers + missing + [float(f.known_events_7d)]
        + [float(f.day_of_week == day) for day in range(7)]
        + [float(f.month == month) for month in range(1, 13)]
        + [float(horizon == day) for day in range(7)]
    )


def _solve(matrix: list[list[float]], vector: list[float]) -> list[float]:
    """Solve a small positive-definite ridge system by pivoted elimination."""
    size = len(vector)
    augmented = [row[:] + [value] for row, value in zip(matrix, vector)]
    for col in range(size):
        pivot = max(range(col, size), key=lambda row: abs(augmented[row][col]))
        augmented[col], augmented[pivot] = augmented[pivot], augmented[col]
        scale = augmented[col][col]
        if abs(scale) < 1e-12:
            raise ValueError("El sistema de regresión no tiene solución estable")
        for row in range(col + 1, size):
            factor = augmented[row][col] / scale
            for item in range(col + 1, size + 1):
                augmented[row][item] -= factor * augmented[col][item]
            augmented[row][col] = 0.0
    result = [0.0] * size
    for row in range(size - 1, -1, -1):
        result[row] = (
            augmented[row][size]
            - sum(augmented[row][col] * result[col] for col in range(row + 1, size))
        ) / augmented[row][row]
    return result


class RidgeForecaster:
    """Regularized linear regression on log sales, fitted without external packages."""

    def __init__(self, penalty: float = 0.1) -> None:
        if not math.isfinite(penalty) or penalty <= 0:
            raise ValueError("La penalización debe ser positiva y finita")
        self.penalty = penalty
        self.means: list[float] | None = None
        self.scales: list[float] | None = None
        self.coefficients: list[float] | None = None

    def fit(self, rows: list[ForecastExample]) -> "RidgeForecaster":
        if not rows or any(row.label_sales is None for row in rows):
            raise ValueError("Se requieren ejemplos de entrenamiento etiquetados")
        raw = [_raw_features(row) for row in rows]
        count = len(raw)
        width = len(raw[0])
        self.means = [sum(values[col] for values in raw) / count for col in range(width)]
        self.scales = [
            max(1.0, math.sqrt(sum((values[col] - self.means[col]) ** 2
                                   for values in raw) / count))
            for col in range(width)
        ]
        transformed = [self._vector(values) for values in raw]
        dimension = width + 1
        gram = [[0.0] * dimension for _ in range(dimension)]
        rhs = [0.0] * dimension
        for row, example in zip(transformed, rows):
            target = math.log1p(float(example.label_sales))
            for i in range(dimension):
                rhs[i] += row[i] * target
                for j in range(i + 1):
                    gram[i][j] += row[i] * row[j]
        for i in range(dimension):
            for j in range(i):
                gram[j][i] = gram[i][j]
            if i:
                gram[i][i] += self.penalty * count
        self.coefficients = _solve(gram, rhs)
        return self

    def _vector(self, values: list[float]) -> list[float]:
        assert self.means is not None and self.scales is not None
        return [1.0] + [
            (value - mean) / scale
            for value, mean, scale in zip(values, self.means, self.scales)
        ]

    def predict_point(self, row: ForecastExample) -> float:
        if self.coefficients is None:
            raise ValueError("El modelo ML aún no está entrenado")
        estimate = sum(
            weight * value
            for weight, value in zip(self.coefficients, self._vector(_raw_features(row)))
        )
        return max(0.0, math.expm1(min(estimate, 20.0)))


def _quantile(ordered: list[float], probability: float) -> float:
    position = probability * (len(ordered) - 1)
    lower = math.floor(position)
    upper = math.ceil(position)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


@dataclass(frozen=True)
class ResidualCalibration:
    """Empirical additive residuals learned only from completed validation weeks."""

    residuals: tuple[float, ...]

    @classmethod
    def fit(cls, model: MeanBaseline | RidgeForecaster,
            rows: list[ForecastExample]) -> "ResidualCalibration":
        if not rows or any(row.label_sales is None for row in rows):
            raise ValueError("Se requieren ejemplos de validación etiquetados")
        return cls(tuple(sorted(float(row.label_sales) - model.predict_point(row) for row in rows)))

    def adjusted(self, point: float, probability: float) -> float:
        if not self.residuals or not 0 <= probability <= 1:
            raise ValueError("Calibración o cuantil inválido")
        return max(0.0, point + _quantile(self.residuals, probability))


@dataclass(frozen=True)
class Forecast:
    forecast_start_date: date
    target_date: date
    store_nbr: int
    family: str
    prediction_cutoff_at: datetime
    q10: float
    q50: float
    q90: float
    business_target: float


def predict_rows(model: MeanBaseline | RidgeForecaster, calibration: ResidualCalibration,
                 rows: list[ForecastExample], costs: CostPolicy) -> list[Forecast]:
    """Produce nonnegative daily quantiles and a cost-optimal demand target."""
    forecasts = []
    for row in rows:
        point = model.predict_point(row)
        forecasts.append(Forecast(
            row.forecast_start_date, row.target_date, row.store_nbr, row.family,
            row.prediction_cutoff_at,
            calibration.adjusted(point, 0.1),
            calibration.adjusted(point, 0.5),
            calibration.adjusted(point, 0.9),
            calibration.adjusted(point, costs.decision_quantile),
        ))
    return forecasts


def forecast_week(inbox_dir: Path, start_date: date, cutoff_at: datetime,
                  model: MeanBaseline | RidgeForecaster,
                  calibration: ResidualCalibration, costs: CostPolicy) -> list[Forecast]:
    """Forecast the next seven days at exactly 06:00 UTC before start_date."""
    if cutoff_at != _prediction_cutoff(start_date):
        raise ValueError("cutoff_at debe ser el corte semanal de predicción")
    rows = prepare_forecast_examples(inbox_dir, start_date, start_date, cutoff_at)
    return predict_rows(model, calibration, rows, costs)


@dataclass(frozen=True)
class Metrics:
    mae: float
    rmse: float
    business_cost: float
    pinball_loss: float | None = None
    coverage_80: float | None = None


def _cost(actual: float, target: float, costs: CostPolicy) -> float:
    return (costs.underforecast_per_unit * max(actual - target, 0.0)
            + costs.overforecast_per_unit * max(target - actual, 0.0))


def score_points(model: MeanBaseline | RidgeForecaster, rows: list[ForecastExample],
                 costs: CostPolicy) -> Metrics:
    """Selection metrics on raw validation point predictions."""
    if not rows or any(row.label_sales is None for row in rows):
        raise ValueError("Se requieren etiquetas para evaluar")
    points = [model.predict_point(row) for row in rows]
    errors = [float(row.label_sales) - point for row, point in zip(rows, points)]
    size = len(rows)
    return Metrics(
        sum(abs(error) for error in errors) / size,
        math.sqrt(sum(error * error for error in errors) / size),
        sum(_cost(float(row.label_sales), point, costs)
            for row, point in zip(rows, points)) / size,
    )


def score_forecasts(forecasts: list[Forecast], rows: list[ForecastExample],
                    costs: CostPolicy) -> Metrics:
    """Held-out metrics, including interval coverage and mean pinball loss."""
    if not rows or len(forecasts) != len(rows) or any(row.label_sales is None for row in rows):
        raise ValueError("Se requieren pronósticos y etiquetas alineados")
    errors = []
    costs_observed = []
    pinballs = []
    covered = 0
    for forecast, row in zip(forecasts, rows):
        if ((forecast.forecast_start_date, forecast.target_date, forecast.store_nbr,
             forecast.family, forecast.prediction_cutoff_at)
                != (row.forecast_start_date, row.target_date, row.store_nbr,
                    row.family, row.prediction_cutoff_at)):
            raise ValueError("Pronósticos y etiquetas no coinciden")
        actual = float(row.label_sales)
        error = actual - forecast.q50
        errors.append(error)
        costs_observed.append(_cost(actual, forecast.business_target, costs))
        covered += forecast.q10 <= actual <= forecast.q90
        for probability, value in ((0.1, forecast.q10), (0.5, forecast.q50), (0.9, forecast.q90)):
            difference = actual - value
            pinballs.append(max(probability * difference, (probability - 1) * difference))
    size = len(rows)
    return Metrics(
        sum(abs(error) for error in errors) / size,
        math.sqrt(sum(error * error for error in errors) / size),
        sum(costs_observed) / size,
        sum(pinballs) / len(pinballs),
        covered / size,
    )


def should_replace(incumbent: Metrics, challenger: Metrics, costs: CostPolicy) -> bool:
    """Promote ML only on validation if cost improves enough and MAE does not worsen."""
    return (challenger.business_cost
            < incumbent.business_cost * (1 - costs.min_relative_improvement)
            and challenger.mae <= incumbent.mae)


@dataclass(frozen=True)
class SelectedForecast:
    """Selected model and daily calibration with cutoffs derived during evaluation."""

    name: str
    model: MeanBaseline | RidgeForecaster
    daily_calibration: ResidualCalibration
    model_available_at: datetime
    daily_calibration_available_at: datetime
    model_state_sha256: str


def model_state_payload(model: MeanBaseline | RidgeForecaster) -> dict:
    """Return the complete safe JSON state used for the model fingerprint."""
    if type(model) is MeanBaseline:
        state = {"type": "mean_baseline"}
    elif type(model) is RidgeForecaster and all(
        value is not None for value in (model.means, model.scales, model.coefficients)
    ):
        state = {
            "type": "ridge", "penalty": model.penalty,
            "means": model.means, "scales": model.scales,
            "coefficients": model.coefficients,
        }
    else:
        raise ValueError("El modelo seleccionado no tiene un estado acreditable")
    return state


def model_state_sha256(model: MeanBaseline | RidgeForecaster) -> str:
    """Fingerprint the selected model state so later mutation cannot reuse its cutoff."""
    state = model_state_payload(model)
    encoded = json.dumps(state, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class EvaluationResult:
    baseline: MeanBaseline
    ml: RidgeForecaster
    baseline_calibration: ResidualCalibration
    ml_calibration: ResidualCalibration
    costs: CostPolicy
    validation_baseline: Metrics
    validation_ml: Metrics
    test_baseline: Metrics
    test_ml: Metrics
    selected_model: str
    selected_forecast: SelectedForecast
    selection_start: date
    test_start: date
    ridge_penalty: float
    train_rows: int
    calibration_rows: int
    validation_rows: int
    test_rows: int
    partitions: "EvaluationPartitions | None" = None


@dataclass(frozen=True)
class EvaluationPartitions:
    """The exact supervised examples consumed by the temporal evaluation."""

    train: tuple[ForecastExample, ...]
    calibration: tuple[ForecastExample, ...]
    selection: tuple[ForecastExample, ...]
    test: tuple[ForecastExample, ...]


def evaluate_models(inbox_dir: Path, train_start: date, validation_start: date,
                    selection_start: date, test_start: date, last_test_start: date,
                    report_cutoff_at: datetime, costs_path: Path) -> EvaluationResult:
    """Fit, calibrate, select and test on disjoint chronological weekly blocks.

    Each next block begins after a one-week embargo, so the preceding block's
    complete labels are available before its first forecast. Model selection
    uses calibrated business targets and median forecasts, just like testing.
    """
    costs = load_costs(costs_path)
    boundaries = (validation_start, selection_start, test_start, last_test_start)
    if (any(not isinstance(day, date) or isinstance(day, datetime) for day in
            (train_start, *boundaries))
            or not train_start < validation_start < selection_start < test_start <= last_test_start
            or any((day - train_start).days % 7 for day in boundaries)
            or (validation_start - train_start).days < 14
            or (selection_start - validation_start).days < 14
            or (test_start - selection_start).days < 14):
        raise ValueError("Se requieren semanas ordenadas y una semana de embargo entre particiones")
    inbox_dir = Path(inbox_dir)
    train = prepare_forecast_examples(
        inbox_dir, train_start, validation_start - timedelta(days=14),
        _prediction_cutoff(validation_start), require_labels=True,
    )
    calibration = prepare_forecast_examples(
        inbox_dir, validation_start, selection_start - timedelta(days=14),
        _prediction_cutoff(selection_start), require_labels=True,
    )
    selection = prepare_forecast_examples(
        inbox_dir, selection_start, test_start - timedelta(days=14),
        _prediction_cutoff(test_start), require_labels=True,
    )
    if not train or not calibration or not selection:
        raise ValueError("Cada partición debe contener semanas completas etiquetadas")
    baseline = MeanBaseline()
    baseline_calibration = ResidualCalibration.fit(baseline, calibration)
    validation_baseline = score_forecasts(
        predict_rows(baseline, baseline_calibration, selection, costs), selection, costs,
    )
    candidates = []
    for penalty in RIDGE_PENALTIES:
        model = RidgeForecaster(penalty).fit(train)
        fitted_calibration = ResidualCalibration.fit(model, calibration)
        metrics = score_forecasts(
            predict_rows(model, fitted_calibration, selection, costs), selection, costs,
        )
        candidates.append((metrics, model, fitted_calibration))
    validation_ml, ml, ml_calibration = min(
        candidates, key=lambda item: (item[0].business_cost, item[0].mae, item[1].penalty)
    )
    # Do not inspect test labels until the Ridge penalty and replacement decision are fixed.
    selected_model = "ml" if should_replace(validation_baseline, validation_ml, costs) else "baseline"
    selected_forecast = SelectedForecast(
        selected_model,
        ml if selected_model == "ml" else baseline,
        ml_calibration if selected_model == "ml" else baseline_calibration,
        _prediction_cutoff(test_start),
        _prediction_cutoff(selection_start),
        model_state_sha256(ml if selected_model == "ml" else baseline),
    )
    test = prepare_forecast_examples(
        inbox_dir, test_start, last_test_start, report_cutoff_at, require_labels=True,
    )
    if not test:
        raise ValueError("Cada partición debe contener semanas completas etiquetadas")
    test_baseline = score_forecasts(
        predict_rows(baseline, baseline_calibration, test, costs), test, costs,
    )
    test_ml = score_forecasts(
        predict_rows(ml, ml_calibration, test, costs), test, costs,
    )
    return EvaluationResult(
        baseline, ml, baseline_calibration, ml_calibration, costs,
        validation_baseline, validation_ml, test_baseline, test_ml,
        selected_model, selected_forecast, selection_start, test_start, ml.penalty,
        len(train), len(calibration), len(selection), len(test),
        EvaluationPartitions(tuple(train), tuple(calibration), tuple(selection), tuple(test)),
    )
