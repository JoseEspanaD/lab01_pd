# Motor de Reposición de Inventario — Laboratorio 1

Proyecto de Product Development con datos públicos de Corporación Favorita. El objetivo es construir un flujo reproducible que, a partir de ventas y otras fuentes históricas, permita generar pronósticos y recomendaciones de reposición. La ingesta actual simula entregas y reconstruye cortes históricos para las cinco fuentes.

## Estructura mínima

```text
src/inventory/       Paquete Python: ingesta, preparación, pronósticos y evaluación
tests/               Pruebas pytest con datos sintéticos
data/raw/            Cinco CSV originales, asociados a archivos .dvc
data/inbox/          Entregas simuladas e inmutables; entrada de solo lectura para la ingesta
data/processed/      Salidas derivadas previstas
config/              Costos y umbral de reemplazo versionados
DECISIONES.md        Contratos y decisiones de modelado (DEC-001 a DEC-005)
```

Los CSV y las entregas generadas están excluidos de Git. Los archivos `data/raw/*.csv.dvc` sí se versionan.

## Requisitos e instalación

Se requieren Python **3.12 o superior**, Git y DVC. `pytest` está declarado en `requirements-dev.txt`. Desde la raíz del repositorio:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -e .
python -m pip install -r requirements-dev.txt
python -m pytest
```

Si se utiliza una versión posterior a 3.12, sustituye `python3.12` por el ejecutable correspondiente. La instalación editable permite `import inventory` sin configurar `PYTHONPATH`. DVC se instala por separado; no es una dependencia del paquete. Las pruebas usan datos sintéticos y no necesitan los CSV de Kaggle.

## Datos originales

Descarga los archivos de la competencia [Store Sales - Time Series Forecasting de Kaggle](https://www.kaggle.com/competitions/store-sales-time-series-forecasting/data). Kaggle puede requerir iniciar sesión y aceptar las reglas de la competencia. Extrae estos cinco archivos originales, sin renombrarlos ni editarlos, directamente en `data/raw/`:

```text
data/raw/train.csv
data/raw/transactions.csv
data/raw/oil.csv
data/raw/holidays_events.csv
data/raw/stores.csv
```

Los otros archivos de la descarga, como `test.csv` y `sample_submission.csv`, no forman parte de los cinco datasets versionados aquí. Cada archivo `.csv.dvc` registra la ruta y la huella del CSV esperado. Si se obtienen versiones o contenidos distintos, no coincidirán con esos metadatos.

La configuración versionada `.dvc/config` no define un remoto. Un remoto local configurado en otra máquina no se recupera desde un clon limpio; por eso **no se puede asumir que `dvc pull` funcione**. Con un remoto accesible y los objetos publicados, DVC podría recuperar los datos; mientras no exista, se necesitan los CSV originales de Kaggle.

## Entregas e ingesta actuales

Para consultar la CLI disponible y simular **una sola fecha de ventas** desde la raíz:

```bash
python -m inventory.train_delivery --help
python -m inventory.train_delivery 2017-08-15
```

La segunda orden lee `data/raw/train.csv` y publica, si hay registros para esa fecha, una entrega en `data/inbox/`. Es la única CLI de entregas disponible; las otras fuentes se manejan mediante funciones Python.

| Fuente | Simulador | Ingesta | Disponibilidad simulada |
| --- | --- | --- | --- |
| `train` | `inventory.train_delivery.simulate_train_delivery` | `inventory.sales_ingestion.ingest_sales` | D+1 a las 06:00 UTC |
| `transactions` | `inventory.transactions.simulate_transactions_delivery` | `inventory.transactions.ingest_transactions` | D+1 a las 06:00 UTC |
| `oil` | `inventory.oil.simulate_oil_delivery` | `inventory.oil.ingest_oil` | D+1 a las 06:00 UTC |
| `holidays_events` | `inventory.holidays_events.simulate_holidays_delivery` | `inventory.holidays_events.ingest_holidays` | D+1 a las 06:00 UTC |
| `stores` | `inventory.stores.simulate_stores_delivery` | `inventory.stores.ingest_stores` | T0 = 2013-01-01 00:00 UTC |

Los simuladores de fuentes fechadas reciben una fecha de observación y generan solo su lote; `stores` publica el catálogo inicial completo. No sobrescriben entregas existentes y repetir una operación con los mismos datos devuelve la misma ruta. `select_deliveries(inbox_dir, cutoff_at)` elige entregas por disponibilidad y revisión; cada función de ingesta lee solo las seleccionadas de su fuente, valida sus registros y devuelve resultados ordenados sin escribir en `data/inbox/` ni en `data/processed/`. `cutoff_at` debe ser un `datetime` UTC. Los precios vacíos de `oil` se conservan como faltantes y los eventos de una fecha mantienen su alcance geográfico. Las correcciones de `transactions`, `oil`, `holidays_events` y `stores` se publican como revisiones completas con `available_at` explícito; el simulador de `train` solo publica la revisión inicial. El contrato de rutas y cortes está en [DEC-001](DECISIONES.md).

Las pruebas pytest usan CSV y bandejas temporales. La prueba `tests/test_all_sources_integration.py` verifica las cinco fuentes en un mismo corte, la disponibilidad, una corrección y la estabilidad de un corte anterior; `tests/test_sales_flow_integration.py` cubre además una corrección externa de ventas.

## Preparación de entrenamiento

`inventory.data_preparation.prepare_training_data(inbox_dir, start_date, end_date, cutoff_at)` devuelve una lista ordenada de `TrainingExample` en memoria. `start_date` y `end_date` son fechas objetivo inclusivas; `cutoff_at` es un `datetime` UTC que limita las etiquetas de ventas disponibles para entrenar. No escribe archivos. Por ejemplo:

```python
from datetime import date, datetime, timezone
from pathlib import Path
from inventory.data_preparation import prepare_training_data

rows = prepare_training_data(
    Path("data/inbox"), date(2017, 8, 10), date(2017, 8, 15),
    datetime(2017, 8, 16, 6, tzinfo=timezone.utc),
)
```

Cada fila corresponde a una fecha objetivo, tienda y familia. Para pronosticar ventas de `D`, sus `features` se reconstruyen únicamente con entregas disponibles en `D−1` a las 06:00 UTC. La preparación recorre una vez los metadatos de las entregas y avanza por sus revisiones disponibles a medida que cambian los cortes de las filas; cada lote seleccionado se lee una sola vez durante ese recorrido. La etiqueta `label_sales` está separada de `features` y debe estar disponible en el corte de preparación. Solo se crean filas para combinaciones tienda–familia ya observadas en ventas antes de su corte; las series nuevas quedan fuera. Se incluyen día de semana y mes de `D`, atributos de tienda, última venta y promoción conocidas, media de ventas observadas en los siete días históricos posibles (`D−8` a `D−2`), últimas transacciones y precio del petróleo conocidos, edades de esos datos y número de eventos conocidos en esa ventana que aplican por ámbito nacional, provincia o ciudad. El último precio del petróleo puede ser `None` si la última cotización entregada estaba vacía. Las demás observaciones faltantes también se representan con `None`; el conteo de eventos conocidos es cero cuando no hay ninguno. No se imputan valores ni se usan eventos de `D` anticipadamente.

La disponibilidad D+1 y T0 son supuestos, no fechas reales de publicación. El catálogo de `stores` se trata como estable desde T0 y los feriados futuros no se adelantan; consulta las limitaciones de [DEC-001 y DEC-002](DECISIONES.md) antes de usar estos datos en predicciones.

## Pronósticos semanales y evaluación

`prepare_forecast_examples(inbox_dir, first_start_date, last_start_date, cutoff_at, require_labels=False)` genera siete filas diarias por cada inicio semanal, separados entre sí por siete días. Para una semana que empieza en `D`, todos los días `D` a `D+6` comparten el corte de `D−1` a las 06:00 UTC. La inferencia utiliza pares tienda–familia conocidos antes del corte sin exigir ventas futuras. Con `require_labels=True`, solo devuelve semanas cuya tienda–familia tiene las siete ventas observadas y disponibles al `cutoff_at` de preparación; no interpreta ausencias como cero. Las etiquetas permanecen fuera de `Features`.

El baseline utiliza la media histórica de ventas observadas y el modelo ML utiliza regresión ridge regularizada sobre el logaritmo de las ventas, con banderas explícitas de faltantes. Ridge aplica `log1p` a ventas históricas, promociones y transacciones no negativas antes de estandarizar; calcula medias y escalas **solo con entrenamiento**. La penalización es proporcional a la cantidad de ejemplos. Entre las penalizaciones `0.01`, `0.1` y `1.0` se elige la de menor costo de negocio en el bloque de **selección**, con MAE como desempate; la prueba no interviene en esa elección. `result.ridge_penalty` muestra el valor elegido. Ambos modelos devuelven cuantiles 10, 50 y 90 calibrados con residuos anteriores al bloque de selección. `business_target` es un objetivo de **demanda**, calculado con el cuantil asociado a los costos; no es un pedido.

La evaluación separa entrenamiento, **calibración**, **selección** y prueba en semanas ordenadas. Hay una semana de embargo antes de cada bloque siguiente: así, las siete ventas de la última semana utilizada ya están disponibles al corte del siguiente. `validation_start` inicia la calibración y `selection_start` inicia la selección. `result.calibration_rows` y `result.validation_rows` cuentan por separado sus ejemplos. El costo de selección y el de prueba se calculan sobre el mismo `business_target` calibrado; MAE se calcula sobre el cuantil 0.50. Por ejemplo, con entregas suficientes:

```python
from datetime import date, datetime, timezone
from pathlib import Path
from inventory.forecasting import evaluate_models, forecast_week

inbox = Path("data/inbox")
result = evaluate_models(
    inbox,
    train_start=date(2017, 1, 15),
    validation_start=date(2017, 5, 7),
    selection_start=date(2017, 6, 4),
    test_start=date(2017, 7, 2),
    last_test_start=date(2017, 7, 30),
    report_cutoff_at=datetime(2017, 8, 7, 6, tzinfo=timezone.utc),
    costs_path=Path("config/forecast_costs.json"),
)
print(result.validation_baseline, result.validation_ml)
print(result.test_baseline, result.test_ml, result.selected_model)

start = date(2017, 8, 15)
cutoff = datetime(2017, 8, 14, 6, tzinfo=timezone.utc)
model, calibration = (
    (result.ml, result.ml_calibration) if result.selected_model == "ml"
    else (result.baseline, result.baseline_calibration)
)
forecasts = forecast_week(inbox, start, cutoff, model, calibration, result.costs)
```

`config/forecast_costs.json` fija costos unitarios de subestimación y sobreestimación y exige una mejora relativa mínima del 5 %. El baseline es el vigente: Ridge solo lo reemplaza si en selección reduce el costo medio más que ese umbral y no aumenta MAE. La prueba se reserva para informar MAE, RMSE, costo medio, pérdida pinball y cobertura del intervalo 10–90 %. Los modelos, resultados y pronósticos se mantienen en memoria; no se escriben datasets ni artefactos. Los cuantiles son globales y pueden ser poco fiables con escasa calibración. Esta métrica es un costo diario de error de demanda, todavía no el costo de un pedido semanal con inventario. Falta integrar inventario, pedidos en tránsito y plazos de entrega para generar pedidos finales. Tampoco hay un flujo de Airflow operativo. El contrato completo se documenta en [DEC-003, DEC-004 y DEC-005](DECISIONES.md).
