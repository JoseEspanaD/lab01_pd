# Motor de Reposición de Inventario — Laboratorio 1

Proyecto de Product Development con datos públicos de Corporación Favorita. El objetivo es construir un flujo reproducible que, a partir de ventas y otras fuentes históricas, permita generar pronósticos y recomendaciones de reposición. La ingesta actual simula entregas y reconstruye cortes históricos para las cinco fuentes.

## Estructura mínima

```text
src/inventory/       Paquete Python: ingesta, preparación, pronósticos, pedidos y seguimiento
tests/               Pruebas pytest con datos sintéticos
data/raw/            Cinco CSV originales, asociados a archivos .dvc
data/inbox/          Entregas simuladas e inmutables; entrada de solo lectura para la ingesta
data/published/      Publicaciones y reportes inmutables; artefactos excluidos de Git
data/processed/      Salidas derivadas previstas
config/              Costos y umbral de reemplazo versionados
DECISIONES.md        Contratos y decisiones de modelado (DEC-001 a DEC-007)
```

Los CSV y las entregas generadas están excluidos de Git. Los archivos `data/raw/*.csv.dvc` sí se versionan.

## Requisitos e instalación

Se requieren Python **3.12 o superior** y Git. Para instalar el proyecto y DVC desde un clon nuevo:

```bash
git clone https://github.com/JoseEspanaD/lab01_pd.git
cd lab01_pd
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -e .
python -m pip install -r requirements-dev.txt
python -m pip install dvc
python -m pytest
```

Si se utiliza una versión posterior a 3.12, sustituye `python3.12` por el ejecutable correspondiente. La instalación editable permite `import inventory` sin configurar `PYTHONPATH`. DVC se instala por separado; no es una dependencia del paquete. Las pruebas usan datos sintéticos y no necesitan los CSV de Kaggle.

## Datos originales

Git versiona el código, `.dvc/config` y los punteros `data/raw/*.csv.dvc`; DagsHub almacena los objetos de los CSV. El remoto HTTPS `dagshub` ya está configurado como predeterminado. Para recuperar los cinco archivos desde un clon, cada docente debe usar su propia cuenta y token de DagsHub. Configura la autenticación HTTP básica **local** y ejecuta la descarga desde la raíz del repositorio:

```bash
dvc remote modify --local dagshub auth basic
dvc remote modify --local dagshub user TU_USUARIO_DAGSHUB
dvc remote modify --local dagshub ask_password true
dvc pull data/raw/*.csv.dvc
```

Cuando necesite acceder al remoto, DVC solicitará el token de DagsHub interactivamente como contraseña. Nunca lo compartas, lo incluyas en comandos ni lo subas a Git. `--local` escribe estos ajustes en `.dvc/config.local`, que está excluido de Git; no configures ahí una contraseña persistente. La recuperación autenticada de los cinco CSV se verificó con una segunda cuenta sin permisos de colaboración y una caché DVC vacía: `5 files fetched and 5 files added`; todos coincidieron en MD5 y tamaño con sus punteros. Una prueba de `dvc pull` anónimo devolvió HTTP 401, por lo que no se considera una vía de acceso verificada.

También puedes obtener los CSV directamente de la competencia [Store Sales - Time Series Forecasting de Kaggle](https://www.kaggle.com/competitions/store-sales-time-series-forecasting/data), que puede requerir iniciar sesión y aceptar sus reglas. Extrae estos cinco archivos originales, sin renombrarlos ni editarlos, directamente en `data/raw/`:

```text
data/raw/train.csv
data/raw/transactions.csv
data/raw/oil.csv
data/raw/holidays_events.csv
data/raw/stores.csv
```

Los otros archivos de la descarga, como `test.csv` y `sample_submission.csv`, no forman parte de los cinco datasets versionados aquí. Cada archivo `.csv.dvc` registra la ruta y la huella del CSV esperado. Si se obtienen versiones o contenidos distintos, no coincidirán con esos metadatos. Los datos procesados, modelos y pronósticos publicados todavía requieren versionado posterior con DVC.

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

`config/forecast_costs.json` fija costos unitarios de subestimación y sobreestimación y exige una mejora relativa mínima del 5 %. El baseline es el vigente: Ridge solo lo reemplaza si en selección reduce el costo medio más que ese umbral y no aumenta MAE. La prueba se reserva para informar MAE, RMSE, costo medio, pérdida pinball y cobertura del intervalo 10–90 %. `evaluate_models` y `forecast_week` devuelven resultados en memoria; la publicación se realiza mediante el módulo descrito más abajo. Los cuantiles son globales y pueden ser poco fiables con escasa calibración. Esta métrica es un costo diario de error de demanda, todavía no el costo de un pedido semanal con inventario. Falta integrar inventario, pedidos en tránsito y plazos de entrega para generar pedidos finales. Tampoco hay un flujo de Airflow operativo. El contrato completo se documenta en [DEC-003, DEC-004 y DEC-005](DECISIONES.md).

## Pedidos semanales sugeridos

`inventory.ordering.suggest_weekly_orders` devuelve una cantidad bruta de cobertura por tienda y familia para los siete días siguientes. Reutiliza `forecast_week`, el modelo y la calibración diaria existentes. La calibración semanal suma las siete ventas observadas y resta la suma de los siete pronósticos medianos **para cada semana histórica completa**; usa solo etiquetas entregadas antes del corte de decisión. Los cuantiles 10, 50 y 90 del total semanal se obtienen de esos errores semanales, no sumando cuantiles diarios. Las semanas con alguna venta ausente se excluyen y, si no queda ninguna semana completa, la función falla explícitamente.

El cuantil de decisión es `underforecast_per_unit / (underforecast_per_unit + overforecast_per_unit)`, actualmente 5/6; `suggested_units` es su valor semanal redondeado hacia arriba y limitado a cero. `min_relative_improvement` solo interviene en la selección del modelo. Para cambiar la decisión con el mismo modelo y los mismos datos, pase otra versión de `config/forecast_costs.json` a `costs_path`; el resultado incluye los costos utilizados. Para reproducir una decisión histórica, conserve esa versión de configuración y use el mismo corte y las mismas entregas inmutables.

```python
from inventory.ordering import suggest_weekly_orders

orders = suggest_weekly_orders(
    inbox, start, cutoff, result,
    calibration_first_start=date(2017, 7, 9),
    calibration_last_start=date(2017, 7, 30),
    costs_path=Path("config/forecast_costs.json"),
)
```

El ejemplo usa el `result`, `start` y `cutoff` definidos arriba. `evaluate_models` incorpora en `result.selected_forecast` el modelo elegido, su calibración diaria, el corte en que ya estaban disponibles las etiquetas para seleccionar el modelo y el corte de disponibilidad de la calibración diaria. También registra una huella del estado del modelo. `suggest_weekly_orders` exige ese resultado, comprueba ambas fechas contra **cada corte histórico** y el corte objetivo, y rechaza un modelo Ridge cuyo estado haya cambiado. Las fechas representan disponibilidad de la información en la simulación de entregas, no una fecha real de publicación del artefacto. Un `EvaluationResult` construido manualmente con metadatos falsos no ofrece esta garantía; use el devuelto por `evaluate_models`. Cada recomendación conserva ambas fechas y la huella para auditoría. La función reconstruye las etiquetas de calibración semanal al corte solicitado. Los costos de faltante y merma son una aproximación por unidad de demanda semanal insuficiente o excedente. Sin existencias, pedidos en tránsito, plazos de entrega ni vida útil, `suggested_units` **no es un pedido neto ejecutable**: supone disponibilidad de toda la cantidad al inicio de la semana y que el excedente se valora al final. La calibración semanal global puede ser inestable con pocas semanas o no representar bien una serie particular. Véase [DEC-006](DECISIONES.md).

## Seguimiento de pronósticos publicados

`publish_forecasts` genera los siete pronósticos diarios de cada tienda–familia para un único corte y los guarda en `data/published/forecasts/<publication_id>.json`. El identificador lo proporciona el proceso que publica: repetirlo con los mismos insumos devuelve el artefacto original; usarlo con contenido diferente produce un error de conflicto. El archivo contiene la hora UTC real de escritura, las predicciones, la procedencia lógica y huella del modelo, la fecha y huella de la calibración diaria, el contenido y huella de costos, y las rutas y huellas de las entregas seleccionadas al corte. `read_publication` verifica el esquema y la huella. Las escrituras son atómicas y no sustituyen archivos existentes. `data/published/` está excluido de Git y puede versionarse posteriormente con DVC.

El modo predeterminado, `kind="real"`, toma la hora del reloj durante la escritura y solo admite publicar desde el corte de predicción hasta antes del primer día pronosticado. No acepta una fecha de publicación proporcionada por el llamador. Una reconstrucción retrospectiva exige `kind="historical_simulation"`: conserva la hora real de creación en `recorded_at`, deja `published_at=None` y permanece rotulada como simulación en los reportes. Guardar un artefacto local acredita su escritura, no demuestra entrega a un sistema externo. Para reconstruir el corte histórico del ejemplo anterior:

```python
from inventory.published_forecasts import publish_forecasts

publication = publish_forecasts(
    inbox, start, result, Path("config/forecast_costs.json"),
    publication_id="weekly-20170815",
    kind="historical_simulation",
)
```

`evaluate_publication` lee **las predicciones guardadas**, sin recalcular el modelo, y usa `ingest_sales(inbox, evaluation_at)` para comparar solo las ventas visibles en ese instante UTC. Guarda un reporte por publicación y fecha de evaluación en `data/published/monitoring/`. Las filas sin venta quedan `pending`; una venta cero solo se puntúa cuando existe como registro real. El reporte incluye MAE, RMSE, sesgo medio (`venta − q50`), cobertura inclusiva del intervalo `q10`–`q90`, cantidad evaluada y pendiente. Si no hay observaciones, las cuatro métricas son `None`. Una corrección posterior crea un reporte nuevo al evaluarse con un corte posterior; los anteriores permanecen intactos. Repetir el mismo corte y las mismas ventas es idempotente; si el contenido visible para un corte ya reportado cambia por una entrega retroactiva, se rechaza el conflicto.

```python
from inventory.forecast_monitoring import evaluate_publication

report = evaluate_publication(
    inbox, publication.publication_id,
    evaluation_at=datetime(2017, 8, 16, 6, tzinfo=timezone.utc),
)
print(report.metrics)
```

La fecha de disponibilidad de modelo y calibración procede de los cortes lógicos de `evaluate_models`, no de un registro externo de artefactos; su objeto en memoria puede falsificarse y el modelo y la calibración todavía no se conservan como artefactos independientes. El manifiesto de entregas identifica las entradas visibles para **la predicción al corte**, pero `EvaluationResult` no conserva un manifiesto completo de las versiones usadas al entrenar y seleccionar el modelo. Para reproducir una evaluación se necesitan la publicación y las entregas inmutables originales. Las huellas detectan cambios accidentales de contenido, pero la retención y recuperación entre máquinas requieren el futuro versionado de artefactos. Véase [DEC-007](DECISIONES.md).
