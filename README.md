# Motor de Reposición de Inventario — Laboratorio 1

Proyecto de Product Development con datos públicos de Corporación Favorita. El objetivo es construir un flujo reproducible que, a partir de ventas y otras fuentes históricas, permita generar pronósticos y recomendaciones de reposición. La ingesta actual simula entregas y reconstruye cortes históricos para las cinco fuentes.

## Estructura mínima

```text
src/inventory/       Paquete Python: ingesta, preparación, pronósticos, pedidos y reproducción
tests/               Pruebas pytest con datos sintéticos
data/raw/            Cinco CSV originales, asociados a archivos .dvc
data/inbox/          Entregas simuladas e inmutables; entrada de solo lectura para la ingesta
data/published/      Publicaciones y reportes inmutables; artefactos excluidos de Git
data/processed/      Salidas derivadas previstas
data/reproductions/ Instantánea y particiones de la demostración histórica, mediante DVC
models/              Modelo y calibración serializados, mediante DVC
config/              Costos y umbral de reemplazo versionados
DECISIONES.md        Contratos y decisiones técnicas
```

Los CSV y los artefactos generados están excluidos de Git. Los punteros `.dvc` sí se versionan; DagsHub almacena los objetos.

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

Los otros archivos de la descarga, como `test.csv` y `sample_submission.csv`, no forman parte de los cinco datasets versionados aquí. Cada archivo `.csv.dvc` registra la ruta y la huella del CSV esperado. Si se obtienen versiones o contenidos distintos, no coincidirán con esos metadatos. La demostración histórica descrita abajo añade punteros DVC para la instantánea, las particiones, el modelo, la publicación y el manifiesto; otros artefactos operativos no quedan versionados automáticamente.

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

`config/forecast_costs.json` fija costos unitarios de subestimación y sobreestimación y exige una mejora relativa mínima del 5 %. El baseline es el vigente: Ridge solo lo reemplaza si en selección reduce el costo medio más que ese umbral y no aumenta MAE. La prueba se reserva para informar MAE, RMSE, costo medio, pérdida pinball y cobertura del intervalo 10–90 %. `evaluate_models` conserva en `result.partitions` las filas exactas de entrenamiento, calibración, selección y prueba, que la reproducción histórica persiste; `forecast_week` devuelve pronósticos en memoria. Los cuantiles son globales y pueden ser poco fiables con escasa calibración. Esta métrica es un costo diario de error de demanda, todavía no el costo de un pedido semanal con inventario. Falta integrar inventario, pedidos en tránsito y plazos de entrega para generar pedidos finales. Tampoco hay un flujo de Airflow operativo. El contrato completo se documenta en [DEC-003, DEC-004 y DEC-005](DECISIONES.md).

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

La fecha de disponibilidad de modelo y calibración procede de los cortes lógicos de `evaluate_models`, no de un registro externo de artefactos; un objeto en memoria fabricado externamente todavía podría falsificar esos campos. La demostración siguiente conserva las entradas, particiones, modelo y calibración realmente usados en una ejecución concreta. El manifiesto de la publicación general por sí solo identifica las entradas visibles para la predicción, no todas las versiones históricas del entrenamiento. Véase [DEC-007](DECISIONES.md).

## Reproducción histórica con DVC

El escenario `historical-20170801-store1-grocery-i` reconstruye **una simulación histórica**, no una publicación real de 2017. Pronostica los siete días del 1 al 7 de agosto de 2017 para tienda 1 y familia `GROCERY I`, con corte **2017-07-31 06:00 UTC**. Usa únicamente esa serie de `train.csv`, las fuentes auxiliares entre el 27 de mayo y el 30 de julio, y el catálogo de tiendas. Las entregas de fuentes fechadas se simulan en **D+1 a las 06:00 UTC**; el catálogo, en **T0 = 2013-01-01 00:00 UTC**. Estas son disponibilidades supuestas: Kaggle no proporciona las fechas reales de entrega ni permite acreditar históricamente la vigencia de todos los atributos de tienda.

La evaluación usa cuatro semanas de origen, con una semana de embargo entre ellas: entrenamiento 2017-06-04, calibración 2017-06-18, selección 2017-07-02 y prueba 2017-07-16. Las etiquetas de prueba se leen al corte de reporte **2017-07-24 06:00 UTC**; la selección del modelo y su calibración se fijan antes del corte del pronóstico. `data/reproductions/<id>/inbox` contiene los lotes simulados e inmutables; `processed.json` contiene las filas exactas de las cuatro particiones; `models/<id>.json` contiene el estado numérico del modelo y los residuos diarios en JSON, sin pickle; `data/published/forecasts/<id>.json` es la publicación inmutable con `kind=historical_simulation` y `published_at=null`. `manifest.json` une hashes de esos contenidos, punteros DVC, cinco CSV originales, configuración, parámetros, corte y revisión Git del código. El manifiesto no se referencia a sí mismo.

Desde un **clon limpio de la revisión que contenga los punteros del escenario**, instala el proyecto y DVC como se indica arriba. Configura `auth=basic`, tu usuario y `ask_password=true` con los comandos de «Datos originales»; DVC solicitará tu **propio token** de DagsHub en la terminal. Nunca lo pegues en el chat, en un comando ni en un archivo versionado. Después ejecuta:

```bash
dvc pull
dvc status
python -m scripts.verify_integrated_lineage
```

`dvc pull` recupera los cinco CSV y los cinco artefactos de la demostración desde DagsHub. El manifiesto DVC original conserva el SHA del commit de la rama `10d0db2`; GitHub integró ese contenido mediante Squash and merge en `3707bd5`. La referencia adicional `lineage/historical-20170801-store1-grocery-i.json` vincula el manifiesto original por su SHA-256 al commit integrado, sin alterar el artefacto histórico. El nuevo verificador exige que `3707bd5` sea antecesor del checkout y que **cada archivo de código** de ese commit y del espacio de trabajo coincida con las huellas del manifiesto. Si el commit original de la rama está disponible, también comprueba su contenido. Después ejecuta todas las verificaciones existentes de punteros, lotes, particiones, modelo, calibración y siete predicciones. El resultado esperado es `forecast_rows: 7`, `cutoff_at: 2017-07-31T06:00:00+00:00`, `selected_model: baseline` e `integrated_git_commit: 3707bd575870f01475ebfe202fd52897c9518395`. En los siete días archivados, `q50` es **2368.285714** al redondear a seis decimales. No reescribe la publicación. Si un objeto no se recupera, consulta el acceso a DagsHub de tu cuenta; la lectura anónima con `dvc pull` no se ha verificado y devolvió HTTP 401 en la prueba previa.

Para construir localmente la demostración desde los cinco CSV originales de Kaggle que coincidan con sus `.dvc`, ejecuta `python -m inventory.historical_reproduction build`. El comando procesa `train.csv` en una sola pasada y escribe la instantánea en `data/reproductions/`, fuera de la bandeja operativa `data/inbox/`; repetirlo con las mismas entradas es idempotente, mientras una diferencia se rechaza. Para registrar una ejecución nueva con DVC, aplica `dvc add` a `inbox`, `processed.json`, el JSON del modelo y el JSON de la publicación, en ese orden. Cuando exista el commit de código, fija su SHA mediante `python -m inventory.historical_reproduction manifest --source-git-commit SHA_COMMIT_CODIGO` y ejecuta `dvc add data/reproductions/historical-20170801-store1-grocery-i/manifest.json`. Un borrador local sin SHA se puede finalizar una sola vez si sus demás referencias no cambiaron; un manifiesto ya finalizado no se sustituye. Si el PR se integra con Squash and merge, el SHA de la rama deja de ser antecesor de `main`: conserva ese manifiesto y añade una referencia pequeña al commit integrado que compruebe el hash del manifiesto y el contenido del código, como hace esta demostración. Esta referencia se registra **después** del merge, evitando que su propio SHA dependa del commit que la contiene. Ningún comando de reproducción requiere Airflow.
