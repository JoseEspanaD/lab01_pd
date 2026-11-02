# Motor de Reposición de Inventario — Laboratorio 1

Proyecto de Product Development con datos públicos de Corporación Favorita. El objetivo es construir un flujo reproducible que, a partir de ventas y otras fuentes históricas, permita generar pronósticos y recomendaciones de reposición. La ingesta actual simula entregas y reconstruye cortes históricos para las cinco fuentes.

## Estructura mínima

```text
src/inventory/       Paquete Python: ingesta, preparación, pronósticos, pedidos y reproducción
tests/               Pruebas pytest con datos sintéticos
airflow/dags/         DAGs de prueba, etapas manuales y flujo operativo completo
compose.yaml          Airflow 3 local con API, scheduler, dag processor y SQLite
data/raw/            Cinco CSV originales, asociados a archivos .dvc
data/producer-input/ CSV de corrección externos, excluidos de Git
data/inbox/          Entregas simuladas e inmutables; entrada de solo lectura para la ingesta
data/published/      Pronósticos, pedidos y reportes inmutables; excluidos de Git
data/processed/      Reportes de ingesta y ejemplos preparados por corte
data/reproductions/ Instantánea y particiones de la demostración histórica, mediante DVC
models/              Modelo y calibración serializados, mediante DVC
config/              Costos y umbral de reemplazo versionados
DECISIONES.md        Contratos y decisiones técnicas
```

Los CSV y los artefactos generados están excluidos de Git. Los punteros `.dvc` sí se versionan; DagsHub almacena los objetos.

Las [evidencias de entrega](docs/evidencias_entrega_lab1.md) reúnen la comparación temporal baseline/Ridge, capturas auténticas de Airflow y el seguimiento de la semana reproducida desde un clon limpio.

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

## Airflow 3 local con Docker Compose

La infraestructura ejecuta **Airflow 3.3.2 con Python 3.12**, `LocalExecutor` y SQLite persistido. Incluye `airflow_smoke`, los cuatro DAGs manuales de etapa (`ingest_deliveries`, `prepare_training_data`, `weekly_forecast_orders`, `monitor_published_forecasts`) y `inventory_operational_flow`, manual por defecto y programable solo para una simulación histórica acotada. Se requieren Docker Engine y Docker Compose; Airflow se ejecuta en contenedores y no se instala en el virtualenv del proyecto. En Ubuntu WSL2, desde la raíz del repositorio:

```bash
python3 scripts/init_airflow_env.py
docker compose config --quiet
docker compose up -d
docker compose ps
curl -fsS http://127.0.0.1:8080/api/v2/monitor/health
docker compose exec airflow-dag-processor airflow dags list-import-errors
docker compose exec airflow-dag-processor airflow dags list
```

`init_airflow_env.py` crea `.env` con permisos privados, un UID local y claves aleatorias compartidas por los servicios. Si `.env` ya existe, no lo reemplaza. La inicialización `airflow-init` ejecuta `airflow db migrate` antes de iniciar los demás servicios. Abre **http://127.0.0.1:8080** e inicia sesión con el usuario `admin`; consulta su contraseña generada en tu propia terminal mediante:

```bash
docker compose exec airflow-api-server cat /opt/airflow/state/simple_auth_manager_passwords.json.generated
```

La contraseña, la base SQLite y los logs se guardan en `airflow/state/` y `airflow/logs/`, excluidos de Git. No compartas el contenido de `.env` ni la contraseña. En la interfaz, busca `airflow_smoke`, pulsa **Trigger** y comprueba que la tarea `check_inventory_import` termina en verde. También puedes probar el scheduler y `LocalExecutor` desde la terminal:

```bash
docker compose exec airflow-scheduler airflow dags trigger airflow_smoke
docker compose exec airflow-scheduler airflow dags list-runs airflow_smoke
docker compose down
```

`docker compose down` detiene los servicios y conserva la base y los logs. Para volver a iniciar, usa `docker compose up -d`; no borres `airflow/state/` si quieres conservar el historial. El puerto 8080 solo se publica en `127.0.0.1`. La configuración limita a una tarea y un proceso de análisis de DAG a la vez, y usa un solo proceso de API. SQLite y el gestor simple de usuarios son apropiados para esta práctica local, no para producción ni concurrencia alta. Con aproximadamente **5.63 GiB** disponibles para Docker, vigila `docker stats --no-stream`; el margen de memoria puede ser estrecho al procesar el CSV grande de ventas.

Los contenedores montan `src/` y `airflow/dags/` en modo lectura; `PYTHONPATH` permite importar `inventory`. Montan también `config/`, `data/raw/`, `data/inbox/`, `data/reproductions/` y `models/` en lectura, y `data/processed/` para los reportes de ingesta y preparación y `data/published/` para pronósticos, pedidos y reportes de monitoreo. El scheduler tiene además un montaje separado de escritura de la bandeja en `/opt/airflow/producer/inbox`, utilizado solo por la tarea productora del flujo encadenado; las tareas consumidoras leen `/opt/airflow/lab/data/inbox` montado en lectura. Recupera los objetos DVC **en el host** con tu propia autenticación, siguiendo «Datos originales» y «Reproducción histórica con DVC», antes de ejecutar DAGs que los necesiten. No se monta `.dvc/config.local`, la caché DVC ni ningún token de DagsHub en los contenedores. Véase [DEC-011](DECISIONES.md).

### Productor de entregas simuladas

El servicio puntual `inventory-producer` lee los cinco CSV originales en modo lectura y puede escribir en `data/inbox/`; la tarea productora del flujo encadenado usa otro montaje de escritura de esa misma bandeja. Ambos seleccionan **una fecha de observación por ejecución**, de modo que no vuelven a publicar todo el historial. Los cuatro CSV fechados solo generan lotes cuando su disponibilidad D+1 06:00 UTC no supera el corte indicado; `stores` publica su catálogo inicial desde T0. Si una fuente no tiene filas para la fecha, no crea un lote. El corte debe ser UTC explícito. Para producir la fecha 2017-07-30 y después validarla con el DAG:

```bash
docker compose run --rm inventory-producer --observation-date 2017-07-30 --cutoff-at 2017-07-31T06:00:00Z
docker compose exec airflow-scheduler airflow dags trigger -c '{"cutoff_at":"2017-07-31T06:00:00Z"}' ingest_deliveries
docker compose exec airflow-scheduler airflow dags list-runs ingest_deliveries
```

Repetir la primera orden conserva las entregas idénticas; un CSV cambiado para la misma revisión provoca un conflicto y no sobrescribe el lote. Para avanzar históricamente, ejecuta el productor una vez por fecha deseada y usa el corte correspondiente. La operación examina los CSV originales en cada ejecución, incluido `train.csv`, pero solo publica la fecha solicitada. El productor no inventa revisiones a partir de los CSV originales.

Una corrección es un CSV **completo para una fuente y fecha**, colocado externamente en `data/producer-input/`. `train` solo admite su revisión inicial con el simulador actual. Por ejemplo, si `data/producer-input/transactions-correction.csv` contiene las transacciones corregidas de 2017-07-30:

```bash
docker compose run --rm inventory-producer --observation-date 2017-07-30 --cutoff-at 2017-08-01T06:00:00Z --source transactions --revision 2 --available-at 2017-08-01T06:00:00Z --correction-path /opt/airflow/lab/data/producer-input/transactions-correction.csv
docker compose exec airflow-scheduler airflow dags trigger -c '{"cutoff_at":"2017-08-01T06:00:00Z"}' ingest_deliveries
```

La corrección exige una disponibilidad posterior a D+1 y no posterior al corte; para `stores`, posterior a T0. El corte anterior conserva su selección y el nuevo reporte incorpora la revisión. Una corrección con el mismo número y contenido distinto se rechaza. Las fechas de disponibilidad son supuestos de simulación, no fechas de publicación acreditadas por Kaggle. Véase [DEC-013](DECISIONES.md).

### DAG de ingesta y validación

`ingest_deliveries` se ejecuta solo de forma manual y **exige** `cutoff_at` en la configuración de la ejecución, con formato `AAAA-MM-DDTHH:MM:SSZ` o sufijo `+00:00`. No sustituye un corte ausente por la hora actual ni por la fecha lógica del DAG. Lee `data/inbox/` sin modificarlo, usa las funciones existentes de selección e ingesta de las cinco fuentes y rechaza lotes inválidos o disponibilidades anteriores a **D+1 06:00 UTC** para fuentes fechadas y **T0** para el catálogo. Un lote posterior al corte no entra en el reporte. Si no hay entregas visibles, la tarea falla claramente.

Para validar entregas operativas que ya haya preparado el productor externo en `data/inbox/`, proporciona su corte real de simulación. Por ejemplo:

```bash
docker compose exec airflow-scheduler airflow dags trigger -c '{"cutoff_at":"2017-08-16T06:00:00Z"}' ingest_deliveries
docker compose exec airflow-scheduler airflow dags list-runs ingest_deliveries
```

El resultado se guarda en `data/processed/ingestion/<corte-UTC>.json`: contiene el corte, las rutas relativas y SHA-256 de las entregas seleccionadas y la cantidad de registros válidos por fuente. Repetir un corte con entradas idénticas conserva el mismo archivo; una diferencia para ese corte se rechaza. Una corrección disponible después del corte anterior aparece en un reporte nuevo con un corte posterior. El reporte está excluido de Git y aún no se versiona automáticamente con DVC. La operación vuelve a validar todos los lotes visibles, por lo que su costo crecerá con la bandeja; la optimización incremental queda para otra fase.

Para probar el DAG sin poblar ni modificar `data/inbox/`, una vez recuperados los artefactos DVC de la demostración histórica puedes usar su instantánea en modo lectura. El reporte de esta prueba se guarda solo en el estado local de Airflow. Detén el scheduler durante `dags test` para que no compita por el mismo run y vuelve a iniciarlo al terminar:

```bash
docker compose stop airflow-scheduler
docker compose run --rm -e INVENTORY_INBOX_DIR=/opt/airflow/lab/data/reproductions/historical-20170801-store1-grocery-i/inbox -e INVENTORY_PROCESSED_DIR=/opt/airflow/state/ingestion-demo airflow-scheduler airflow dags test ingest_deliveries -c '{"cutoff_at":"2017-07-31T06:00:00Z"}'
docker compose start airflow-scheduler
```

Este corte y las fechas D+1/T0 son **simulados**, como en la reproducción histórica; no acreditan fechas reales de entrega de Kaggle. Véase [DEC-012](DECISIONES.md).

### DAG de preparación por corte

`prepare_training_data` se ejecuta manualmente **después** de `ingest_deliveries` para el mismo `cutoff_at`; exige además `start_date` y `end_date`, fechas objetivo inclusivas. El reporte de ingesta debe existir y sus entregas seleccionadas deben coincidir con la bandeja actual. Por ejemplo, después de producir las fechas 2017-07-30 y 2017-08-01 y de ejecutar la ingesta con corte 2017-08-02 06:00 UTC:

```bash
docker compose exec airflow-scheduler airflow dags trigger -c '{"start_date":"2017-08-01","end_date":"2017-08-01","cutoff_at":"2017-08-02T06:00:00Z"}' prepare_training_data
docker compose exec airflow-scheduler airflow dags list-runs prepare_training_data
```

La operación reutiliza `inventory.data_preparation.prepare_training_data`: cada etiqueta debe estar disponible al corte solicitado y cada variable se calcula con el corte propio de su fecha objetivo, D−1 06:00 UTC. Guarda un JSON inmutable en `data/processed/preparation/<corte-UTC>/<inicio>_<fin>.json`, con filas, huella del reporte de ingesta y huellas de **todas** las revisiones disponibles hasta el corte, incluidas las anteriores que pueden ser necesarias para variables históricas. Las fechas, horas y decimales se serializan como texto. Una repetición con el mismo linaje lee el resultado existente sin recalcular ejemplos; si aparece una entrega retroactiva o cambia una entrada, se rechaza el corte en vez de sobrescribirlo. Los artefactos operativos están excluidos de Git y aún no se versionan automáticamente con DVC. Véase [DEC-014](DECISIONES.md).

### DAG de pronósticos diarios y cobertura semanal

`weekly_forecast_orders` es manual. `forecast_start_date` identifica el inicio de los siete días objetivo y `cutoff_at` debe ser exactamente el día anterior a las 06:00 UTC. Exige el reporte de ingesta del corte y la preparación del intervalo `train_start` a `forecast_start_date−1` para el **mismo** corte. También exige los cinco inicios de partición de `evaluate_models`, `report_cutoff_at` de evaluación, la primera y última semana de calibración del total, y `kind` (`real` o `historical_simulation`). El corte de evaluación no puede superar el de decisión. La preparación suministra las etiquetas de control para las semanas de calibración; las variables y la selección de modelo se reconstruyen con los cortes propios de `forecasting.py` y `ordering.py`.

Para repetir por etapas la demostración histórica de una serie, recupera antes con DVC la instantánea `data/reproductions/historical-20170801-store1-grocery-i/inbox` y usa estos comandos desde la raíz. El DAG encadenado de la sección «Flujo operativo encadenado» es la ruta principal; esta secuencia permite inspeccionar cada etapa. Los resultados quedan en `airflow/state/weekly-demo/`, fuera de Git:

```bash
docker compose up -d
docker compose stop airflow-scheduler
docker compose run --rm -e INVENTORY_INBOX_DIR=/opt/airflow/lab/data/reproductions/historical-20170801-store1-grocery-i/inbox -e INVENTORY_PROCESSED_DIR=/opt/airflow/state/weekly-demo/processed airflow-scheduler airflow dags test ingest_deliveries -c '{"cutoff_at":"2017-07-31T06:00:00Z"}'
docker compose run --rm -e INVENTORY_INBOX_DIR=/opt/airflow/lab/data/reproductions/historical-20170801-store1-grocery-i/inbox -e INVENTORY_PROCESSED_DIR=/opt/airflow/state/weekly-demo/processed airflow-scheduler airflow dags test prepare_training_data -c '{"start_date":"2017-06-04","end_date":"2017-07-31","cutoff_at":"2017-07-31T06:00:00Z"}'
docker compose run --rm -e INVENTORY_INBOX_DIR=/opt/airflow/lab/data/reproductions/historical-20170801-store1-grocery-i/inbox -e INVENTORY_PROCESSED_DIR=/opt/airflow/state/weekly-demo/processed -e INVENTORY_PUBLISHED_DIR=/opt/airflow/state/weekly-demo/published airflow-scheduler airflow dags test weekly_forecast_orders -c '{"forecast_start_date":"2017-08-01","cutoff_at":"2017-07-31T06:00:00Z","train_start":"2017-06-04","validation_start":"2017-06-18","selection_start":"2017-07-02","test_start":"2017-07-16","last_test_start":"2017-07-16","report_cutoff_at":"2017-07-24T06:00:00Z","calibration_first_start":"2017-07-16","calibration_last_start":"2017-07-16","kind":"historical_simulation"}'
docker compose start airflow-scheduler
```

El DAG publica siete pronósticos diarios por serie mediante `publish_forecasts` y una cobertura semanal bruta mediante las funciones de `ordering`. `kind=historical_simulation` deja `published_at=None` y conserva la hora real de reconstrucción. Las rutas `published/forecasts/weekly-<fecha>-<corte>.json` y `published/orders/weekly-<fecha>-<corte>.json` son inmutables; los pedidos enlazan la huella de la preparación, la publicación diaria, los costos, el modelo y sus fechas de evaluación. Repetir el mismo corte conserva los archivos; entradas o costos distintos causan conflicto. La cobertura bruta no descuenta inventario ni pedidos en tránsito. Véase [DEC-015](DECISIONES.md).

### DAG de monitoreo de publicaciones

`monitor_published_forecasts` recibe `publication_id` y `evaluation_at` UTC explícitos. Lee los siete pronósticos **ya publicados** y solo compara ventas disponibles al corte de evaluación. Cada pronóstico sin venta visible queda `pending`; una venta registrada con valor cero sí cuenta como observada. El reporte inmutable se guarda bajo `published/monitoring/<publication_id>/<corte-UTC>.json`, con huella de la publicación y rutas y SHA-256 de las revisiones de ventas seleccionadas. Calcula MAE, RMSE, sesgo (`venta − q50`) y cobertura inclusiva `q10`–`q90` sobre los pares observados; cuando no hay ninguno, esas métricas son `None`.

Después de crear la publicación `weekly-20170801-20170731T060000Z` con los comandos anteriores, estas órdenes prueban primero un corte sin ventas objetivo visibles y luego el corte en que llega la primera fecha. La entrega del productor es idempotente si ya existe:

```bash
docker compose stop airflow-scheduler
docker compose run --rm -e INVENTORY_PUBLISHED_DIR=/opt/airflow/state/weekly-demo/published airflow-scheduler airflow dags test monitor_published_forecasts -c '{"publication_id":"weekly-20170801-20170731T060000Z","evaluation_at":"2017-08-01T06:00:00Z"}'
docker compose run --rm inventory-producer --observation-date 2017-08-01 --cutoff-at 2017-08-02T06:00:00Z
docker compose run --rm -e INVENTORY_PUBLISHED_DIR=/opt/airflow/state/weekly-demo/published airflow-scheduler airflow dags test monitor_published_forecasts -c '{"publication_id":"weekly-20170801-20170731T060000Z","evaluation_at":"2017-08-02T06:00:00Z"}'
docker compose start airflow-scheduler
```

Las revisiones con `available_at` posterior a un corte no alteran su reporte; un corte nuevo puede mostrar ventas corregidas. Repetir publicación y corte conserva el archivo, y un cambio retroactivo del contenido visible causa conflicto. El monitoreo es manual y no necesita reentrenar ni regenerar la publicación. Véase [DEC-016](DECISIONES.md).

### Flujo operativo encadenado

`inventory_operational_flow` une las cinco etapas en un solo DAG de Airflow 3: `validate → produce → ingest → prepare → publish → monitor`. Cada etapa depende del éxito de la anterior; las tareas tienen un reintento con un minuto de espera y el DAG admite un solo run activo. Los **runs manuales** deben indicar en `dag_run.conf` `mode`, `evaluation_at` UTC y, en modo `forecast`, todos los parámetros de `weekly_forecast_orders`. Los runs programados derivan esos parámetros del reloj histórico optativo descrito abajo. `cutoff_at` de pronóstico debe ser D−1 06:00 UTC respecto a `forecast_start_date`; se transmite sin cambios al productor, la ingesta, la preparación y la publicación. `train_start` y `forecast_start_date−1` delimitan la preparación. `evaluation_at` es el corte del monitoreo y puede ser posterior al de decisión, pero nunca anterior ni futuro. Ningún dato posterior a `cutoff_at` entra en el pronóstico.

El objeto opcional `production` contiene `observation_date` y, si corresponde, `source`, `revision`, `available_at` y `correction_path`. Al omitirlo, el DAG usa entregas históricas ya conservadas en la bandeja, útil para backfill. Al incluirlo, publica solamente esa fecha desde los cinco CSV originales (`source=all` por defecto) o una fuente; repetir una ejecución no duplica ni sustituye entregas. `correction_path` debe señalar un CSV completo montado en `/opt/airflow/lab/data/producer-input/`. La tarea `validate` comprueba los cortes antes de producir. Las operaciones vuelven a verificar sus insumos y rechazan conflictos retroactivos.

#### Semana histórica nueva desde los cinco CSV originales

Esta receta reproduce **2017-07-25 a 2017-07-31** desde un clon nuevo, sin `dvc pull` ni instantáneas preparadas. Requiere Python 3.12 o superior, Docker Engine y Compose, y los cinco CSV originales de Kaggle en un directorio local. `python3` debe apuntar a una versión compatible. Introduce la ruta de ese directorio cuando se solicite; los archivos se copian sin alterar su contenido. No copies `.env`, cachés, `data/inbox/`, `data/processed/`, `data/published/` ni `airflow/state/` de otra instalación.

```bash
git clone --branch main --single-branch https://github.com/JoseEspanaD/lab01_pd.git lab01_pd
cd lab01_pd
python3 --version
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e .
python -m pip install -r requirements-dev.txt
read -r -p 'Directorio con los cinco CSV originales: ' KAGGLE_CSV_DIR
for name in train transactions oil holidays_events stores; do
  test -f "$KAGGLE_CSV_DIR/$name.csv" || { printf 'Falta %s.csv\n' "$name"; exit 1; }
done
for name in train transactions oil holidays_events stores; do
  cp -- "$KAGGLE_CSV_DIR/$name.csv" "data/raw/$name.csv"
done
python -m pytest -q
```

El nombre explícito `lab1jul25` aísla los contenedores y la red de Compose de otros clones. Comprueba que el puerto local 8080 esté libre antes de iniciar el servicio; `down` al final solo afecta a este proyecto. La bandeja comienza vacía salvo por `.gitkeep`.

```bash
python3 scripts/init_airflow_env.py
docker compose -p lab1jul25 config --quiet
docker compose -p lab1jul25 up -d
curl -fsS http://127.0.0.1:8080/api/v2/monitor/health
docker compose -p lab1jul25 exec airflow-dag-processor airflow dags list-import-errors
```

Produce las **58 fechas de observación** del 2017-05-27 al 2017-07-23. Son las necesarias para las variables históricas y las cuatro semanas de evaluación de este ejemplo. Cada fecha usa su propio corte D+1 a las 06:00 UTC; `stores` publica el catálogo inicial en T0. El bucle ejecuta la operación existente una vez por fecha en un solo contenedor, sin copiar resultados DVC ni regenerar los lotes anteriores. Lee `train.csv` completo en cada iteración y puede tardar varios minutos.

```bash
docker compose -p lab1jul25 run --rm --entrypoint python inventory-producer -c '
from datetime import date, timedelta
from pathlib import Path
from inventory.operations.production import produce_for_date

raw = Path("/opt/airflow/lab/data/raw")
inbox = Path("/opt/airflow/lab/data/inbox")
first, last = date(2017, 5, 27), date(2017, 7, 23)
for offset in range((last - first).days + 1):
    day = first + timedelta(days=offset)
    cutoff = (day + timedelta(days=1)).isoformat() + "T06:00:00Z"
    result = produce_for_date(day.isoformat(), cutoff, raw, inbox)
    print(day, len(result["deliveries"]), flush=True)
'
```

La semana objetivo empieza el **2017-07-25** y comparte el corte de pronóstico **2017-07-24 06:00 UTC**. Entrenamiento, calibración, selección y prueba empiezan el 4 y 18 de junio, y el 2 y 16 de julio, respectivamente; hay una semana de embargo entre bloques. El corte de reporte de prueba es también 2017-07-24 06:00 UTC. `historical_simulation` identifica una reconstrucción, no una publicación real de 2017. Con el scheduler activo, dispara el DAG y espera a que su run más reciente indique `success`:

```bash
FORECAST_CONF='{"mode":"forecast","forecast_start_date":"2017-07-25","cutoff_at":"2017-07-24T06:00:00Z","evaluation_at":"2017-07-25T06:00:00Z","train_start":"2017-06-04","validation_start":"2017-06-18","selection_start":"2017-07-02","test_start":"2017-07-16","last_test_start":"2017-07-16","report_cutoff_at":"2017-07-24T06:00:00Z","calibration_first_start":"2017-07-16","calibration_last_start":"2017-07-16","kind":"historical_simulation"}'
wait_flow() {
  while :; do
    FLOW_STATE=$(docker compose -p lab1jul25 exec airflow-scheduler airflow dags list-runs -o json inventory_operational_flow | python -c 'import json,sys; print(json.load(sys.stdin)[0]["state"])')
    printf 'Estado: %s\n' "$FLOW_STATE"
    case "$FLOW_STATE" in success) return 0 ;; failed) return 1 ;; esac
    sleep 5
  done
}
docker compose -p lab1jul25 exec airflow-scheduler airflow dags trigger -c "$FORECAST_CONF" inventory_operational_flow
wait_flow
```

Verifica los artefactos creados en este clon. Con los CSV originales de la prueba, deben aparecer 161 entregas seleccionadas, 89 100 filas preparadas, 12 474 pronósticos diarios, 1 782 recomendaciones semanales y un primer monitoreo con 12 474 pares pendientes. Los nombres de archivo identifican el corte y no contienen datos futuros.

```bash
python - <<'PY'
import json
from pathlib import Path

def payload(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))["payload"]

cut = "2017-07-24T060000Z"
publication = "weekly-20170725-20170724T060000Z"
ingestion = payload(f"data/processed/ingestion/{cut}.json")
prepared = payload(f"data/processed/preparation/{cut}/2017-06-04_2017-07-24.json")
forecasts = payload(f"data/published/forecasts/{publication}.json")
orders = payload(f"data/published/orders/{publication}.json")
initial = payload(f"data/published/monitoring/{publication}/2017-07-25T060000.000000Z.json")
assert ingestion["cutoff_at"] == forecasts["prediction_cutoff_at"] == "2017-07-24T06:00:00+00:00"
assert len(ingestion["selected_deliveries"]) == 161
assert prepared["row_count"] == 89100
assert len(forecasts["forecasts"]) == 12474
assert orders["order_count"] == 1782
assert initial["metrics"]["evaluated_pairs"] == 0
assert initial["metrics"]["pending_pairs"] == 12474
print("Ingesta, preparación, pronósticos, pedidos y monitoreo inicial: OK")
PY
```

Para comprobar idempotencia, registra las huellas de esos cinco archivos, repite el mismo trigger y comprueba que no cambien. Después, un `refresh` produce las ventas del primer día objetivo, disponibles el **2017-07-26 06:00 UTC**, y crea un nuevo reporte de monitoreo sin recalcular el pronóstico:

```bash
ARTIFACTS=(
  data/processed/ingestion/2017-07-24T060000Z.json
  data/processed/preparation/2017-07-24T060000Z/2017-06-04_2017-07-24.json
  data/published/forecasts/weekly-20170725-20170724T060000Z.json
  data/published/orders/weekly-20170725-20170724T060000Z.json
  data/published/monitoring/weekly-20170725-20170724T060000Z/2017-07-25T060000.000000Z.json
)
ARTIFACT_HASHES_BEFORE=$(sha256sum "${ARTIFACTS[@]}")
docker compose -p lab1jul25 exec airflow-scheduler airflow dags trigger -c "$FORECAST_CONF" inventory_operational_flow
wait_flow
test "$ARTIFACT_HASHES_BEFORE" = "$(sha256sum "${ARTIFACTS[@]}")"
docker compose -p lab1jul25 exec airflow-scheduler airflow dags trigger -c '{"mode":"refresh","evaluation_at":"2017-07-26T06:00:00Z","production":{"observation_date":"2017-07-25","source":"train"}}' inventory_operational_flow
wait_flow
python - <<'PY'
import json
from pathlib import Path

path = Path("data/published/monitoring/weekly-20170725-20170724T060000Z/2017-07-26T060000.000000Z.json")
metrics = json.loads(path.read_text(encoding="utf-8"))["payload"]["metrics"]
assert (metrics["evaluated_pairs"], metrics["pending_pairs"]) == (1782, 10692)
print(metrics)
PY
docker compose -p lab1jul25 down
```

La ejecución comprobada está descrita en [docs/reproduccion_clon_limpio.md](docs/reproduccion_clon_limpio.md). El productor procesa una fecha por llamada; no se ha probado una corrección de contenido con estos cinco CSV. Las disponibilidades D+1/T0 son supuestos de simulación, no registros de entrega reales de Kaggle.

#### Reloj periódico para una simulación histórica

La programación de `inventory_operational_flow` está **desactivada por defecto**. Para observarla, usa un clon y un nombre Compose propios: ejecuta los pasos de instalación, inicio de Compose y producción de las 58 fechas de la receta anterior, sustituyendo `lab1jul25` por `lab1periodic01`. Detente después del bucle productor: no ejecutes los triggers manuales de esa receta. Los cinco CSV originales son las únicas entradas; no se requiere la instantánea DVC. Antes de cambiar el calendario, pausa el DAG ya registrado:

```bash
PROJECT=lab1periodic01
docker compose -p "$PROJECT" exec airflow-scheduler airflow dags pause inventory_operational_flow
WALL_START_AT=$(python - <<'PY'
from datetime import datetime, timedelta, timezone

instant = datetime.now(timezone.utc) + timedelta(minutes=10)
instant = instant.replace(second=0, microsecond=0)
instant += timedelta(minutes=(-instant.minute) % 5)
print(instant.strftime("%Y-%m-%dT%H:%M:%SZ"))
PY
)
cat >> .env <<EOF
INVENTORY_SIMULATION_ENABLED=true
INVENTORY_SIMULATION_WALL_START_AT=$WALL_START_AT
INVENTORY_SIMULATION_FIRST_CUTOFF_AT=2017-07-24T06:00:00Z
INVENTORY_SIMULATION_TICK_MINUTES=5
INVENTORY_SIMULATION_TICKS=10
EOF
docker compose -p "$PROJECT" config --quiet
docker compose -p "$PROJECT" up -d --force-recreate airflow-api-server airflow-scheduler airflow-dag-processor
docker compose -p "$PROJECT" exec airflow-dag-processor airflow dags list-import-errors
docker compose -p "$PROJECT" exec airflow-scheduler airflow dags unpause inventory_operational_flow
```

El primer tick de reloj `WALL_START_AT` representa **2017-07-24 06:00 UTC**; cada tick real de cinco minutos avanza **un día histórico**, sin usar la fecha del sistema como venta. El tick 0 produce de forma idempotente el 23 de julio y pronostica el 25–31 de julio; los ticks 1 y 2 solo actualizan entregas, ingesta y monitoreo con cortes del 25 y 26 de julio. El tick 7 publica la semana del 1 de agosto. Las particiones de evaluación se desplazan siete días entre publicaciones. Los diez ticks terminan en el corte histórico **2017-08-02 06:00 UTC**; Airflow no programa más runs de este calendario. `evaluation_at` coincide con el corte de cada tick: ningún monitoreo ve ventas de una fecha posterior. Todas las publicaciones programadas tienen `kind=historical_simulation`.

Observa al menos tres runs creados por el scheduler, sin dispararlos manualmente. En la lista deben figurar `scheduled__...` con estado `success`; las primeras tres fechas lógicas están separadas cinco minutos. El primer run crea pronósticos y pedidos, el tercero incorpora las primeras ventas objetivo al monitoreo, sin crear otro pronóstico ni pedido:

```bash
for attempt in {1..50}; do
  SCHEDULED_OK=$(docker compose -p "$PROJECT" exec -T airflow-scheduler airflow dags list-runs -o json inventory_operational_flow | python -c 'import json,sys; print(sum(r["state"] == "success" and r["run_id"].startswith("scheduled__") for r in json.load(sys.stdin)))')
  printf 'Runs programados exitosos: %s\n' "$SCHEDULED_OK"
  if test "$SCHEDULED_OK" -ge 3; then break; fi
  sleep 30
done
test "$SCHEDULED_OK" -ge 3
docker compose -p "$PROJECT" exec airflow-scheduler airflow dags list-runs inventory_operational_flow
test -f data/published/forecasts/weekly-20170725-20170724T060000Z.json
test -f data/published/orders/weekly-20170725-20170724T060000Z.json
test -f data/published/monitoring/weekly-20170725-20170724T060000Z/2017-07-26T060000.000000Z.json
```

Pausa y detén **solo** el proyecto de esta prueba; no uses `down -v` si quieres conservar sus artefactos. Para otra semana histórica, configura otro primer corte UTC a las 06:00 y prepara previamente sus observaciones de entrenamiento. `WALL_START_AT` debe ser un tick UTC futuro alineado con el intervalo; `TICK_MINUTES` puede ser 1, 2, 3, 4, 5, 6, 10, 12, 15, 20, 30 o 60, y `TICKS` entre 1 y 366. Si Compose estuvo detenido, `catchup=True` recupera en orden los ticks pendientes dentro del límite; pausa antes de detenerlo si no quieres esa recuperación. Los runs manuales continúan aceptando `dag_run.conf` explícito para backfill y correcciones externas. Para desactivar también el calendario en un reinicio, cambia `INVENTORY_SIMULATION_ENABLED=false` en tu `.env` local.

```bash
docker compose -p "$PROJECT" exec airflow-scheduler airflow dags pause inventory_operational_flow
docker compose -p "$PROJECT" down
```

#### Demostración con instantánea histórica DVC

Para una ejecución histórica de una serie desde los objetos DVC, copia la instantánea recuperada a un directorio local excluido de Git, omitiendo el catálogo inicial para que la tarea `produce` lo genere desde `stores.csv`. La copia preserva el resto del historial y evita volver a crear 65 días de entregas:

```bash
python -c 'from pathlib import Path; from shutil import copytree; src=Path("data/reproductions/historical-20170801-store1-grocery-i/inbox"); copytree(src, Path("airflow/state/operational-demo/inbox"), ignore=lambda directory, names: {"source=stores"} if Path(directory)==src else set())'
docker compose up -d
docker compose stop airflow-scheduler
docker compose run --rm -e INVENTORY_INBOX_DIR=/opt/airflow/state/operational-demo/inbox -e INVENTORY_PRODUCER_INBOX_DIR=/opt/airflow/state/operational-demo/inbox -e INVENTORY_PROCESSED_DIR=/opt/airflow/state/operational-demo/processed -e INVENTORY_PUBLISHED_DIR=/opt/airflow/state/operational-demo/published airflow-scheduler airflow dags test inventory_operational_flow -c '{"mode":"forecast","forecast_start_date":"2017-08-01","cutoff_at":"2017-07-31T06:00:00Z","evaluation_at":"2017-08-01T06:00:00Z","train_start":"2017-06-04","validation_start":"2017-06-18","selection_start":"2017-07-02","test_start":"2017-07-16","last_test_start":"2017-07-16","report_cutoff_at":"2017-07-24T06:00:00Z","calibration_first_start":"2017-07-16","calibration_last_start":"2017-07-16","kind":"historical_simulation","production":{"observation_date":"2017-07-30","source":"stores"}}'
```

El primer monitoreo deja siete pares pendientes porque el corte de evaluación todavía no tiene ventas objetivo. Para publicar ventas de la primera fecha y actualizar **todas** las publicaciones históricas afectadas, usa `mode=refresh`; este modo usa `evaluation_at` como corte de producción e ingesta, y omite preparación y pronóstico:

```bash
docker compose run --rm -e INVENTORY_INBOX_DIR=/opt/airflow/state/operational-demo/inbox -e INVENTORY_PRODUCER_INBOX_DIR=/opt/airflow/state/operational-demo/inbox -e INVENTORY_PROCESSED_DIR=/opt/airflow/state/operational-demo/processed -e INVENTORY_PUBLISHED_DIR=/opt/airflow/state/operational-demo/published airflow-scheduler airflow dags test inventory_operational_flow -c '{"mode":"refresh","evaluation_at":"2017-08-02T06:00:00Z","production":{"observation_date":"2017-08-01","source":"train"}}'
docker compose start airflow-scheduler
```

El monitoreo recorre las publicaciones guardadas y crea un nuevo reporte únicamente cuando cambia la revisión de ventas de alguna fecha objetivo, o cuando aún no existe reporte; también revisa publicaciones ya evaluadas para captar correcciones. No convierte faltantes en cero. Una revisión externa completa de `train` con `available_at` posterior se incorpora con otro `refresh` al nuevo corte; el productor actual solo crea la revisión inicial de `train`. Un segundo run idéntico conserva entregas y artefactos; un conflicto para un mismo corte falla sin sobrescribir. Para backfill, ejecuta los cortes en orden cronológico y conserva la bandeja, los reportes y los CSV de corrección externos. `airflow dags test` debe ejecutarse con el scheduler detenido para evitar que ambos intenten procesar el mismo run; para ejecuciones normales usa `airflow dags trigger` con el scheduler activo y los montajes predeterminados. En modo `real`, el primer monitoreo requiere un corte posterior a la hora real de publicación; si aún no existe, un `refresh` posterior lo evaluará. La única programación disponible es la simulación histórica optativa descrita arriba; no hay eventos automáticos de llegada, espera por lotes tardíos ni activación desde servicios externos. Véanse [DEC-017 y DEC-018](DECISIONES.md).

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

Para consultar la CLI original de ventas y simular **una sola fecha de ventas** desde la raíz:

```bash
python -m inventory.train_delivery --help
python -m inventory.train_delivery 2017-08-15
```

La segunda orden lee `data/raw/train.csv` y publica, si hay registros para esa fecha, una entrega en `data/inbox/`. La CLI `python -m inventory.operations.production` reúne las cinco fuentes en una fecha y acepta `--raw-dir` y `--inbox-dir` para pruebas fuera de Compose.

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

`config/forecast_costs.json` fija costos unitarios de subestimación y sobreestimación y exige una mejora relativa mínima del 5 %. El baseline es el vigente: Ridge solo lo reemplaza si en selección reduce el costo medio más que ese umbral y no aumenta MAE. La prueba se reserva para informar MAE, RMSE, costo medio, pérdida pinball y cobertura del intervalo 10–90 %. `evaluate_models` conserva en `result.partitions` las filas exactas de entrenamiento, calibración, selección y prueba, que la reproducción histórica persiste; `forecast_week` devuelve pronósticos en memoria. Los cuantiles son globales y pueden ser poco fiables con escasa calibración. Esta métrica es un costo diario de error de demanda, todavía no el costo de un pedido semanal con inventario. Falta integrar inventario, pedidos en tránsito y plazos de entrega para generar pedidos finales. El DAG `inventory_operational_flow` encadena ingesta, preparación, pronósticos, pedidos y monitoreo; los DAGs de etapa siguen disponibles para ejecuciones manuales. El contrato completo se documenta en [DEC-003, DEC-004 y DEC-005](DECISIONES.md).

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

## DVC de un período operativo de Airflow

El flujo de pronóstico guarda ahora `data/published/model_states/<publication_id>.json` de forma inmutable. Contiene el estado del modelo **seleccionado** por la regla existente, los residuos de calibración diaria y semanal, métricas y cortes de disponibilidad. Sus huellas se comprueban contra el pronóstico y los pedidos publicados. No modifica la selección baseline/Ridge. Para congelar un período deben haber terminado el run de pronóstico y el `refresh` que produjo el reporte de monitoreo deseado. Por ejemplo, después de la receta «Semana histórica nueva desde los cinco CSV originales», con evaluación del **2017-07-26 06:00 UTC**:

```bash
PUBLICATION_ID=weekly-20170725-20170724T060000Z
EVALUATION_AT=2017-07-26T06:00:00Z
ARCHIVE=data/reproductions/operational/${PUBLICATION_ID}__eval-20170726T060000Z
python -m inventory.operations.operational_archive freeze \
  --publication-id "$PUBLICATION_ID" --evaluation-at "$EVALUATION_AT"
python -m inventory.operations.operational_archive verify --archive "$ARCHIVE"
dvc add "$ARCHIVE"
dvc status
```

La congelación copia solo lotes y revisiones con `available_at` no posterior al corte de evaluación; el manifiesto distingue cuáles estaban disponibles en el corte anterior de pronóstico. Incluye ingesta, preparación, modelo, pronósticos, pedidos y **un reporte** de monitoreo, con SHA-256 de cada archivo, de los cinco punteros DVC originales, del código y de costos. Se verifica antes de aceptar la carpeta. Repetir el comando con los mismos insumos conserva sus bytes y fechas; una corrección retroactiva o un archivo distinto bajo el mismo identificador causa conflicto. Para otro corte de monitoreo se crea otra carpeta, sin modificar la anterior. DVC trabaja con la copia cerrada y no ejecuta `checkout` ni `add` sobre la bandeja o las publicaciones vivas de Airflow.

Después de revisar el puntero generado, un PR debe incorporar **solo** `"$ARCHIVE.dvc"` y el código/documentación; los JSON y CSV de la carpeta permanecen fuera de Git. Publicar los objetos DVC requiere una autorización de escritura en DagsHub. Usa la autenticación local descrita en «Datos originales» y, cuando corresponda publicar, ejecuta:

```bash
dvc push "$ARCHIVE.dvc"
```

Desde un clon limpio de la revisión que incluya ese puntero, instala el paquete y DVC como se indicó al inicio. Usa tu propia cuenta de DagsHub; el token se solicita de forma interactiva y no se guarda en Git. Para recuperar y comprobar el mismo período:

```bash
dvc remote modify --local dagshub auth basic
dvc remote modify --local dagshub user TU_USUARIO_DAGSHUB
dvc remote modify --local dagshub ask_password true
PUBLICATION_ID=weekly-20170725-20170724T060000Z
ARCHIVE=data/reproductions/operational/${PUBLICATION_ID}__eval-20170726T060000Z
dvc pull
dvc status
python -m inventory.operations.operational_archive verify --archive "$ARCHIVE"
```

El verificador vuelve a calcular la evaluación temporal, los pronósticos, pedidos y monitoreo desde la instantánea, sin reescribir una publicación. Exige el mismo código y configuración registrados en el manifiesto; para una versión posterior del repositorio usa el commit o tag que incorporó el puntero. El remoto DVC actual requiere autenticación para lectura. El archivo operativo solo queda recuperable desde DagsHub después de que su objeto se haya enviado con `dvc push` y su puntero forme parte de la revisión clonada. Véase [DEC-019](DECISIONES.md).
