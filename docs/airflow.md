# Airflow 3: operación y recetas

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

Los contenedores montan `src/` y `airflow/dags/` en modo lectura; `PYTHONPATH` permite importar `inventory`. Montan también `config/`, `data/raw/`, `data/inbox/`, `data/reproductions/` y `models/` en lectura, y `data/processed/` para los reportes de ingesta y preparación y `data/published/` para pronósticos, pedidos y reportes de monitoreo. El scheduler tiene además un montaje separado de escritura de la bandeja en `/opt/airflow/producer/inbox`, utilizado solo por la tarea productora del flujo encadenado; las tareas consumidoras leen `/opt/airflow/lab/data/inbox` montado en lectura. Recupera los objetos DVC **en el host** con tu propia autenticación, siguiendo [datos originales](dvc.md#datos-originales) y [reproducción histórica con DVC](dvc.md#reproducción-histórica-con-dvc), antes de ejecutar DAGs que los necesiten. No se monta `.dvc/config.local`, la caché DVC ni ningún token de DagsHub en los contenedores. Véase [DEC-011](../DECISIONES.md).

## Productor de entregas simuladas

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

La corrección exige una disponibilidad posterior a D+1 y no posterior al corte; para `stores`, posterior a T0. El corte anterior conserva su selección y el nuevo reporte incorpora la revisión. Una corrección con el mismo número y contenido distinto se rechaza. Las fechas de disponibilidad son supuestos de simulación, no fechas de publicación acreditadas por Kaggle. Véase [DEC-013](../DECISIONES.md).

## DAG de ingesta y validación

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

Este corte y las fechas D+1/T0 son **simulados**, como en la reproducción histórica; no acreditan fechas reales de entrega de Kaggle. Véase [DEC-012](../DECISIONES.md).

## DAG de preparación por corte

`prepare_training_data` se ejecuta manualmente **después** de `ingest_deliveries` para el mismo `cutoff_at`; exige además `start_date` y `end_date`, fechas objetivo inclusivas. El reporte de ingesta debe existir y sus entregas seleccionadas deben coincidir con la bandeja actual. Por ejemplo, después de producir las fechas 2017-07-30 y 2017-08-01 y de ejecutar la ingesta con corte 2017-08-02 06:00 UTC:

```bash
docker compose exec airflow-scheduler airflow dags trigger -c '{"start_date":"2017-08-01","end_date":"2017-08-01","cutoff_at":"2017-08-02T06:00:00Z"}' prepare_training_data
docker compose exec airflow-scheduler airflow dags list-runs prepare_training_data
```

La operación reutiliza `inventory.data_preparation.prepare_training_data`: cada etiqueta debe estar disponible al corte solicitado y cada variable se calcula con el corte propio de su fecha objetivo, D−1 06:00 UTC. Guarda un JSON inmutable en `data/processed/preparation/<corte-UTC>/<inicio>_<fin>.json`, con filas, huella del reporte de ingesta y huellas de **todas** las revisiones disponibles hasta el corte, incluidas las anteriores que pueden ser necesarias para variables históricas. Las fechas, horas y decimales se serializan como texto. Una repetición con el mismo linaje lee el resultado existente sin recalcular ejemplos; si aparece una entrega retroactiva o cambia una entrada, se rechaza el corte en vez de sobrescribirlo. Los artefactos operativos están excluidos de Git y aún no se versionan automáticamente con DVC. Véase [DEC-014](../DECISIONES.md).

## DAG de pronósticos diarios y cobertura semanal

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

El DAG publica siete pronósticos diarios por serie mediante `publish_forecasts` y una cobertura semanal bruta mediante las funciones de `ordering`. `kind=historical_simulation` deja `published_at=None` y conserva la hora real de reconstrucción. Las rutas `published/forecasts/weekly-<fecha>-<corte>.json` y `published/orders/weekly-<fecha>-<corte>.json` son inmutables; los pedidos enlazan la huella de la preparación, la publicación diaria, los costos, el modelo y sus fechas de evaluación. Repetir el mismo corte conserva los archivos; entradas o costos distintos causan conflicto. La cobertura bruta no descuenta inventario ni pedidos en tránsito. Véase [DEC-015](../DECISIONES.md).

## DAG de monitoreo de publicaciones

`monitor_published_forecasts` recibe `publication_id` y `evaluation_at` UTC explícitos. Lee los siete pronósticos **ya publicados** y solo compara ventas disponibles al corte de evaluación. Cada pronóstico sin venta visible queda `pending`; una venta registrada con valor cero sí cuenta como observada. El reporte inmutable se guarda bajo `published/monitoring/<publication_id>/<corte-UTC>.json`, con huella de la publicación y rutas y SHA-256 de las revisiones de ventas seleccionadas. Calcula MAE, RMSE, sesgo (`venta − q50`) y cobertura inclusiva `q10`–`q90` sobre los pares observados; cuando no hay ninguno, esas métricas son `None`.

Después de crear la publicación `weekly-20170801-20170731T060000Z` con los comandos anteriores, estas órdenes prueban primero un corte sin ventas objetivo visibles y luego el corte en que llega la primera fecha. La entrega del productor es idempotente si ya existe:

```bash
docker compose stop airflow-scheduler
docker compose run --rm -e INVENTORY_PUBLISHED_DIR=/opt/airflow/state/weekly-demo/published airflow-scheduler airflow dags test monitor_published_forecasts -c '{"publication_id":"weekly-20170801-20170731T060000Z","evaluation_at":"2017-08-01T06:00:00Z"}'
docker compose run --rm inventory-producer --observation-date 2017-08-01 --cutoff-at 2017-08-02T06:00:00Z
docker compose run --rm -e INVENTORY_PUBLISHED_DIR=/opt/airflow/state/weekly-demo/published airflow-scheduler airflow dags test monitor_published_forecasts -c '{"publication_id":"weekly-20170801-20170731T060000Z","evaluation_at":"2017-08-02T06:00:00Z"}'
docker compose start airflow-scheduler
```

Las revisiones con `available_at` posterior a un corte no alteran su reporte; un corte nuevo puede mostrar ventas corregidas. Repetir publicación y corte conserva el archivo, y un cambio retroactivo del contenido visible causa conflicto. El monitoreo es manual y no necesita reentrenar ni regenerar la publicación. Véase [DEC-016](../DECISIONES.md).

## Flujo operativo encadenado

`inventory_operational_flow` une las cinco etapas en un solo DAG de Airflow 3: `validate → produce → ingest → prepare → publish → monitor`. Cada etapa depende del éxito de la anterior; las tareas tienen un reintento con un minuto de espera y el DAG admite un solo run activo. Los **runs manuales** deben indicar en `dag_run.conf` `mode`, `evaluation_at` UTC y, en modo `forecast`, todos los parámetros de `weekly_forecast_orders`. Los runs programados derivan esos parámetros del reloj histórico optativo descrito abajo. `cutoff_at` de pronóstico debe ser D−1 06:00 UTC respecto a `forecast_start_date`; se transmite sin cambios al productor, la ingesta, la preparación y la publicación. `train_start` y `forecast_start_date−1` delimitan la preparación. `evaluation_at` es el corte del monitoreo y puede ser posterior al de decisión, pero nunca anterior ni futuro. Ningún dato posterior a `cutoff_at` entra en el pronóstico.

El objeto opcional `production` contiene `observation_date` y, si corresponde, `source`, `revision`, `available_at` y `correction_path`. Al omitirlo, el DAG usa entregas históricas ya conservadas en la bandeja, útil para backfill. Al incluirlo, publica solamente esa fecha desde los cinco CSV originales (`source=all` por defecto) o una fuente; repetir una ejecución no duplica ni sustituye entregas. `correction_path` debe señalar un CSV completo montado en `/opt/airflow/lab/data/producer-input/`. La tarea `validate` comprueba los cortes antes de producir. Las operaciones vuelven a verificar sus insumos y rechazan conflictos retroactivos.

### Semana histórica nueva desde los cinco CSV originales

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

La ejecución comprobada está descrita en [docs/reproduccion_clon_limpio.md](reproduccion_clon_limpio.md). El productor procesa una fecha por llamada; no se ha probado una corrección de contenido con estos cinco CSV. Las disponibilidades D+1/T0 son supuestos de simulación, no registros de entrega reales de Kaggle.

### Reloj periódico para una simulación histórica

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

### Demostración con instantánea histórica DVC

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

El monitoreo recorre las publicaciones guardadas y crea un nuevo reporte únicamente cuando cambia la revisión de ventas de alguna fecha objetivo, o cuando aún no existe reporte; también revisa publicaciones ya evaluadas para captar correcciones. No convierte faltantes en cero. Una revisión externa completa de `train` con `available_at` posterior se incorpora con otro `refresh` al nuevo corte; el productor actual solo crea la revisión inicial de `train`. Un segundo run idéntico conserva entregas y artefactos; un conflicto para un mismo corte falla sin sobrescribir. Para backfill, ejecuta los cortes en orden cronológico y conserva la bandeja, los reportes y los CSV de corrección externos. `airflow dags test` debe ejecutarse con el scheduler detenido para evitar que ambos intenten procesar el mismo run; para ejecuciones normales usa `airflow dags trigger` con el scheduler activo y los montajes predeterminados. En modo `real`, el primer monitoreo requiere un corte posterior a la hora real de publicación; si aún no existe, un `refresh` posterior lo evaluará. La única programación disponible es la simulación histórica optativa descrita arriba; no hay eventos automáticos de llegada, espera por lotes tardíos ni activación desde servicios externos. Véanse [DEC-017 y DEC-018](../DECISIONES.md).
