# Motor de Reposición de Inventario — Laboratorio 1

Este laboratorio reconstruye entregas de las cinco fuentes históricas de Corporación Favorita para pronosticar ventas diarias, recomendar cobertura semanal y medir el desempeño cuando llegan ventas o correcciones. Las disponibilidades D+1/T0 son **simuladas**; cada operación usa un corte UTC explícito y conserva sus resultados por corte.

## Instalación y ejecución

Se requieren **Python 3.12 o superior**, Git, Docker Engine y Docker Compose. Desde un clon limpio:

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

Si tu Python compatible tiene otro nombre, sustituye `python3.12`. Las pruebas usan datos sintéticos y no requieren Kaggle. Coloca en `data/raw/`, sin modificar su contenido ni nombre, **`train.csv`, `transactions.csv`, `oil.csv`, `holidays_events.csv` y `stores.csv`** de [Store Sales - Time Series Forecasting](https://www.kaggle.com/competitions/store-sales-time-series-forecasting/data). También puedes recuperarlos con [DVC y tu propia cuenta de DagsHub](docs/dvc.md#datos-originales). Ninguna credencial se versiona.

Para iniciar Airflow 3.3.2 en un proyecto Compose local:

```bash
python3 scripts/init_airflow_env.py
docker compose config --quiet
docker compose up -d
curl -fsS http://127.0.0.1:8080/api/v2/monitor/health
docker compose exec airflow-dag-processor airflow dags list-import-errors
```

Abre [Airflow en localhost](http://127.0.0.1:8080). El usuario es `admin`; consulta la contraseña **en tu terminal** con `docker compose exec airflow-api-server cat /opt/airflow/state/simple_auth_manager_passwords.json.generated`. Al terminar, usa `docker compose down`; conserva el estado local. [Configuración, servicios y DAGs](docs/airflow.md#airflow-3-operación-y-recetas).

### Semana histórica nueva desde los cinco CSV originales

Para ejecutar el producto desde cero, sigue la [receta completa de una semana histórica no publicada](docs/airflow.md#semana-histórica-nueva-desde-los-cinco-csv-originales). Incluye clon, copiado de CSV, producción de las 58 fechas requeridas, cortes UTC, disparo de `inventory_operational_flow`, comprobación de pronósticos, pedidos y monitoreo, repetición idempotente y limpieza de un proyecto Compose aislado. La [prueba real desde un clon limpio](docs/reproduccion_clon_limpio.md) documenta comandos y resultados.

### Reloj periódico para una simulación histórica

La [simulación periódica optativa](docs/airflow.md#reloj-periódico-para-una-simulación-histórica) procesa ticks históricos; está desactivada por defecto. Los otros cinco DAGs se ejecutan manualmente.

## Producto y documentación

### Pronósticos semanales y evaluación

`inventory_operational_flow` encadena `validate → produce → ingest → prepare → publish → monitor`. El productor deposita entregas por fecha y fuente; la ingesta selecciona solo revisiones disponibles al corte. La preparación evita datos futuros. El pronóstico compara un baseline con Ridge, produce cuantiles 10, 50 y 90 y aplica la regla de selección por costo; los pedidos son cobertura semanal bruta según costos configurables. El monitoreo evalúa publicaciones inmutables solo contra ventas observadas hasta su corte, dejando pendientes los faltantes. Los runs repetidos conservan artefactos idénticos y rechazan conflictos. Consulta la [evaluación temporal y regla de selección](docs/producto.md#pronósticos-semanales-y-evaluación).

| Documento | Contenido |
| --- | --- |
| [Operación con Airflow](docs/airflow.md) | Compose, seis DAGs, productor, ejecución manual, backfill, correcciones y calendario histórico. |
| [Producto y modelos](docs/producto.md) | Contratos de ingesta, variables, evaluación temporal, cuantiles, pedidos y monitoreo. |
| [DVC y reproducción](docs/dvc.md) | CSV originales, linaje histórico y congelación de un período operativo. |
| [Evidencias de entrega](docs/evidencias_entrega_lab1.md) | Comparación baseline/Ridge, tres runs programados, seis tareas exitosas y métricas de monitoreo. |
| [Reproducción desde clon limpio](docs/reproduccion_clon_limpio.md) | Comandos esenciales y resultados observados con solo cinco CSV. |
| [Decisiones y limitaciones](DECISIONES.md) | Alternativas, razones técnicas y límites de la simulación. |

El código de negocio está en `src/inventory/`, los DAGs en `airflow/dags/` y las pruebas en `tests/`. Las entregas y salidas operativas permanecen fuera de Git. Los cinco CSV, la demostración histórica y el [período operativo congelado](data/reproductions/operational/weekly-20170725-20170724T060000Z__eval-20170726T060000Z.dvc) tienen punteros DVC; las ejecuciones nuevas se congelan explícitamente en el host, no desde Airflow.

## Reproducción histórica con DVC

La demostración `historical-20170801-store1-grocery-i` pronostica la serie tienda 1 / `GROCERY I` del **2017-08-01 al 2017-08-07**, con corte **2017-07-31 06:00 UTC**. Desde un clon de la revisión que incluye sus punteros, instala el proyecto y DVC como arriba. Configura la autenticación **solo local**; DVC pedirá tu token en la terminal:

```bash
dvc remote modify --local dagshub auth basic
dvc remote modify --local dagshub user TU_USUARIO_DAGSHUB
dvc remote modify --local dagshub ask_password true
dvc pull
dvc status
python -m scripts.verify_integrated_lineage
```

El verificador comprobó `forecast_rows: 7`, `selected_model: baseline`, `cutoff_at: 2017-07-31T06:00:00+00:00` e `integrated_git_commit: 3707bd575870f01475ebfe202fd52897c9518395`; `q50` fue **2368.285714** en los siete días archivados. La disponibilidad D+1/T0 y la publicación son simuladas. [Procedimiento, manifiesto y construcción desde los CSV](docs/dvc.md#reproducción-histórica-con-dvc).

### DVC de un período operativo de Airflow

Para la semana operativa **2017-07-25 a 2017-07-31**, existe una [instantánea DVC inmutable](data/reproductions/operational/weekly-20170725-20170724T060000Z__eval-20170726T060000Z.dvc) de entradas, preparación, modelo, pronósticos, pedidos y monitoreo. [Congelación, `dvc add`, `dvc push`, `dvc pull` y verificación](docs/dvc.md#dvc-de-un-período-operativo-de-airflow). El [resumen de evidencias](docs/evidencias_entrega_lab1.md) contiene la comparación de modelos, capturas de Airflow y seguimiento observado.
