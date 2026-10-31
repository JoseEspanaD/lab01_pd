# Motor de Reposición de Inventario — Laboratorio 1

Proyecto de Product Development con datos públicos de Corporación Favorita. El objetivo es construir un flujo reproducible que, a partir de ventas y otras fuentes históricas, permita generar pronósticos y recomendaciones de reposición. La ingesta actual simula entregas y reconstruye cortes históricos para las cinco fuentes.

## Estructura mínima

```text
src/inventory/       Paquete Python: simulación, ingesta y preparación de datos
tests/               Pruebas pytest con datos sintéticos
data/raw/            Cinco CSV originales, asociados a archivos .dvc
data/inbox/          Entregas simuladas e inmutables; entrada de solo lectura para la ingesta
data/processed/      Salidas derivadas previstas
DECISIONES.md        Contratos de entregas y preparación histórica (DEC-001/002)
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

La disponibilidad D+1 y T0 son supuestos, no fechas reales de publicación. El catálogo de `stores` se trata como estable desde T0 y los feriados futuros no se adelantan; consulta las limitaciones de [DEC-001 y DEC-002](DECISIONES.md) antes de usar estos datos en predicciones. Aún no hay persistencia en `data/processed/`, entrenamiento, pronósticos, recomendaciones ni un flujo de Airflow operativo.
