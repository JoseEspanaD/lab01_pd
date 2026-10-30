# Motor de Reposición de Inventario — Laboratorio 1

Proyecto de Product Development con datos públicos de Corporación Favorita. El objetivo es construir un flujo reproducible que, a partir de ventas y otras fuentes históricas, permita generar pronósticos y recomendaciones de reposición. La rama `feat/data-ingestion` implementa la primera parte: entregas simuladas y lectura de ventas según un corte temporal.

## Estructura mínima

```text
src/inventory/       Paquete Python: simulación, selección e ingesta de ventas
tests/               Pruebas pytest con datos sintéticos
data/raw/            Cinco CSV originales, asociados a archivos .dvc
data/inbox/          Entregas simuladas e inmutables; entrada de solo lectura para la ingesta
data/processed/      Salidas derivadas previstas
DECISIONES.md        Contrato de entregas y cortes históricos (DEC-001)
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

Para consultar la CLI y simular **una sola fecha de ventas** desde la raíz:

```bash
python -m inventory.train_delivery --help
python -m inventory.train_delivery 2017-08-15
```

La segunda orden lee `data/raw/train.csv` y publica, si hay registros para esa fecha, una entrega en `data/inbox/`. No sobrescribe una entrega existente: repetirla con los mismos datos devuelve la misma ruta. La disponibilidad simulada para ventas es D+1 a las 06:00 UTC. La CLI es un productor de la bandeja; la ingesta trata `data/inbox/` como entrada de solo lectura.

También están implementadas las funciones Python `select_deliveries(inbox_dir, cutoff_at)` e `ingest_sales(inbox_dir, cutoff_at)`. La primera selecciona rutas por disponibilidad, fecha y revisión; la segunda lee y valida únicamente las entregas seleccionadas de `train`. `cutoff_at` debe ser un `datetime` con zona UTC. Las correcciones se conservan como lotes completos e inmutables según [DEC-001](DECISIONES.md); todavía no hay una CLI que las publique.

Quedan pendientes los productores y la ingesta de `transactions`, `oil`, `holidays_events` y `stores`, la escritura en `data/processed/`, los pronósticos y las recomendaciones. Tampoco hay un flujo de Airflow operativo para esta ingesta.
