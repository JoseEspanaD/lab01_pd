# Evidencia: reproducción desde cinco CSV en un clon limpio

**Fecha:** 2026-11-01. **Código:** `origin/main` en `e2ffe0e3aedc08df8b89249d6d47b0e9ce962d7d`. El clon aislado estuvo en `/tmp/lab1-cleanroom-20261101` y usó el proyecto Compose `lab1clean20261101`, distinto del principal. Se copiaron únicamente `train.csv`, `transactions.csv`, `oil.csv`, `holidays_events.csv` y `stores.csv` desde la ruta local; sus SHA-256 coincidieron antes y después. No se copiaron `.env`, cachés, SQLite, bandejas ni resultados operativos, y no se ejecutó `dvc pull`. Los objetos de la demostración histórica DVC no estaban presentes.

## Comandos esenciales ejecutados

```bash
git clone --branch main --single-branch --no-tags https://github.com/JoseEspanaD/lab01_pd.git /tmp/lab1-cleanroom-20261101
python3 -m venv .venv
.venv/bin/python -m pip install -e .
.venv/bin/python -m pip install -r requirements-dev.txt
.venv/bin/python -m pip install dvc
.venv/bin/python -m pytest -q
python3 scripts/init_airflow_env.py
docker compose -p lab1clean20261101 config --quiet
docker compose -p lab1clean20261101 up -d
docker compose -p lab1clean20261101 run --rm inventory-producer --observation-date 2017-05-27 --cutoff-at 2017-05-28T06:00:00Z
```

Las otras **57 fechas**, del 2017-05-28 al 2017-07-23, se produjeron en un solo contenedor `inventory-producer`, llamando a `produce_for_date` una vez por fecha con corte D+1 06:00 UTC. La [receta del README](../README.md) contiene el bucle y la configuración JSON íntegra. Se disparó dos veces `inventory_operational_flow` con `mode=forecast`, objetivo **2017-07-25 a 2017-07-31**, corte de decisión **2017-07-24 06:00 UTC** y `kind=historical_simulation`. Después se disparó `mode=refresh` con observación `train` del 2017-07-25 y evaluación **2017-07-26 06:00 UTC**. Los tres runs terminaron en `success`. Al finalizar se ejecutó `docker compose -p lab1clean20261101 down`; no quedaron proyectos Compose activos.

## Resultados observados

| Comprobación | Resultado |
| --- | --- |
| `pytest` en el clon | **191 passed** en 33.68 s |
| Airflow | Compose válido, endpoint de salud correcto, sin errores de importación de DAGs |
| Productor | **58 fechas**, **161 entregas**; primera fecha 17.56 s y restantes 1001.55 s |
| Primer run integrado | `success` en unos 47 s; **89 100 filas preparadas**, **12 474 pronósticos** y **1 782 pedidos**, modelo seleccionado `baseline` |
| Monitoreo inicial | 0 pares evaluados y **12 474 pendientes**; métricas nulas |
| Repetición idéntica | `success` en unos 39 s; SHA-256 y tiempos de modificación de los cinco artefactos operativos idénticos |
| `refresh` | `success` en unos 28 s; reporte nuevo con **1 782 evaluados** y **10 692 pendientes**, MAE 93.77, RMSE 312.30, sesgo −59.68 y cobertura 0.8311 |

La producción recorre `train.csv` completo por cada fecha, por lo que el backfill fue el tramo más lento. Esta ejecución **no probó una corrección de contenido**: los cinco CSV originales no incluyen revisiones y fabricar una habría añadido otro dato de entrada. Las disponibilidades históricas D+1 06:00 UTC y T0 son **supuestos de simulación**, no fechas reales de entrega acreditadas por Kaggle. Las publicaciones y el seguimiento son reconstrucciones históricas, no publicaciones reales de 2017.
