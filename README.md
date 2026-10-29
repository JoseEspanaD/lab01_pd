Repositorio / Laboratorio 01 Product Development

# Motor de Reposición de Inventario

Proyecto del Laboratorio 1 de Product Development de la Maestría en Data Science. Usará datos públicos de Corporación Favorita para generar pronósticos de ventas y recomendaciones de inventario mediante un producto de Machine Learning reproducible y automatizado.

## Objetivo general

Construir un flujo reproducible que transforme datos de ventas en pronósticos y recomendaciones de reposición de inventario.

## Tecnologías previstas

Python, Git y GitHub, DVC, Apache Airflow 3, Docker Compose, pytest y GitHub Actions.

## Estructura inicial

```text
.github/workflows/  Automatización futura de GitHub Actions
airflow/dags/       DAGs de orquestación futuros
config/             Configuración versionable
data/raw/           Datos originales
data/inbox/         Entrada externa de solo lectura durante la ejecución
data/processed/     Datos transformados
data/published/     Datos y pronósticos publicados
models/             Modelos entrenados
reports/            Resultados e informes generados
src/inventory/      Paquete Python para la lógica de negocio
tests/              Pruebas automatizadas
```

## Estado actual

La estructura y los archivos de configuración iniciales están preparados. Aún no hay funcionalidades, pipelines, datos descargados ni modelos entrenados.
