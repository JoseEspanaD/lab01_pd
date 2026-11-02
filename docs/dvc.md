# DVC: datos originales y reproducción

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

Los otros archivos de la descarga, como `test.csv` y `sample_submission.csv`, no forman parte de los cinco datasets versionados aquí. Cada archivo `.csv.dvc` registra la ruta y la huella del CSV esperado. Si se obtienen versiones o contenidos distintos, no coincidirán con esos metadatos. La demostración histórica descrita [a continuación](#reproducción-histórica-con-dvc) añade punteros DVC para la instantánea, las particiones, el modelo, la publicación y el manifiesto; otros artefactos operativos no quedan versionados automáticamente.

## Reproducción histórica con DVC

El escenario `historical-20170801-store1-grocery-i` reconstruye **una simulación histórica**, no una publicación real de 2017. Pronostica los siete días del 1 al 7 de agosto de 2017 para tienda 1 y familia `GROCERY I`, con corte **2017-07-31 06:00 UTC**. Usa únicamente esa serie de `train.csv`, las fuentes auxiliares entre el 27 de mayo y el 30 de julio, y el catálogo de tiendas. Las entregas de fuentes fechadas se simulan en **D+1 a las 06:00 UTC**; el catálogo, en **T0 = 2013-01-01 00:00 UTC**. Estas son disponibilidades supuestas: Kaggle no proporciona las fechas reales de entrega ni permite acreditar históricamente la vigencia de todos los atributos de tienda.

La evaluación usa cuatro semanas de origen, con una semana de embargo entre ellas: entrenamiento 2017-06-04, calibración 2017-06-18, selección 2017-07-02 y prueba 2017-07-16. Las etiquetas de prueba se leen al corte de reporte **2017-07-24 06:00 UTC**; la selección del modelo y su calibración se fijan antes del corte del pronóstico. `data/reproductions/<id>/inbox` contiene los lotes simulados e inmutables; `processed.json` contiene las filas exactas de las cuatro particiones; `models/<id>.json` contiene el estado numérico del modelo y los residuos diarios en JSON, sin pickle; `data/published/forecasts/<id>.json` es la publicación inmutable con `kind=historical_simulation` y `published_at=null`. `manifest.json` une hashes de esos contenidos, punteros DVC, cinco CSV originales, configuración, parámetros, corte y revisión Git del código. El manifiesto no se referencia a sí mismo.

Desde un **clon limpio de la revisión que contenga los punteros del escenario**, instala el proyecto y DVC como se indica en el [README](../README.md#instalación-y-ejecución). Configura `auth=basic`, tu usuario y `ask_password=true` con los comandos de «Datos originales»; DVC solicitará tu **propio token** de DagsHub en la terminal. Nunca lo pegues en el chat, en un comando ni en un archivo versionado. Después ejecuta:

```bash
dvc pull
dvc status
python -m scripts.verify_integrated_lineage
```

`dvc pull` recupera los cinco CSV y los cinco artefactos de la demostración desde DagsHub. El manifiesto DVC original conserva el SHA del commit de la rama `10d0db2`; GitHub integró ese contenido mediante Squash and merge en `3707bd5`. La referencia adicional `lineage/historical-20170801-store1-grocery-i.json` vincula el manifiesto original por su SHA-256 al commit integrado, sin alterar el artefacto histórico. El nuevo verificador exige que `3707bd5` sea antecesor del checkout y que **cada archivo de código** de ese commit y del espacio de trabajo coincida con las huellas del manifiesto. Si el commit original de la rama está disponible, también comprueba su contenido. Después ejecuta todas las verificaciones existentes de punteros, lotes, particiones, modelo, calibración y siete predicciones. El resultado esperado es `forecast_rows: 7`, `cutoff_at: 2017-07-31T06:00:00+00:00`, `selected_model: baseline` e `integrated_git_commit: 3707bd575870f01475ebfe202fd52897c9518395`. En los siete días archivados, `q50` es **2368.285714** al redondear a seis decimales. No reescribe la publicación. Si un objeto no se recupera, consulta el acceso a DagsHub de tu cuenta; la lectura anónima con `dvc pull` no se ha verificado y devolvió HTTP 401 en la prueba previa.

Para construir localmente la demostración desde los cinco CSV originales de Kaggle que coincidan con sus `.dvc`, ejecuta `python -m inventory.historical_reproduction build`. El comando procesa `train.csv` en una sola pasada y escribe la instantánea en `data/reproductions/`, fuera de la bandeja operativa `data/inbox/`; repetirlo con las mismas entradas es idempotente, mientras una diferencia se rechaza. Para registrar una ejecución nueva con DVC, aplica `dvc add` a `inbox`, `processed.json`, el JSON del modelo y el JSON de la publicación, en ese orden. Cuando exista el commit de código, fija su SHA mediante `python -m inventory.historical_reproduction manifest --source-git-commit SHA_COMMIT_CODIGO` y ejecuta `dvc add data/reproductions/historical-20170801-store1-grocery-i/manifest.json`. Un borrador local sin SHA se puede finalizar una sola vez si sus demás referencias no cambiaron; un manifiesto ya finalizado no se sustituye. Si el PR se integra con Squash and merge, el SHA de la rama deja de ser antecesor de `main`: conserva ese manifiesto y añade una referencia pequeña al commit integrado que compruebe el hash del manifiesto y el contenido del código, como hace esta demostración. Esta referencia se registra **después** del merge, evitando que su propio SHA dependa del commit que la contiene. Ningún comando de reproducción requiere Airflow.

## DVC de un período operativo de Airflow

El flujo de pronóstico guarda ahora `data/published/model_states/<publication_id>.json` de forma inmutable. Contiene el estado del modelo **seleccionado** por la regla existente, los residuos de calibración diaria y semanal, métricas y cortes de disponibilidad. Sus huellas se comprueban contra el pronóstico y los pedidos publicados. No modifica la selección baseline/Ridge. Para congelar un período deben haber terminado el run de pronóstico y el `refresh` que produjo el reporte de monitoreo deseado. Por ejemplo, después de la [receta desde cinco CSV](airflow.md#semana-histórica-nueva-desde-los-cinco-csv-originales), con evaluación del **2017-07-26 06:00 UTC**:

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

Desde un clon limpio de la revisión que incluya ese puntero, instala el paquete y DVC como se indica en el [README](../README.md#instalación-y-ejecución). Usa tu propia cuenta de DagsHub; el token se solicita de forma interactiva y no se guarda en Git. Para recuperar y comprobar el mismo período:

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

El verificador vuelve a calcular la evaluación temporal, los pronósticos, pedidos y monitoreo desde la instantánea, sin reescribir una publicación. Exige el mismo código y configuración registrados en el manifiesto; para una versión posterior del repositorio usa el commit o tag que incorporó el puntero. El remoto DVC actual requiere autenticación para lectura. El archivo operativo solo queda recuperable desde DagsHub después de que su objeto se haya enviado con `dvc push` y su puntero forme parte de la revisión clonada. Véase [DEC-019](../DECISIONES.md).
