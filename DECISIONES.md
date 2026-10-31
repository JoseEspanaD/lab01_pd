# Registro de decisiones

Registrar aquí las decisiones técnicas y de producto cuando se acuerden. Crear una entrada por decisión con la siguiente plantilla:

## [ID] Título de la decisión

- **Identificador:** [ID]
- **Fecha:** [AAAA-MM-DD]
- **Contexto:** [Problema, restricciones y necesidades relevantes]
- **Alternativas consideradas:** [Opciones evaluadas]
- **Decisión adoptada:** [Opción elegida]
- **Justificación:** [Motivos de la elección]
- **Consecuencias:** [Efectos esperados, compromisos y tareas derivadas]

## DEC-001 Entregas simuladas e ingesta histórica

- **Identificador:** DEC-001
- **Fecha:** 2026-10-30
- **Contexto:** Los cinco CSV de `data/raw/` contienen fechas de observación o evento, pero no fechas reales de entrega ni un historial de correcciones. La ingesta debe permitir reproducir qué datos estaban disponibles en un corte histórico sin escribir en `data/inbox/` durante la ejecución del producto.
- **Alternativas consideradas:** Un manifiesto central junto a archivos inmutables permitiría ampliar los metadatos, pero exigiría mantener ambos sincronizados. Sobrescribir el archivo de cada fecha impediría reconstruir cortes anteriores. Aplicar correcciones como cambios por fila complicaría la selección. Se descartan estas alternativas para el laboratorio.
- **Decisión adoptada:**
  - `data/raw/` conserva los cinco CSV originales, versionados mediante DVC; no representa entregas diarias. `data/inbox/` contiene entregas simuladas e inmutables, preparadas por un proceso externo antes de la ingesta; durante la ejecución del producto es una entrada de solo lectura. `data/processed/` contiene resultados derivados y reproducibles para el corte solicitado. Los datos generados no se almacenan directamente en Git.
  - Cada entrega contiene el lote completo de una fuente y una fecha de observación. Su ruta es `data/inbox/source=<fuente>/observation_date=<AAAA-MM-DD>/available_at=<AAAA-MM-DDTHHMMSSZ>/revision=<NNNN>/delivery_id=<id>/data.csv`. `available_at` es un instante UTC; `revision` es un entero positivo que aumenta para cada par fuente y fecha de observación. La ruta y su contenido no se modifican tras publicar la entrega. Se rechazan identificadores de entrega duplicados, incluso si el contenido coincide, y revisiones duplicadas para el mismo par fuente y fecha de observación.
  - Para `train`, `transactions`, `oil` y `holidays_events`, la entrega ordinaria de la fecha D está disponible en D+1 a las 06:00 UTC. Solo se generan entregas para fechas con registros. En `holidays_events`, todas las filas de D forman un lote; los eventos de fechas futuras no se consideran disponibles anticipadamente.
  - `stores` se entrega una vez como catálogo inicial. Dado que no tiene fechas, se le asigna `observation_date=2013-01-01` y disponibilidad simulada T0 = `2013-01-01T00:00:00Z`. Solo puede utilizarse en cortes desde T0.
  - Una corrección se publica como una revisión nueva, completa e inmutable del lote de su fuente y fecha de observación; conserva la entrega anterior. Para un instante de corte `cutoff_at`, se consideran únicamente entregas con `available_at <= cutoff_at`. Para cada par fuente y fecha de observación se elige la de mayor `available_at` y, si hay empate, la de mayor `revision`. Una corrección posterior no altera el resultado de un corte anterior.
  - `observation_date` identifica el día al que corresponden los datos: fecha de ventas, transacciones, precio o evento; para `stores` es la fecha simulada del catálogo. `cutoff_at` identifica el instante UTC hasta el que se permite conocer entregas. Por ejemplo, las ventas de D no están disponibles en D+1 a las 05:59 UTC, pero sí a las 06:00 UTC.
- **Justificación:** Los metadatos en rutas permiten seleccionar lotes y reconstruir cortes sin un catálogo adicional. Reemplazar lotes completos mediante revisiones simplifica la ingesta idempotente y conserva las correcciones.
- **Consecuencias:** D+1 a las 06:00 UTC y T0 son supuestos de simulación, no fechas observadas de publicación. Los CSV no permiten determinar cuándo se anunciaron feriados o traslados ni cuándo cambiaron los atributos de las tiendas; en particular, `transferred`, `type` y `cluster` podrían reflejar información conocida más tarde. Tratar el catálogo de `stores` como disponible y estable desde T0 puede introducir información futura en cortes antiguos, por lo que ese supuesto debe identificarse al usarlo en predicciones históricas. Esta política tampoco permite anticipar feriados futuros. Las predicciones solo deben usar entregas disponibles en su `cutoff_at`.

## DEC-002 Preparación puntual de ejemplos de entrenamiento

- **Identificador:** DEC-002
- **Fecha:** 2026-10-31
- **Contexto:** Se necesitan ejemplos supervisados de las cinco fuentes sin mezclar el momento en que se predice con el momento posterior en que se conoce la etiqueta.
- **Alternativas consideradas:** Unir todas las fuentes en un único corte de entrenamiento introduciría correcciones y observaciones futuras en ejemplos antiguos. Usar ventas del día anterior a la fecha objetivo también filtraría información: con el corte elegido esas ventas aún no han llegado. Se pospone la generación de series nuevas sin historial.
- **Decisión adoptada:**
  - La función `prepare_training_data` recibe un intervalo inclusivo de fechas objetivo y un `cutoff_at` UTC de preparación. Genera filas en memoria, ordenadas por fecha, tienda y familia, sin escribir en `data/inbox/` ni `data/processed/`.
  - Para fecha objetivo `D`, el horizonte es un día y el corte de predicción se fija en `D−1` a las 06:00 UTC. La preparación reutiliza los contratos de selección y los analizadores de las cinco fuentes: recorre una vez los metadatos, avanza los lotes vigentes en orden de disponibilidad y mantiene índices por fecha, tienda y familia. Lee cada lote que llega a ser seleccionado en un corte de predicción una sola vez; las revisiones reemplazan lotes completos. Las ventas de `D`, leídas por separado al `cutoff_at` de preparación, sirven solo como etiqueta si ya están disponibles. Las revisiones posteriores al corte de predicción no cambian las variables de la fila; las correcciones de la etiqueta sí pueden cambiar la etiqueta en preparaciones posteriores.
  - Se generan filas solo para pares tienda–familia con al menos una venta visible antes del corte de predicción y con etiqueta visible en el corte de preparación. La última venta y promoción provienen del par; transacciones de la tienda; petróleo de la fecha; atributos del catálogo de la tienda. Los eventos nacionales se aplican a todas las tiendas, los regionales por `state` y los locales por `city`. Sin catálogo visible solo se pueden aplicar eventos nacionales.
  - Las variables históricas usan la última observación visible y su edad, una media de ventas observadas en `D−8` a `D−2` y el conteo de todos los registros de eventos visibles en esa ventana que corresponden a la tienda. Las observaciones ausentes y las cotizaciones vacías se dejan como `None`, sin imputación; el conteo sin eventos conocidos es cero. Se incluyen el día de semana y el mes de la fecha objetivo, que son datos de calendario conocidos antes del corte. `label_sales` se mantiene fuera de `Features`.
- **Justificación:** Los cortes por fila reproducen la información disponible al predecir, mientras que el corte de preparación controla la disponibilidad de la etiqueta. Los tipos inmutables y el orden estable permiten obtener resultados reproducibles sin almacenamiento adicional.
- **Consecuencias:** No se producen ejemplos para series nuevas ni etiquetas aún no entregadas. El conteo de eventos describe registros históricos conocidos, incluidos los marcados `transferred`, y no pretende representar el calendario futuro. Se conserva el supuesto de DEC-001 sobre el catálogo de tiendas: sus atributos podrían contener información futura que los CSV originales no permiten fechar con precisión. La preparación no crea modelos ni artefactos persistentes.
