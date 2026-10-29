# Instrucciones para Codex

- Nunca modificar directamente la rama `main`.
- No realizar commits, pushes ni abrir Pull Requests sin autorización explícita.
- No instalar dependencias sin autorización.
- Mantener la lógica de negocio en módulos Python dentro de `src/inventory/`.
- Mantener los DAGs de Airflow enfocados exclusivamente en orquestación.
- Utilizar `pytest` para las pruebas automatizadas.
- Evitar almacenar datasets, modelos y resultados generados directamente en Git.
- Utilizar DVC para versionar los artefactos correspondientes cuando se configure.
- Tratar `data/inbox/` como una bandeja externa de solo lectura durante la ejecución del producto.
- Evitar data leakage: cada predicción debe utilizar únicamente información disponible en su fecha de corte.
- Diseñar operaciones idempotentes cuando corresponda.
- Mantener el código modular, legible y sencillo.
- Documentar decisiones técnicas relevantes en `DECISIONES.md`.
- Antes de modificar archivos existentes, revisar su contenido.
- No realizar cambios fuera del alcance de la tarea solicitada.
