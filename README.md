# Workshop Databricks — de cero a héroe para telecomunicaciones

Workshop de 180 minutos sobre calidad de red, clientes y productos de un operador
móvil ficticio. Los datos son sintéticos y anónimos, con características geográficas
y operativas plausibles para Ecuador. El operador ficticio se llama **Equinoccio Móvil**.

Todo se ejecuta **dentro del workspace**, desde notebooks. No se necesita CLI, tokens
ni instalación local: cada notebook usa el SDK de Databricks para Python con la
autenticación del propio notebook y crea los assets **del participante**.

## Recorrido

```text
Job (PySpark) → 11 CSV en UC Volume → Lakeflow: 11 Bronze → 11 Silver
              → DQX validated/quarantine → 5 Gold → 2 Metric Views
              → AI/BI Dashboard + Genie Space + Databricks App
```

## Inicio rápido (participantes)

1. En **Workspace**, abra su carpeta de usuario y pulse **Create → Git folder**.
   Pegue la URL de este repositorio y pulse **Create Git folder**.
2. Abra `notebooks/00_configuracion`, conecte compute **Serverless**, escriba su
   **catálogo** en el widget y ejecute todo el notebook.
3. Ejecute los notebooks en orden. Cada uno muestra un enlace al asset que crea.
4. Al final de cada módulo, resuelva su reto en [docs/RETOS.md](docs/RETOS.md).

| Notebook | Qué hace | Acción en la interfaz |
|---|---|---|
| `00_configuracion` | Valida permisos y servicios y guarda su configuración | — |
| `01_unity_catalog` | Crea su schema y el volume `raw_data` | Revisar Catalog Explorer |
| `02_datos` | Crea su **job** de generación de datos y valida los CSV | **Run now** en el job |
| `03_pipeline` | Crea su **pipeline** Lakeflow (no lo ejecuta) | **Run pipeline** y revisar el DAG |
| `04_dqx_quality` | Separa registros válidos y cuarentena con DQX | — |
| `05_sql_queries` | Crea las cinco vistas Gold | — |
| `06_metric_views` | Crea las dos Metric Views | — |
| `07_dashboard` | Prompt de Genie Code y dashboard de referencia | Construir el dashboard |
| `08_genie` | Crea y mejora su Genie Space | Preguntas en modo Chat |
| `09_app` | Crea y despliega su Databricks App | Abrir y modificar la App |
| `99_limpieza` | Elimina todos sus assets (escriba `BORRAR`) | — |

Use compute **Serverless** en todos los notebooks, incluidos los SQL (`05`, `06`):
cargan la configuración con `%run ./_config`, que no funciona en un SQL Warehouse.

## Qué crea cada participante

A partir del usuario (`ana.perez@empresa.com` → `ana_perez`):

| Asset | Nombre |
|---|---|
| Schema | `<catálogo>.z2h_ana_perez` (con volume `raw_data`) |
| Job | `z2h - Preparar datos - ana_perez` |
| Pipeline | `z2h - Calidad de red - ana_perez` |
| Dashboard de referencia | `Experiencia móvil 360 (referencia) - ana_perez` |
| Genie Space | `Analista 360 de Red y Clientes - ana_perez` |
| App | `z2h-ana-perez` |

Los notebooks son idempotentes: si se vuelven a ejecutar, actualizan el asset en
lugar de duplicarlo. La configuración se guarda en
`/Workspace/Users/<usuario>/.z2h/config.json`.

## Requisitos por workspace

- Unity Catalog y un catálogo con `USE CATALOG` y `CREATE SCHEMA` para cada participante.
- SQL Warehouse **PRO o Serverless** con `CAN USE` (Genie no admite Classic).
- Notebooks, jobs y pipelines serverless; AI/BI Dashboards, Genie, Metric Views y
  Databricks Apps habilitados. Máximo 100 Apps por workspace.
- Git folders sin restricción de URL para este repositorio (o con la URL en la lista permitida).
- Salida a PyPI: los notebooks actualizan `databricks-sdk` e instalan DQX.

El administrador de su workspace valida estos requisitos antes de la sesión.

## Estructura

- `notebooks/`: módulos del workshop, `_config` compartido y `tareas/generar_datos` (tarea del job).
- `pipeline/transformations/`: código fuente del pipeline Lakeflow.
- `src/telco_workshop/`: generador PySpark, helpers del SDK (`workshop.py`) y
  definiciones del dashboard y del Genie Space.
- `apps/network-monitor/`: App Streamlit inicial y reto de modernización.
- `config/`: instrucciones y preguntas de referencia del Genie Space.
- `docs/`: retos por módulo y guía del dashboard.
- `pyproject.toml`: marca la raíz del repositorio para los notebooks; no lo elimine.

## Contingencias

- `05_checkpoint_if_needed.sql`: continúa desde las tablas `*_raw_checkpoint` que crea
  el job si Lakeflow no está disponible.
- `04_alt_sql_quality.sql`: reglas de calidad en SQL si DQX no se instala.
- `03_alt_spark_etl.py`: comparación didáctica con PySpark imperativo. Sus tres tablas
  `demo_spark_*` no alimentan ninguna etapa posterior.

## Guías

- [Retos por módulo](docs/RETOS.md)
- [Construcción del AI/BI Dashboard](docs/DASHBOARD_GUIDE.md)
- [App inicial y reto de modernización](apps/network-monitor/README.md)
- [Política de seguridad del repositorio](SECURITY.md)
