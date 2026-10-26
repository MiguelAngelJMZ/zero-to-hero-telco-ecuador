# Workshop Databricks — de cero a héroe para telecomunicaciones

Workshop de 80 minutos sobre calidad de red, clientes y productos de un operador
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

## Los datos

Equinoccio Móvil es un operador móvil ecuatoriano ficticio. El workshop responde una
pregunta de negocio: **¿cómo se relaciona la calidad de la red con la experiencia y el
valor de los clientes?** Si una celda se degrada en Guayaquil, ¿cuántos clientes se
afectan, cuánto ingreso está en riesgo y quién podría darse de baja?

Los datos son sintéticos, sin PII, y corresponden a **agosto de 2026**: 7 días de
telemetría de red (1–7) y 14 días de consumo (1–14). Cubren 12 cantones de 9
provincias en las 4 regiones naturales (Costa, Sierra, Amazonía e Insular). Los
montos están en USD.

### Las 11 fuentes

| Dominio | Fuente | Qué representa | Filas |
|---|---|---|---:|
| Red | `network_sites` | Sitios (torres) con provincia, cantón, región natural y coordenadas | 60 |
| | `radio_cells` | Celdas 4G y 5G de cada sitio, con banda y capacidad | 180 |
| | `cell_tower_metrics` | Mediciones por celda: disponibilidad, señal, latencia y velocidad | 30.240 |
| Operación | `network_alarms` | Alarmas de red con severidad e impacto en el servicio | 1.200 |
| | `maintenance_orders` | Órdenes de trabajo con prioridad, costo y tiempo objetivo | 400 |
| Clientes | `products` | Planes prepago, pospago y empresa (USD 5–89,99 al mes) | 18 |
| | `customers` | Clientes seudónimos con segmento y provincia | 3.000 |
| | `subscriptions` | Líneas contratadas por cada cliente | 4.200 |
| | `usage_daily` | Consumo diario por línea: datos, voz, facturación y recargas | 58.800 |
| Experiencia | `support_tickets` | Reclamos de cobertura, velocidad, facturación y otros | 1.500 |
| | `customer_surveys` | Encuestas NPS y CSAT | 800 |

Los datos incluyen **impurezas intencionales** que el pipeline y DQX deben detectar:
tarifas negativas, sitios inexistentes, duplicados y porcentajes fuera de rango.

### De las fuentes a las métricas

| Capa | Objetos | Para qué |
|---|---|---|
| Bronze | 11 tablas | Copia fiel de los CSV, con trazabilidad del archivo de origen |
| Silver | 11 tablas | Datos limpios, deduplicados y enriquecidos (celda → sitio → provincia) |
| Calidad (DQX) | 6 `validated` + 6 `quarantine` | Registros confiables separados de los que requieren corrección |
| Gold | `gold_network_hourly` | Red por hora, sitio y tecnología |
| | `gold_site_daily_360` | Sitio por día: red, alarmas, tickets y mantenimiento |
| | `gold_customer_product_daily` | Línea por día: consumo, facturación, atención y experiencia de red |
| | `gold_customer_360` | Cliente: valor, experiencia y riesgo de baja |
| | `gold_incident_impact` | Alarma: clientes y líneas impactadas e ingreso en riesgo |
| Metric Views | `mv_network_quality` | KPIs de red, definidos una sola vez |
| | `mv_customer_product_experience` | KPIs de clientes, productos e ingresos |

### Definiciones clave

| Métrica | Definición |
|---|---|
| **Cumplimiento de calidad de red** | % de mediciones que cumplen **a la vez** disponibilidad ≥ 99 %, latencia ≤ 80 ms y downlink ≥ 20 Mbps. Mide calidad de servicio, **no** tiempo de reparación. |
| **Latencia** | RTT en ms: ida y vuelta de un paquete entre el dispositivo y un servidor de referencia. Sube con la congestión de la celda y con señal débil. |
| **Disponibilidad** | % del tiempo en que la celda estuvo en servicio. |
| **Downlink** | Velocidad de descarga en Mbps. |
| **Estado de salud del sitio** | Diario: `Crítico` si la calidad de red es < 55 %; `Atención` si es < 65 % o hay tickets críticos o más de 2 abiertos; `Saludable` en el resto. |
| **Riesgo de baja** | Banda Alto, Medio o Bajo por cliente, que combina la calidad de red experimentada, los tickets de red y el NPS. |
| **ARPU** | Ingreso facturado del período dividido por clientes únicos. |
| **NPS** (Net Promoter Score) | Lealtad según la pregunta "del 0 al 10, ¿recomendaría Equinoccio Móvil?": promotores (9–10), pasivos (7–8) y detractores (0–6). NPS = % promotores − % detractores, de −100 a +100. |
| **Ingreso en riesgo** | Ingreso facturado de las líneas afectadas por un incidente. |
| **Tiempo objetivo (SLA de atención)** | Horas para resolver tickets (4–72 h según severidad) y órdenes de mantenimiento (2–72 h según prioridad). |

## Inicio rápido (participantes)

1. En **Workspace**, abra su carpeta de usuario y pulse **Create → Git folder**.
   Pegue la URL de este repositorio y pulse **Create Git folder**.
2. Abra `notebooks/00_configuracion`, conecte compute **Serverless**, escriba su
   **catálogo** en el widget y ejecute todo el notebook.
3. Abra `notebooks/00b_preparar_todo` y pulse **Run all**. Lanza un job que prepara
   datos, pipeline, calidad, Gold y Metric Views (~15 min). Siga su avance con los enlaces.
4. Cuando la verificación de `00b` esté en `OK`, continúe con `07_dashboard`,
   `08_genie` y `09_app`.
5. Retos del taller y para después: [docs/RETOS.md](docs/RETOS.md).

Los notebooks `01` a `06` hacen lo mismo que `00b`, paso a paso. Úselos después del
taller para estudiar cada módulo en detalle.

| Notebook | Qué hace | Acción en la interfaz |
|---|---|---|
| `00_configuracion` | Valida permisos y servicios y guarda su configuración | — |
| `00b_preparar_todo` | Ejecuta `01` a `06` como un solo job (formato del taller) | Seguir el job y el DAG |
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
| Job del taller (`00b`) | `z2h - Preparar todo - ana_perez` |
| Job paso a paso (`02`) | `z2h - Preparar datos - ana_perez` |
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
