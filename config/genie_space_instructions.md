# Instrucciones del Genie Agent

Nombre: `Analista 360 de Red y Clientes`.

## Fuentes

Fuentes semánticas primarias:

- `mv_network_quality`
- `mv_customer_product_experience`

Fuentes Gold de detalle:

- `gold_customer_360`
- `gold_incident_impact`
- `gold_site_daily_360`

El script de creación sustituye dinámicamente `<catalog>` y `<schema>`. Este
archivo no debe contener nombres de workspace, warehouse, usuario ni credenciales.

## Cómo está curado el espacio

La definición vive en `src/telco_workshop/genie.py` y sigue las buenas prácticas de
Databricks: el significado de los datos va en superficies estructuradas y las
instrucciones de texto solo describen el comportamiento esencial.

| Superficie | Contenido |
|---|---|
| Descripción de cada fuente | Para qué sirve y su granularidad; Metric Views para KPIs, Gold para detalle |
| Sinónimos por columna | Términos de negocio: calidad de red (también "SLA"), velocidad, provincia, churn, reclamos, FCR, líneas |
| Medidas | Definidas una sola vez en las Metric Views (módulo 06) |
| SQL de ejemplo | Cuatro patrones: KPIs de red, KPIs de clientes, incidentes y sitios |
| Benchmarks | Las dos preguntas de Chat, con SQL verificado |
| Instrucciones de texto | `PURPOSE`, `DISAMBIGUATION`, `DATA QUALITY NOTES`, `CONSTRAINTS` e `Instructions you must follow when providing summaries` (menos de 2.000 caracteres) |

Las reglas que agrega el participante en la sección 4 de `notebooks/08_genie` se
insertan en `DISAMBIGUATION`, antes del bloque de resúmenes, que debe ir al final.

## Dos preguntas para modo Chat

1. Compara disponibilidad, latencia, throughput y cumplimiento de calidad de red por provincia
   y tecnología durante los siete días de telemetría. Muéstrame las tres
   combinaciones con peor desempeño.
2. ¿Qué cinco productos tienen mayor ARPU y cómo se comparan en consumo de datos,
   tickets por cada 100 líneas activas, NPS y cumplimiento de calidad de red?

## Dos preguntas para modo Deep Research

1. Investiga las provincias cuyo cumplimiento de calidad de red esté por debajo de
   95%. Cruza calidad de celdas, alarmas, órdenes de mantenimiento, tickets,
   líneas afectadas, consumo y NPS. Identifica patrones y causas probables,
   cuantifica el impacto por segmento y producto, diferencia correlación de
   causalidad y propone cinco acciones priorizadas con evidencia.
2. Evalúa conjuntamente riesgo de baja y oportunidad de migración de plan. Busca
   segmentos con consumo cercano o superior a la franquicia, mala experiencia de
   red, tickets repetidos o NPS bajo; compara rentabilidad y experiencia entre
   productos, provincias y regiones naturales; estima el universo afectado y recomienda acciones de
   red, atención y portafolio. Incluye metodología, supuestos, limitaciones y
   análisis de sensibilidad.

## Nota sobre Deep Research

La documentación pública valida Genie Agents y su API, pero no publica un campo
de configuración de API llamado `Deep Research`. Verifique la disponibilidad y el
nombre del modo en la interfaz del workspace. Si no estuviera habilitado, ejecute
las dos preguntas como investigación guiada de varios pasos en Chat y explique
esta contingencia; no presente el fallback como un modo Deep Research nativo.
