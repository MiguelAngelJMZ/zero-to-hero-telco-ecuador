# AI/BI Dashboard — guía de construcción

Nombre sugerido: `Experiencia móvil 360 — <participante>`.

El dashboard es la principal construcción visual del workshop. Se crea desde la
interfaz para practicar datasets, canvas, filtros y publicación. Use el catálogo y
el schema asignados al participante (`<catálogo>.z2h_<usuario>`); no copie nombres
fijos de otro entorno.

`notebooks/07_dashboard` ofrece dos apoyos:

- **Genie Code:** imprime un prompt de alto nivel, con su catálogo y schema, que
  describe las preguntas de negocio sin SQL ni diseño de widgets. Créelo en
  **+ New → Dashboard**, abra el asistente y pegue el prompt. Lea el plan que propone,
  revise el SQL de cada dataset e itere con las preguntas de seguimiento.
- **Dashboard de referencia:** lo crea y publica con el SDK como contingencia o como
  punto de comparación.

## Antes de comenzar

Compruebe que `05_sql_queries.sql` y `06_metric_views.sql` finalizaron. Deben existir:

- `gold_network_hourly`
- `gold_site_daily_360`
- `gold_customer_product_daily`
- `gold_customer_360`
- `gold_incident_impact`
- `mv_network_quality`
- `mv_customer_product_experience`

## Datasets

### 1. Calidad de red por provincia y tecnología

```sql
SELECT
  `Provincia` AS region,
  `Tecnología` AS technology,
  MEASURE(`Sitios activos`) AS active_sites,
  MEASURE(`Disponibilidad media`) AS availability_pct,
  MEASURE(`Latencia media`) AS latency_ms,
  MEASURE(`Downlink medio`) AS downlink_mbps,
  MEASURE(`Cumplimiento de calidad de red`) AS network_sla_pct
FROM <catalog>.<schema>.mv_network_quality
GROUP BY `Provincia`, `Tecnología`
```

### 2. Experiencia de clientes y productos

```sql
SELECT
  `Provincia` AS region,
  `Segmento` AS customer_segment,
  `Producto` AS product_name,
  MEASURE(`Clientes activos`) AS active_customers,
  MEASURE(`ARPU`) AS arpu_usd,
  MEASURE(`Tickets por 100 líneas`) AS tickets_per_100_lines,
  MEASURE(`NPS`) AS nps,
  MEASURE(`Clientes de alto riesgo`) AS high_risk_customers,
  MEASURE(`Cumplimiento de calidad del cliente`) AS customer_network_sla_pct
FROM <catalog>.<schema>.mv_customer_product_experience
GROUP BY `Provincia`, `Segmento`, `Producto`
```

### 3. Sitios prioritarios

```sql
SELECT
  event_date,
  region,
  canton,
  site_id,
  site_health_status,
  avg_availability_pct,
  avg_latency_ms,
  avg_downlink_mbps,
  open_tickets,
  impacted_subscriptions
FROM <catalog>.<schema>.gold_site_daily_360
```

### 4. Impacto de incidentes

```sql
SELECT
  opened_date,
  region,
  canton,
  severity,
  business_impact,
  impacted_customers,
  impacted_subscriptions,
  billed_revenue_at_risk_usd
FROM <catalog>.<schema>.gold_incident_impact
```

Reemplace `<catalog>` y `<schema>` desde el selector del editor. No publique el
nombre del entorno de validación en capturas o documentación compartida.

## Canvas mínimo del ejercicio

1. KPI: disponibilidad media.
2. KPI: latencia media.
3. KPI: clientes activos.
4. Barras: cumplimiento de calidad de red por provincia, color por tecnología.
5. Dispersión: ARPU frente a NPS, tamaño por clientes activos y color por segmento.
6. Tabla: sitios, estado, latencia, tickets y suscripciones impactadas.
7. Filtros: provincia, tecnología y segmento.

## Verificación

- Cambiar la provincia actualiza KPI, gráficos y tabla aplicables.
- Las medidas de las Metric Views coinciden con una consulta ejecutada en SQL Editor.
- El dashboard no consulta Bronze, CSV ni datos de cuarentena.
- La publicación no revela usuario, workspace, catálogo de validación ni IDs internos.

## Actividad mínima viable

Si quedan menos de ocho minutos, el instructor duplica un dashboard de referencia.
Cada participante añade un filtro y una visualización. Si AI/BI Dashboards no está
disponible, ejecute los cuatro datasets en SQL Editor y use las capturas sanitizadas.

La captura del dashboard de referencia se añadirá únicamente después de validar el
canvas en el workspace y comprobar que no contiene IDs ni nombres del entorno.
