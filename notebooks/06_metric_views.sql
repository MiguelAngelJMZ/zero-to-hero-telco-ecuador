-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 06 — Metric Views de Unity Catalog
-- MAGIC
-- MAGIC Define las métricas una sola vez y permite agruparlas por cualquier dimensión
-- MAGIC disponible. Dashboard y Genie consumen estos objetos gobernados. En SQL, cada
-- MAGIC medida se consulta con `MEASURE()`.

-- COMMAND ----------

-- MAGIC %run ./_config

-- COMMAND ----------

CREATE OR REPLACE VIEW mv_network_quality WITH METRICS LANGUAGE YAML AS
$$
  version: 1.1
  comment: "Capa semántica gobernada para calidad y operación de red móvil"
  source: gold_network_hourly

  fields:
    - name: Hora
      expr: source.event_hour
      comment: "Hora de la medición"
    - name: Fecha
      expr: source.event_date
      comment: "Fecha de la medición"
    - name: Provincia
      expr: source.region
    - name: Cantón
      expr: source.canton
    - name: Sitio
      expr: source.site_id
    - name: Tecnología
      expr: source.technology
    - name: Banda
      expr: source.frequency_band
    - name: Entorno
      expr: source.environment
    - name: Región natural
      expr: source.natural_region
      comment: "Costa, Sierra, Amazonía o Insular"
    - name: Estado de salud
      expr: source.health_status

  measures:
    - name: Sitios activos
      expr: COUNT(DISTINCT source.site_id)
      comment: "Cantidad de sitios con mediciones"
    - name: Celdas activas
      expr: SUM(source.active_cells)
    - name: Mediciones
      expr: SUM(source.measurement_count)
    - name: Disponibilidad media
      expr: SUM(source.avg_availability_pct * source.measurement_count) / SUM(source.measurement_count)
      comment: "Disponibilidad ponderada por cantidad de mediciones"
    - name: Señal media
      expr: SUM(source.avg_signal_dbm * source.measurement_count) / SUM(source.measurement_count)
    - name: SINR medio
      expr: SUM(source.avg_sinr_db * source.measurement_count) / SUM(source.measurement_count)
    - name: Latencia media
      expr: SUM(source.avg_latency_ms * source.measurement_count) / SUM(source.measurement_count)
    - name: Downlink medio
      expr: SUM(source.avg_downlink_mbps * source.measurement_count) / SUM(source.measurement_count)
    - name: Uplink medio
      expr: SUM(source.avg_uplink_mbps * source.measurement_count) / SUM(source.measurement_count)
    - name: Pérdida de paquetes media
      expr: SUM(source.avg_packet_loss_pct * source.measurement_count) / SUM(source.measurement_count)
    - name: Llamadas caídas
      expr: SUM(source.dropped_calls)
    - name: Tráfico de datos
      expr: SUM(source.data_traffic_gb)
    - name: Usuarios activos
      expr: SUM(source.active_users)
    - name: Alarmas con impacto
      expr: SUM(source.service_impact_alarms)
    - name: Suscripciones impactadas promedio
      expr: AVG(source.impacted_subscriptions_day)
    - name: Cumplimiento SLA de red
      expr: SUM(source.network_sla_compliance_pct * source.measurement_count) / SUM(source.measurement_count)
      comment: "Porcentaje ponderado con disponibilidad >= 99%, latencia <= 80 ms y downlink >= 20 Mbps"
$$;

-- COMMAND ----------

CREATE OR REPLACE VIEW mv_customer_product_experience WITH METRICS LANGUAGE YAML AS
$$
  version: 1.1
  comment: "Capa semántica gobernada para clientes, productos, valor y experiencia"
  source: gold_customer_product_daily

  joins:
    - name: customer_risk
      source: gold_customer_360
      'on': source.customer_id = customer_risk.customer_id
      rely:
        at_most_one_match: true

  fields:
    - name: Fecha
      expr: source.usage_date
    - name: Provincia
      expr: source.region
    - name: Cantón
      expr: source.canton
    - name: Tipo de cliente
      expr: source.customer_type
    - name: Segmento
      expr: source.customer_segment
    - name: Producto
      expr: source.product_name
    - name: Familia de producto
      expr: source.product_family
    - name: Tipo de contrato
      expr: source.contract_type
    - name: Estado de suscripción
      expr: source.subscription_status
    - name: Tecnología
      expr: source.technology
    - name: Sitio principal
      expr: source.primary_site_id
    - name: Riesgo de baja
      expr: customer_risk.churn_risk_band

  measures:
    - name: Clientes activos
      expr: COUNT(DISTINCT CASE WHEN source.subscription_status = 'Activa' THEN source.customer_id END)
    - name: Suscripciones activas
      expr: COUNT(DISTINCT CASE WHEN source.subscription_status = 'Activa' THEN source.subscription_id END)
    - name: Ingreso facturado
      expr: SUM(source.billed_amount_usd)
    - name: ARPU
      expr: SUM(source.billed_amount_usd) / COUNT(DISTINCT source.customer_id)
      comment: "Ingreso del período dividido por clientes únicos"
    - name: Consumo de datos
      expr: SUM(source.data_gb)
    - name: Consumo de voz
      expr: SUM(source.voice_minutes)
    - name: Ingreso por recargas
      expr: SUM(source.recharge_amount_usd)
    - name: Tickets
      expr: SUM(source.ticket_count)
    - name: Tickets de red
      expr: SUM(source.network_ticket_count)
    - name: Tickets por 100 líneas
      expr: 100.0 * SUM(source.ticket_count) / COUNT(DISTINCT source.subscription_id)
    - name: Resolución en primer contacto
      expr: 100.0 * SUM(source.first_contact_resolved_tickets) / NULLIF(SUM(source.resolved_ticket_count), 0)
    - name: Horas medias de resolución
      expr: AVG(source.avg_resolution_hours)
    - name: CSAT medio
      expr: AVG(COALESCE(source.avg_csat_score, source.avg_post_service_csat))
    - name: NPS
      expr: 100.0 * (SUM(source.nps_promoters) - SUM(source.nps_detractors)) / NULLIF(SUM(source.survey_responses), 0)
    - name: Clientes de alto riesgo
      expr: COUNT(DISTINCT CASE WHEN customer_risk.churn_risk_band = 'Alto' THEN source.customer_id END)
    - name: Cumplimiento SLA de cliente
      expr: AVG(source.network_sla_compliance_pct)
$$;

-- COMMAND ----------

-- Ejemplo 1: agrupación flexible de medidas de red.
SELECT
  `Provincia`,
  `Tecnología`,
  MEASURE(`Disponibilidad media`) AS disponibilidad_pct,
  MEASURE(`Latencia media`) AS latencia_ms,
  MEASURE(`Downlink medio`) AS downlink_mbps,
  MEASURE(`Cumplimiento SLA de red`) AS sla_pct
FROM mv_network_quality
GROUP BY `Provincia`, `Tecnología`
ORDER BY sla_pct;

-- COMMAND ----------

-- Ejemplo 2: una única definición de ARPU, NPS y experiencia para todos los consumidores.
SELECT
  `Producto`,
  MEASURE(`Clientes activos`) AS clientes_activos,
  MEASURE(`ARPU`) AS arpu_usd,
  MEASURE(`Tickets por 100 líneas`) AS tickets_por_100_lineas,
  MEASURE(`NPS`) AS nps,
  MEASURE(`Cumplimiento SLA de cliente`) AS sla_red_pct
FROM mv_customer_product_experience
GROUP BY `Producto`
ORDER BY arpu_usd DESC;
