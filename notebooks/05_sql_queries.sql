-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 05 — Gold: red, Customer 360, productos e impacto
-- MAGIC
-- MAGIC Use compute **Serverless** (no un SQL Warehouse: `%run` lo requiere). Estas vistas Gold consumen los
-- MAGIC registros validados por DQX y alimentan Metric Views, Dashboard, Genie y App.

-- COMMAND ----------

-- MAGIC %run ./_config

-- COMMAND ----------

CREATE WIDGET TEXT metrics_table DEFAULT "cell_tower_metrics_validated";
CREATE WIDGET TEXT alarms_table DEFAULT "network_alarms_validated";
CREATE WIDGET TEXT usage_table DEFAULT "usage_daily_validated";
CREATE WIDGET TEXT subscriptions_table DEFAULT "subscriptions_validated";
CREATE WIDGET TEXT tickets_table DEFAULT "support_tickets_validated";
CREATE WIDGET TEXT surveys_table DEFAULT "customer_surveys_validated";

-- COMMAND ----------

CREATE OR REPLACE VIEW gold_network_hourly
COMMENT 'Gold horario de calidad de red, alarmas e impacto en suscripciones'
AS
WITH metrics AS (
  SELECT
    event_hour,
    event_date,
    region,
    canton,
    site_id,
    technology,
    frequency_band,
    environment,
    natural_region,
    count(*) AS measurement_count,
    count(DISTINCT cell_id) AS active_cells,
    round(avg(availability_pct), 3) AS avg_availability_pct,
    round(avg(signal_strength_dbm), 2) AS avg_signal_dbm,
    round(avg(sinr_db), 2) AS avg_sinr_db,
    round(avg(latency_ms), 2) AS avg_latency_ms,
    round(avg(downlink_mbps), 2) AS avg_downlink_mbps,
    round(avg(uplink_mbps), 2) AS avg_uplink_mbps,
    round(avg(packet_loss_pct), 3) AS avg_packet_loss_pct,
    sum(dropped_calls) AS dropped_calls,
    round(sum(data_traffic_gb), 2) AS data_traffic_gb,
    sum(active_users) AS active_users,
    round(100.0 * avg(
      CASE WHEN availability_pct >= 99.0 AND latency_ms <= 80 AND downlink_mbps >= 20
           THEN 1.0 ELSE 0.0 END
    ), 2) AS network_sla_compliance_pct
  FROM IDENTIFIER(:metrics_table)
  GROUP BY event_hour, event_date, region, canton, site_id, technology,
           frequency_band, environment, natural_region
), alarms AS (
  SELECT
    date_trunc('hour', opened_at) AS event_hour,
    site_id,
    count(*) AS alarms,
    count_if(service_impact) AS service_impact_alarms,
    sum(CASE WHEN service_impact THEN affected_users_est ELSE 0 END) AS affected_users_est
  FROM IDENTIFIER(:alarms_table)
  GROUP BY date_trunc('hour', opened_at), site_id
), impacted AS (
  SELECT usage_date, site_id, count(DISTINCT subscription_id) AS impacted_subscriptions_day
  FROM IDENTIFIER(:usage_table)
  GROUP BY usage_date, site_id
)
SELECT
  m.*,
  coalesce(a.alarms, 0) AS alarms,
  coalesce(a.service_impact_alarms, 0) AS service_impact_alarms,
  coalesce(a.affected_users_est, 0) AS affected_users_est,
  coalesce(i.impacted_subscriptions_day, 0) AS impacted_subscriptions_day,
  CASE
    WHEN m.network_sla_compliance_pct < 85 OR m.avg_availability_pct < 97 THEN 'Crítico'
    WHEN m.network_sla_compliance_pct < 95 OR m.avg_latency_ms > 80 THEN 'Atención'
    ELSE 'Saludable'
  END AS health_status
FROM metrics m
LEFT JOIN alarms a USING (event_hour, site_id)
LEFT JOIN impacted i ON i.usage_date = m.event_date AND i.site_id = m.site_id;

-- COMMAND ----------

CREATE OR REPLACE VIEW gold_site_daily_360
COMMENT 'Gold diario 360 del sitio: red, alarmas, mantenimiento, tickets e impacto'
AS
WITH network AS (
  SELECT
    event_date,
    region,
    canton,
    site_id,
    first(environment) AS environment,
    first(natural_region) AS natural_region,
    count(DISTINCT technology) AS technologies,
    sum(measurement_count) AS measurement_count,
    max(active_cells) AS active_cells,
    round(sum(avg_availability_pct * measurement_count) / sum(measurement_count), 3) AS avg_availability_pct,
    round(sum(avg_latency_ms * measurement_count) / sum(measurement_count), 2) AS avg_latency_ms,
    round(sum(avg_downlink_mbps * measurement_count) / sum(measurement_count), 2) AS avg_downlink_mbps,
    round(sum(avg_packet_loss_pct * measurement_count) / sum(measurement_count), 3) AS avg_packet_loss_pct,
    sum(dropped_calls) AS dropped_calls,
    round(sum(data_traffic_gb), 2) AS data_traffic_gb,
    sum(service_impact_alarms) AS service_impact_alarms,
    max(impacted_subscriptions_day) AS impacted_subscriptions,
    round(sum(network_sla_compliance_pct * measurement_count) / sum(measurement_count), 2) AS network_sla_compliance_pct
  FROM gold_network_hourly
  GROUP BY event_date, region, canton, site_id
), tickets AS (
  SELECT
    created_date AS event_date,
    site_id,
    count(*) AS tickets,
    count_if(category IN ('Cobertura', 'Velocidad', 'Llamadas')) AS network_tickets,
    count_if(ticket_status IN ('Abierto', 'En progreso')) AS open_tickets,
    count_if(severity = 'Crítica') AS critical_tickets,
    round(avg(resolution_hours), 2) AS avg_resolution_hours
  FROM IDENTIFIER(:tickets_table)
  WHERE site_id IS NOT NULL
  GROUP BY created_date, site_id
), maintenance AS (
  SELECT
    created_date AS event_date,
    site_id,
    count(*) AS maintenance_orders,
    count_if(priority = 'P1') AS p1_orders,
    sum(downtime_minutes) AS downtime_minutes,
    round(sum(cost_usd), 2) AS maintenance_cost_usd
  FROM maintenance_orders_silver
  WHERE priority IN ('P1', 'P2', 'P3', 'P4')
    AND cost_usd >= 0
    AND (completed_at IS NULL OR completed_at >= created_at)
  GROUP BY created_date, site_id
)
SELECT
  n.*,
  coalesce(t.tickets, 0) AS tickets,
  coalesce(t.network_tickets, 0) AS network_tickets,
  coalesce(t.open_tickets, 0) AS open_tickets,
  coalesce(t.critical_tickets, 0) AS critical_tickets,
  t.avg_resolution_hours,
  coalesce(m.maintenance_orders, 0) AS maintenance_orders,
  coalesce(m.p1_orders, 0) AS p1_orders,
  coalesce(m.downtime_minutes, 0) AS downtime_minutes,
  coalesce(m.maintenance_cost_usd, 0) AS maintenance_cost_usd,
  CASE
    WHEN n.network_sla_compliance_pct < 85 OR coalesce(t.critical_tickets, 0) > 0 THEN 'Crítico'
    WHEN n.network_sla_compliance_pct < 95 OR coalesce(t.open_tickets, 0) > 2 THEN 'Atención'
    ELSE 'Saludable'
  END AS site_health_status
FROM network n
LEFT JOIN tickets t USING (event_date, site_id)
LEFT JOIN maintenance m USING (event_date, site_id);

-- COMMAND ----------

CREATE OR REPLACE VIEW gold_customer_product_daily
COMMENT 'Gold diario de cliente, suscripción, producto, uso, atención y experiencia de red'
AS
WITH tickets AS (
  SELECT
    created_date AS event_date,
    subscription_id,
    count(*) AS ticket_count,
    count_if(category IN ('Cobertura', 'Velocidad', 'Llamadas')) AS network_ticket_count,
    count_if(ticket_status IN ('Resuelto', 'Cerrado')) AS resolved_ticket_count,
    count_if(first_contact_resolution) AS first_contact_resolved_tickets,
    round(avg(resolution_hours), 2) AS avg_resolution_hours,
    round(avg(post_service_csat), 2) AS avg_post_service_csat
  FROM IDENTIFIER(:tickets_table)
  GROUP BY created_date, subscription_id
), surveys AS (
  SELECT
    response_date AS event_date,
    subscription_id,
    count(*) AS survey_responses,
    count_if(nps_score >= 9) AS nps_promoters,
    count_if(nps_score <= 6) AS nps_detractors,
    round(avg(nps_score), 2) AS avg_nps_score,
    round(avg(csat_score), 2) AS avg_csat_score
  FROM IDENTIFIER(:surveys_table)
  GROUP BY response_date, subscription_id
), cell_quality AS (
  SELECT
    event_date,
    cell_id,
    round(avg(availability_pct), 3) AS avg_availability_pct,
    round(avg(latency_ms), 2) AS avg_latency_ms,
    round(avg(downlink_mbps), 2) AS avg_downlink_mbps,
    round(100.0 * avg(
      CASE WHEN availability_pct >= 99.0 AND latency_ms <= 80 AND downlink_mbps >= 20
           THEN 1.0 ELSE 0.0 END
    ), 2) AS network_sla_compliance_pct
  FROM IDENTIFIER(:metrics_table)
  GROUP BY event_date, cell_id
)
SELECT
  u.usage_date,
  u.customer_id,
  u.subscription_id,
  u.line_id,
  u.customer_type,
  u.customer_segment,
  u.customer_region AS region,
  u.customer_canton AS canton,
  u.digital_engagement_score,
  u.customer_status,
  u.product_id,
  u.product_name,
  u.product_family,
  u.data_allowance_gb,
  u.unlimited_data,
  u.contract_type,
  u.subscription_status,
  u.monthly_fee_usd,
  u.site_id AS primary_site_id,
  u.primary_cell_id,
  u.technology,
  u.frequency_band,
  u.data_gb,
  u.voice_minutes,
  u.sms_count,
  u.data_5g_pct,
  u.roaming_data_mb,
  u.dropped_calls,
  coalesce(u.recharge_amount_usd, 0) AS recharge_amount_usd,
  u.billed_amount_usd,
  coalesce(t.ticket_count, 0) AS ticket_count,
  coalesce(t.network_ticket_count, 0) AS network_ticket_count,
  coalesce(t.resolved_ticket_count, 0) AS resolved_ticket_count,
  coalesce(t.first_contact_resolved_tickets, 0) AS first_contact_resolved_tickets,
  t.avg_resolution_hours,
  t.avg_post_service_csat,
  coalesce(s.survey_responses, 0) AS survey_responses,
  coalesce(s.nps_promoters, 0) AS nps_promoters,
  coalesce(s.nps_detractors, 0) AS nps_detractors,
  s.avg_nps_score,
  s.avg_csat_score,
  q.avg_availability_pct,
  q.avg_latency_ms,
  q.avg_downlink_mbps,
  q.network_sla_compliance_pct
FROM IDENTIFIER(:usage_table) u
LEFT JOIN tickets t
  ON t.event_date = u.usage_date AND t.subscription_id = u.subscription_id
LEFT JOIN surveys s
  ON s.event_date = u.usage_date AND s.subscription_id = u.subscription_id
LEFT JOIN cell_quality q
  ON q.event_date = u.usage_date AND q.cell_id = u.primary_cell_id;

-- COMMAND ----------

CREATE OR REPLACE VIEW gold_customer_360
COMMENT 'Customer 360 sin PII con uso, valor, soporte, NPS, red y riesgo calculado'
AS
WITH customer_rollup AS (
  SELECT
    customer_id,
    first(customer_type) AS customer_type,
    first(customer_segment) AS customer_segment,
    first(region) AS region,
    first(canton) AS canton,
    max(digital_engagement_score) AS digital_engagement_score,
    first(customer_status) AS customer_status,
    count(DISTINCT subscription_id) AS subscriptions,
    count(DISTINCT product_id) AS products,
    concat_ws(', ', sort_array(collect_set(product_name))) AS product_names,
    round(sum(data_gb), 2) AS total_data_gb,
    round(sum(voice_minutes), 2) AS total_voice_minutes,
    round(sum(billed_amount_usd), 2) AS billed_revenue_usd,
    round(sum(recharge_amount_usd), 2) AS recharge_revenue_usd,
    sum(ticket_count) AS tickets,
    sum(network_ticket_count) AS network_tickets,
    round(avg(avg_post_service_csat), 2) AS avg_post_service_csat,
    sum(survey_responses) AS survey_responses,
    sum(nps_promoters) AS nps_promoters,
    sum(nps_detractors) AS nps_detractors,
    round(avg(avg_nps_score), 2) AS avg_nps_score,
    round(avg(avg_csat_score), 2) AS avg_csat_score,
    round(avg(network_sla_compliance_pct), 2) AS network_sla_compliance_pct,
    round(avg(avg_latency_ms), 2) AS avg_latency_ms,
    round(avg(avg_downlink_mbps), 2) AS avg_downlink_mbps
  FROM gold_customer_product_daily
  GROUP BY customer_id
)
SELECT
  c.*,
  CASE WHEN c.survey_responses > 0
       THEN round(100.0 * (c.nps_promoters - c.nps_detractors) / c.survey_responses, 2)
  END AS nps,
  round(least(100.0,
    0.35 * (100.0 - coalesce(c.network_sla_compliance_pct, 100.0))
    + 4.0 * c.network_tickets
    + 3.0 * (10.0 - coalesce(c.avg_nps_score, 7.0))
    + 0.10 * (100.0 - coalesce(c.digital_engagement_score, 50))
  ), 2) AS churn_risk_score,
  CASE
    WHEN 0.35 * (100.0 - coalesce(c.network_sla_compliance_pct, 100.0))
       + 4.0 * c.network_tickets
       + 3.0 * (10.0 - coalesce(c.avg_nps_score, 7.0)) >= 35 THEN 'Alto'
    WHEN 0.35 * (100.0 - coalesce(c.network_sla_compliance_pct, 100.0))
       + 4.0 * c.network_tickets
       + 3.0 * (10.0 - coalesce(c.avg_nps_score, 7.0)) >= 18 THEN 'Medio'
    ELSE 'Bajo'
  END AS churn_risk_band
FROM customer_rollup c;

-- COMMAND ----------

CREATE OR REPLACE VIEW gold_incident_impact
COMMENT 'Gold de incidentes con mantenimiento, impacto de red y suscripciones afectadas'
AS
WITH maintenance AS (
  SELECT
    alarm_id,
    count(*) AS work_orders,
    count_if(priority = 'P1') AS p1_orders,
    sum(downtime_minutes) AS downtime_minutes,
    round(sum(CASE WHEN cost_usd >= 0 THEN cost_usd ELSE 0 END), 2) AS maintenance_cost_usd,
    max(completed_at) AS last_completed_at
  FROM maintenance_orders_silver
  WHERE alarm_id IS NOT NULL
  GROUP BY alarm_id
), subscriptions AS (
  SELECT
    usage_date,
    site_id,
    count(DISTINCT subscription_id) AS impacted_subscriptions,
    count(DISTINCT customer_id) AS impacted_customers,
    round(sum(billed_amount_usd), 2) AS billed_revenue_at_risk_usd
  FROM IDENTIFIER(:usage_table)
  GROUP BY usage_date, site_id
), quality AS (
  SELECT
    event_date,
    site_id,
    round(avg(latency_ms), 2) AS avg_latency_ms,
    round(avg(downlink_mbps), 2) AS avg_downlink_mbps,
    round(avg(availability_pct), 3) AS avg_availability_pct
  FROM IDENTIFIER(:metrics_table)
  GROUP BY event_date, site_id
)
SELECT
  a.alarm_id,
  a.opened_at,
  a.closed_at,
  a.opened_date,
  a.alarm_type,
  a.severity,
  a.alarm_status,
  a.probable_cause,
  a.service_impact,
  a.affected_users_est,
  a.cell_id,
  a.site_id,
  a.region,
  a.canton,
  a.technology,
  coalesce(m.work_orders, 0) AS work_orders,
  coalesce(m.p1_orders, 0) AS p1_orders,
  coalesce(m.downtime_minutes, 0) AS downtime_minutes,
  coalesce(m.maintenance_cost_usd, 0) AS maintenance_cost_usd,
  m.last_completed_at,
  coalesce(s.impacted_subscriptions, 0) AS impacted_subscriptions,
  coalesce(s.impacted_customers, 0) AS impacted_customers,
  coalesce(s.billed_revenue_at_risk_usd, 0) AS billed_revenue_at_risk_usd,
  q.avg_latency_ms,
  q.avg_downlink_mbps,
  q.avg_availability_pct,
  CASE
    WHEN a.severity = 'Crítica' AND coalesce(s.impacted_subscriptions, 0) >= 25 THEN 'Crítico'
    WHEN a.service_impact OR coalesce(s.impacted_subscriptions, 0) >= 10 THEN 'Alto'
    ELSE 'Moderado'
  END AS business_impact
FROM IDENTIFIER(:alarms_table) a
LEFT JOIN maintenance m USING (alarm_id)
LEFT JOIN subscriptions s ON s.usage_date = a.opened_date AND s.site_id = a.site_id
LEFT JOIN quality q ON q.event_date = a.opened_date AND q.site_id = a.site_id;

-- COMMAND ----------

SELECT 'gold_network_hourly' AS objeto, count(*) AS filas FROM gold_network_hourly
UNION ALL SELECT 'gold_site_daily_360', count(*) FROM gold_site_daily_360
UNION ALL SELECT 'gold_customer_product_daily', count(*) FROM gold_customer_product_daily
UNION ALL SELECT 'gold_customer_360', count(*) FROM gold_customer_360
UNION ALL SELECT 'gold_incident_impact', count(*) FROM gold_incident_impact
ORDER BY objeto;

-- COMMAND ----------

SELECT product_name, customer_segment,
       count(DISTINCT customer_id) AS clientes,
       round(sum(billed_amount_usd) / count(DISTINCT customer_id), 2) AS arpu_periodo_usd,
       round(100.0 * sum(ticket_count) / count(DISTINCT subscription_id), 2) AS tickets_por_100_lineas,
       round(avg(network_sla_compliance_pct), 2) AS sla_red_pct
FROM gold_customer_product_daily
GROUP BY product_name, customer_segment
ORDER BY arpu_periodo_usd DESC
LIMIT 10;
