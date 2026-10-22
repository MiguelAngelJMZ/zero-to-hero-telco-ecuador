-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Punto de recuperación
-- MAGIC Ejecútelo únicamente si Lakeflow/DQX fallan y es necesario continuar con SQL.
-- MAGIC Crea las once Silver desde los checkpoints Delta y seis pares
-- MAGIC `validated`/`quarantine`. Los objetos usan el sufijo `_fallback` durante la
-- MAGIC transformación y publican los contratos que Gold espera.

-- COMMAND ----------

-- MAGIC %run ./_config

-- COMMAND ----------

CREATE OR REPLACE TABLE network_sites_silver_fallback AS
SELECT * FROM network_sites_raw_checkpoint QUALIFY row_number() OVER (PARTITION BY site_id ORDER BY commissioned_date DESC) = 1;

CREATE OR REPLACE TABLE radio_cells_silver_fallback AS
SELECT * FROM radio_cells_raw_checkpoint QUALIFY row_number() OVER (PARTITION BY cell_id ORDER BY site_id) = 1;

CREATE OR REPLACE TABLE products_silver_fallback AS
SELECT * FROM products_raw_checkpoint QUALIFY row_number() OVER (PARTITION BY product_id ORDER BY valid_from DESC) = 1;

CREATE OR REPLACE TABLE customers_silver_fallback AS
SELECT *, CAST(floor(months_between(DATE'2026-08-14', created_date)) AS INT) AS tenure_months
FROM customers_raw_checkpoint
QUALIFY row_number() OVER (PARTITION BY customer_id ORDER BY created_date DESC) = 1;

CREATE OR REPLACE TABLE subscriptions_silver_fallback AS
SELECT s.*, c.customer_type, c.customer_segment, c.region, c.canton, c.age_band,
       c.digital_engagement_score, c.customer_status, p.product_name, p.product_family,
       p.data_allowance_gb, p.unlimited_data, p.is_5g_enabled, p.product_status
FROM subscriptions_raw_checkpoint s
LEFT JOIN customers_silver_fallback c USING (customer_id)
LEFT JOIN products_silver_fallback p USING (product_id)
QUALIFY row_number() OVER (PARTITION BY subscription_id ORDER BY start_date DESC) = 1;

CREATE OR REPLACE TABLE cell_tower_metrics_silver_fallback AS
SELECT m.*, c.site_id, c.technology, c.frequency_band, c.bandwidth_mhz, c.capacity_users,
       s.region, s.canton, s.environment, s.natural_region, s.latitude, s.longitude,
       to_date(m.event_ts) AS event_date, date_trunc('hour', m.event_ts) AS event_hour,
       CASE WHEN m.availability_pct < 99 OR m.latency_ms > 120 OR m.downlink_mbps < 10 THEN 'Crítico'
            WHEN m.latency_ms > 80 OR m.downlink_mbps < 25 THEN 'Atención' ELSE 'Saludable' END AS network_status
FROM cell_tower_metrics_raw_checkpoint m
LEFT JOIN radio_cells_silver_fallback c USING (cell_id)
LEFT JOIN network_sites_silver_fallback s USING (site_id)
QUALIFY row_number() OVER (PARTITION BY measurement_id ORDER BY event_ts DESC) = 1;

CREATE OR REPLACE TABLE network_alarms_silver_fallback AS
SELECT a.*, c.site_id, c.technology, s.region, s.canton, to_date(a.opened_at) AS opened_date
FROM network_alarms_raw_checkpoint a
LEFT JOIN radio_cells_silver_fallback c USING (cell_id)
LEFT JOIN network_sites_silver_fallback s USING (site_id)
QUALIFY row_number() OVER (PARTITION BY alarm_id ORDER BY opened_at DESC) = 1;

CREATE OR REPLACE TABLE maintenance_orders_silver_fallback AS
SELECT o.*, s.region, s.canton, s.environment, s.natural_region, to_date(o.created_at) AS created_date
FROM maintenance_orders_raw_checkpoint o LEFT JOIN network_sites_silver_fallback s USING (site_id)
QUALIFY row_number() OVER (PARTITION BY work_order_id ORDER BY created_at DESC) = 1;

CREATE OR REPLACE TABLE usage_daily_silver_fallback AS
SELECT u.*, sub.customer_id, sub.product_id, sub.line_id, sub.subscription_status,
       sub.contract_type, sub.monthly_fee_usd, sub.customer_type, sub.customer_segment,
       sub.digital_engagement_score, sub.customer_status, sub.region AS customer_region,
       sub.canton AS customer_canton, sub.product_name, sub.product_family,
       sub.data_allowance_gb, sub.unlimited_data, c.site_id, c.technology, c.frequency_band,
       ns.region AS network_region, ns.canton AS network_canton
FROM usage_daily_raw_checkpoint u
LEFT JOIN subscriptions_silver_fallback sub USING (subscription_id)
LEFT JOIN radio_cells_silver_fallback c ON u.primary_cell_id = c.cell_id
LEFT JOIN network_sites_silver_fallback ns USING (site_id)
QUALIFY row_number() OVER (PARTITION BY usage_date, subscription_id ORDER BY ingestion_batch_id DESC) = 1;

CREATE OR REPLACE TABLE support_tickets_silver_fallback AS
SELECT t.*, sub.product_id, sub.product_name, sub.product_family, sub.customer_type,
       sub.customer_segment, sub.contract_type, c.site_id, c.technology,
       coalesce(ns.region, sub.region) AS region, coalesce(ns.canton, sub.canton) AS canton,
       to_date(t.created_at) AS created_date,
       t.resolution_hours IS NOT NULL AND t.resolution_hours <= t.sla_target_hours AS sla_met
FROM support_tickets_raw_checkpoint t
LEFT JOIN subscriptions_silver_fallback sub USING (subscription_id)
LEFT JOIN radio_cells_silver_fallback c USING (cell_id)
LEFT JOIN network_sites_silver_fallback ns USING (site_id)
QUALIFY row_number() OVER (PARTITION BY ticket_id ORDER BY created_at DESC) = 1;

CREATE OR REPLACE TABLE customer_surveys_silver_fallback AS
SELECT e.*, sub.product_id, sub.product_name, sub.product_family, sub.customer_type,
       sub.customer_segment, sub.region, sub.canton,
       CASE WHEN e.nps_score >= 9 THEN 'Promotor' WHEN e.nps_score >= 7 THEN 'Pasivo' ELSE 'Detractor' END AS nps_class
FROM customer_surveys_raw_checkpoint e LEFT JOIN subscriptions_silver_fallback sub USING (subscription_id)
QUALIFY row_number() OVER (PARTITION BY survey_id ORDER BY response_date DESC) = 1;

-- La contingencia SQL usa las mismas condiciones críticas que DQX.
CREATE OR REPLACE TABLE cell_tower_metrics_validated AS
SELECT *, CAST(array() AS ARRAY<STRING>) AS _errors,
       CASE WHEN downlink_mbps NOT BETWEEN 1 AND 1000 THEN array('downlink_workshop_sla') ELSE CAST(array() AS ARRAY<STRING>) END AS _warnings
FROM cell_tower_metrics_silver_fallback
WHERE measurement_id IS NOT NULL AND trim(measurement_id) <> ''
  AND cell_id IS NOT NULL AND trim(cell_id) <> ''
  AND site_id IS NOT NULL AND trim(site_id) <> ''
  AND region IS NOT NULL AND trim(region) <> ''
  AND availability_pct IS NOT NULL AND availability_pct BETWEEN 0 AND 100
  AND signal_strength_dbm IS NOT NULL AND signal_strength_dbm BETWEEN -120 AND -40
  AND latency_ms IS NOT NULL AND latency_ms BETWEEN 0 AND 500
  AND downlink_mbps IS NOT NULL AND technology IS NOT NULL AND technology IN ('4G', '5G');
CREATE OR REPLACE TABLE cell_tower_metrics_quarantine AS
SELECT *, array_compact(array(
  CASE WHEN measurement_id IS NULL OR trim(measurement_id) = '' THEN 'measurement_id_required' END,
  CASE WHEN cell_id IS NULL OR trim(cell_id) = '' THEN 'cell_id_required' END,
  CASE WHEN site_id IS NULL OR trim(site_id) = '' THEN 'cell_resolved' END,
  CASE WHEN region IS NULL OR trim(region) = '' THEN 'region_resolved' END,
  CASE WHEN availability_pct IS NULL OR availability_pct NOT BETWEEN 0 AND 100 THEN 'availability_range' END,
  CASE WHEN signal_strength_dbm IS NULL OR signal_strength_dbm NOT BETWEEN -120 AND -40 THEN 'signal_range' END,
  CASE WHEN latency_ms IS NULL OR latency_ms NOT BETWEEN 0 AND 500 THEN 'latency_range' END,
  CASE WHEN downlink_mbps IS NULL THEN 'downlink_required' END,
  CASE WHEN technology IS NULL OR technology NOT IN ('4G', '5G') THEN 'technology_allowed' END
)) AS _errors, CAST(array() AS ARRAY<STRING>) AS _warnings
FROM cell_tower_metrics_silver_fallback
WHERE measurement_id IS NULL OR trim(measurement_id) = ''
   OR cell_id IS NULL OR trim(cell_id) = '' OR site_id IS NULL OR trim(site_id) = ''
   OR region IS NULL OR trim(region) = ''
   OR availability_pct IS NULL OR availability_pct NOT BETWEEN 0 AND 100
   OR signal_strength_dbm IS NULL OR signal_strength_dbm NOT BETWEEN -120 AND -40
   OR latency_ms IS NULL OR latency_ms NOT BETWEEN 0 AND 500
   OR downlink_mbps IS NULL OR technology IS NULL OR technology NOT IN ('4G', '5G');

CREATE OR REPLACE TABLE subscriptions_validated AS
SELECT *, CAST(array() AS ARRAY<STRING>) AS _errors, CAST(array() AS ARRAY<STRING>) AS _warnings
FROM subscriptions_silver_fallback
WHERE subscription_id IS NOT NULL AND trim(subscription_id) <> ''
  AND customer_segment IS NOT NULL AND trim(customer_segment) <> ''
  AND product_name IS NOT NULL AND trim(product_name) <> ''
  AND monthly_fee_usd IS NOT NULL AND monthly_fee_usd BETWEEN 0 AND 1000
  AND subscription_status IS NOT NULL AND subscription_status IN ('Activa', 'Suspendida', 'Baja');
CREATE OR REPLACE TABLE subscriptions_quarantine AS
SELECT *, array_compact(array(
  CASE WHEN subscription_id IS NULL OR trim(subscription_id) = '' THEN 'subscription_id_required' END,
  CASE WHEN customer_segment IS NULL OR trim(customer_segment) = '' THEN 'customer_resolved' END,
  CASE WHEN product_name IS NULL OR trim(product_name) = '' THEN 'product_resolved' END,
  CASE WHEN monthly_fee_usd IS NULL OR monthly_fee_usd NOT BETWEEN 0 AND 1000 THEN 'monthly_fee_non_negative' END,
  CASE WHEN subscription_status IS NULL OR subscription_status NOT IN ('Activa', 'Suspendida', 'Baja') THEN 'status_allowed' END
)) AS _errors, CAST(array() AS ARRAY<STRING>) AS _warnings
FROM subscriptions_silver_fallback
WHERE subscription_id IS NULL OR trim(subscription_id) = ''
   OR customer_segment IS NULL OR trim(customer_segment) = ''
   OR product_name IS NULL OR trim(product_name) = ''
   OR monthly_fee_usd IS NULL OR monthly_fee_usd NOT BETWEEN 0 AND 1000
   OR subscription_status IS NULL OR subscription_status NOT IN ('Activa', 'Suspendida', 'Baja');

CREATE OR REPLACE TABLE usage_daily_validated AS
SELECT *, CAST(array() AS ARRAY<STRING>) AS _errors, CAST(array() AS ARRAY<STRING>) AS _warnings
FROM usage_daily_silver_fallback
WHERE usage_date IS NOT NULL
  AND customer_id IS NOT NULL AND trim(customer_id) <> ''
  AND site_id IS NOT NULL AND trim(site_id) <> ''
  AND network_region IS NOT NULL AND trim(network_region) <> ''
  AND data_gb IS NOT NULL AND data_gb BETWEEN 0 AND 1000
  AND data_5g_pct IS NOT NULL AND data_5g_pct BETWEEN 0 AND 100;
CREATE OR REPLACE TABLE usage_daily_quarantine AS
SELECT *, array_compact(array(
  CASE WHEN usage_date IS NULL THEN 'usage_date_required' END,
  CASE WHEN customer_id IS NULL OR trim(customer_id) = '' THEN 'subscription_resolved' END,
  CASE WHEN site_id IS NULL OR trim(site_id) = '' THEN 'cell_resolved' END,
  CASE WHEN network_region IS NULL OR trim(network_region) = '' THEN 'network_region_resolved' END,
  CASE WHEN data_gb IS NULL OR data_gb NOT BETWEEN 0 AND 1000 THEN 'data_non_negative' END,
  CASE WHEN data_5g_pct IS NULL OR data_5g_pct NOT BETWEEN 0 AND 100 THEN 'data_5g_range' END
)) AS _errors, CAST(array() AS ARRAY<STRING>) AS _warnings
FROM usage_daily_silver_fallback
WHERE usage_date IS NULL OR customer_id IS NULL OR trim(customer_id) = ''
   OR site_id IS NULL OR trim(site_id) = ''
   OR network_region IS NULL OR trim(network_region) = ''
   OR data_gb IS NULL OR data_gb NOT BETWEEN 0 AND 1000
   OR data_5g_pct IS NULL OR data_5g_pct NOT BETWEEN 0 AND 100;

CREATE OR REPLACE TABLE support_tickets_validated AS
SELECT *, CAST(array() AS ARRAY<STRING>) AS _errors, CAST(array() AS ARRAY<STRING>) AS _warnings
FROM support_tickets_silver_fallback
WHERE ticket_id IS NOT NULL AND trim(ticket_id) <> ''
  AND category IS NOT NULL AND trim(category) <> ''
  AND product_name IS NOT NULL AND trim(product_name) <> ''
  AND ticket_status IS NOT NULL AND ticket_status IN ('Abierto', 'En progreso', 'Resuelto', 'Cerrado');
CREATE OR REPLACE TABLE support_tickets_quarantine AS
SELECT *, array_compact(array(
  CASE WHEN ticket_id IS NULL OR trim(ticket_id) = '' THEN 'ticket_id_required' END,
  CASE WHEN category IS NULL OR trim(category) = '' THEN 'category_required' END,
  CASE WHEN product_name IS NULL OR trim(product_name) = '' THEN 'subscription_resolved' END,
  CASE WHEN ticket_status IS NULL OR ticket_status NOT IN ('Abierto', 'En progreso', 'Resuelto', 'Cerrado') THEN 'status_allowed' END
)) AS _errors, CAST(array() AS ARRAY<STRING>) AS _warnings
FROM support_tickets_silver_fallback
WHERE ticket_id IS NULL OR trim(ticket_id) = ''
   OR category IS NULL OR trim(category) = ''
   OR product_name IS NULL OR trim(product_name) = ''
   OR ticket_status IS NULL OR ticket_status NOT IN ('Abierto', 'En progreso', 'Resuelto', 'Cerrado');

CREATE OR REPLACE TABLE network_alarms_validated AS
SELECT *, CAST(array() AS ARRAY<STRING>) AS _errors, CAST(array() AS ARRAY<STRING>) AS _warnings
FROM network_alarms_silver_fallback
WHERE alarm_id IS NOT NULL AND trim(alarm_id) <> ''
  AND site_id IS NOT NULL AND trim(site_id) <> ''
  AND region IS NOT NULL AND trim(region) <> ''
  AND severity IS NOT NULL AND severity IN ('Crítica', 'Alta', 'Media', 'Baja');
CREATE OR REPLACE TABLE network_alarms_quarantine AS
SELECT *, array_compact(array(
  CASE WHEN alarm_id IS NULL OR trim(alarm_id) = '' THEN 'alarm_id_required' END,
  CASE WHEN site_id IS NULL OR trim(site_id) = '' THEN 'cell_resolved' END,
  CASE WHEN region IS NULL OR trim(region) = '' THEN 'region_resolved' END,
  CASE WHEN severity IS NULL OR severity NOT IN ('Crítica', 'Alta', 'Media', 'Baja') THEN 'severity_allowed' END
)) AS _errors, CAST(array() AS ARRAY<STRING>) AS _warnings
FROM network_alarms_silver_fallback
WHERE alarm_id IS NULL OR trim(alarm_id) = ''
   OR site_id IS NULL OR trim(site_id) = ''
   OR region IS NULL OR trim(region) = ''
   OR severity IS NULL OR severity NOT IN ('Crítica', 'Alta', 'Media', 'Baja');

CREATE OR REPLACE TABLE customer_surveys_validated AS
SELECT *, CAST(array() AS ARRAY<STRING>) AS _errors, CAST(array() AS ARRAY<STRING>) AS _warnings
FROM customer_surveys_silver_fallback
WHERE survey_id IS NOT NULL AND trim(survey_id) <> ''
  AND product_name IS NOT NULL AND trim(product_name) <> ''
  AND nps_score IS NOT NULL AND nps_score BETWEEN 0 AND 10
  AND csat_score IS NOT NULL AND csat_score BETWEEN 1 AND 5;
CREATE OR REPLACE TABLE customer_surveys_quarantine AS
SELECT *, array_compact(array(
  CASE WHEN survey_id IS NULL OR trim(survey_id) = '' THEN 'survey_id_required' END,
  CASE WHEN product_name IS NULL OR trim(product_name) = '' THEN 'subscription_resolved' END,
  CASE WHEN nps_score IS NULL OR nps_score NOT BETWEEN 0 AND 10 THEN 'nps_range' END,
  CASE WHEN csat_score IS NULL OR csat_score NOT BETWEEN 1 AND 5 THEN 'csat_range' END
)) AS _errors, CAST(array() AS ARRAY<STRING>) AS _warnings
FROM customer_surveys_silver_fallback
WHERE survey_id IS NULL OR trim(survey_id) = ''
   OR product_name IS NULL OR trim(product_name) = ''
   OR nps_score IS NULL OR nps_score NOT BETWEEN 0 AND 10
   OR csat_score IS NULL OR csat_score NOT BETWEEN 1 AND 5;
