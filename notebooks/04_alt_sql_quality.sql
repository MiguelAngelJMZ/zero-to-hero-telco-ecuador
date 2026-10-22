-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 04 alternativa — Calidad con SQL
-- MAGIC Úselo si la biblioteca DQX no puede instalarse. Mantiene el flujo del workshop,
-- MAGIC pero no sustituye las capacidades declarativas y reutilizables de DQX.

-- COMMAND ----------

-- MAGIC %run ./_config

-- COMMAND ----------

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
)) AS failed_rules
FROM cell_tower_metrics_silver_fallback
WHERE measurement_id IS NULL OR trim(measurement_id) = ''
   OR cell_id IS NULL OR trim(cell_id) = '' OR site_id IS NULL OR trim(site_id) = ''
   OR region IS NULL OR trim(region) = ''
   OR availability_pct IS NULL OR availability_pct NOT BETWEEN 0 AND 100
   OR signal_strength_dbm IS NULL OR signal_strength_dbm NOT BETWEEN -120 AND -40
   OR latency_ms IS NULL OR latency_ms NOT BETWEEN 0 AND 500
   OR downlink_mbps IS NULL OR technology IS NULL OR technology NOT IN ('4G', '5G');

CREATE OR REPLACE TABLE cell_tower_metrics_validated AS
SELECT *, CASE WHEN downlink_mbps NOT BETWEEN 1 AND 1000 THEN array('downlink_workshop_sla') ELSE array() END AS _warnings
FROM cell_tower_metrics_silver_fallback
WHERE measurement_id IS NOT NULL AND trim(measurement_id) <> ''
  AND cell_id IS NOT NULL AND trim(cell_id) <> '' AND site_id IS NOT NULL AND trim(site_id) <> ''
  AND region IS NOT NULL AND trim(region) <> ''
  AND availability_pct BETWEEN 0 AND 100
  AND signal_strength_dbm BETWEEN -120 AND -40
  AND latency_ms BETWEEN 0 AND 500
  AND downlink_mbps IS NOT NULL AND technology IN ('4G', '5G');

-- COMMAND ----------

CREATE OR REPLACE TABLE subscriptions_quarantine AS
SELECT *, array_compact(array(
  CASE WHEN subscription_id IS NULL OR trim(subscription_id) = '' THEN 'subscription_id_required' END,
  CASE WHEN customer_segment IS NULL OR trim(customer_segment) = '' THEN 'customer_resolved' END,
  CASE WHEN product_name IS NULL OR trim(product_name) = '' THEN 'product_resolved' END,
  CASE WHEN monthly_fee_usd IS NULL OR monthly_fee_usd NOT BETWEEN 0 AND 1000 THEN 'monthly_fee_non_negative' END,
  CASE WHEN subscription_status IS NULL OR subscription_status NOT IN ('Activa', 'Suspendida', 'Baja') THEN 'status_allowed' END
)) AS failed_rules
FROM subscriptions_silver_fallback
WHERE subscription_id IS NULL OR trim(subscription_id) = ''
   OR customer_segment IS NULL OR trim(customer_segment) = ''
   OR product_name IS NULL OR trim(product_name) = ''
   OR monthly_fee_usd IS NULL OR monthly_fee_usd NOT BETWEEN 0 AND 1000
   OR subscription_status IS NULL OR subscription_status NOT IN ('Activa', 'Suspendida', 'Baja');

CREATE OR REPLACE TABLE subscriptions_validated AS
SELECT *, CAST(array() AS ARRAY<STRING>) AS _warnings
FROM subscriptions_silver_fallback
WHERE subscription_id IS NOT NULL AND trim(subscription_id) <> ''
  AND customer_segment IS NOT NULL AND trim(customer_segment) <> ''
  AND product_name IS NOT NULL AND trim(product_name) <> ''
  AND monthly_fee_usd BETWEEN 0 AND 1000
  AND subscription_status IN ('Activa', 'Suspendida', 'Baja');

-- COMMAND ----------

CREATE OR REPLACE TABLE usage_daily_quarantine AS
SELECT *, array_compact(array(
  CASE WHEN usage_date IS NULL THEN 'usage_date_required' END,
  CASE WHEN customer_id IS NULL OR trim(customer_id) = '' THEN 'subscription_resolved' END,
  CASE WHEN site_id IS NULL OR trim(site_id) = '' THEN 'cell_resolved' END,
  CASE WHEN network_region IS NULL OR trim(network_region) = '' THEN 'network_region_resolved' END,
  CASE WHEN data_gb IS NULL OR data_gb NOT BETWEEN 0 AND 1000 THEN 'data_non_negative' END,
  CASE WHEN data_5g_pct IS NULL OR data_5g_pct NOT BETWEEN 0 AND 100 THEN 'data_5g_range' END
)) AS failed_rules
FROM usage_daily_silver_fallback
WHERE usage_date IS NULL OR customer_id IS NULL OR trim(customer_id) = ''
   OR site_id IS NULL OR trim(site_id) = ''
   OR network_region IS NULL OR trim(network_region) = ''
   OR data_gb IS NULL OR data_gb NOT BETWEEN 0 AND 1000
   OR data_5g_pct IS NULL OR data_5g_pct NOT BETWEEN 0 AND 100;

CREATE OR REPLACE TABLE usage_daily_validated AS
SELECT *, CAST(array() AS ARRAY<STRING>) AS _warnings
FROM usage_daily_silver_fallback
WHERE usage_date IS NOT NULL AND customer_id IS NOT NULL AND trim(customer_id) <> ''
  AND site_id IS NOT NULL AND trim(site_id) <> ''
  AND network_region IS NOT NULL AND trim(network_region) <> '' AND data_gb BETWEEN 0 AND 1000
  AND data_5g_pct BETWEEN 0 AND 100;

-- COMMAND ----------

CREATE OR REPLACE TABLE support_tickets_quarantine AS
SELECT *, array_compact(array(
  CASE WHEN ticket_id IS NULL OR trim(ticket_id) = '' THEN 'ticket_id_required' END,
  CASE WHEN category IS NULL OR trim(category) = '' THEN 'category_required' END,
  CASE WHEN product_name IS NULL OR trim(product_name) = '' THEN 'subscription_resolved' END,
  CASE WHEN ticket_status IS NULL OR ticket_status NOT IN ('Abierto', 'En progreso', 'Resuelto', 'Cerrado') THEN 'status_allowed' END
)) AS failed_rules
FROM support_tickets_silver_fallback
WHERE ticket_id IS NULL OR trim(ticket_id) = ''
   OR category IS NULL OR trim(category) = ''
   OR product_name IS NULL OR trim(product_name) = ''
   OR ticket_status IS NULL OR ticket_status NOT IN ('Abierto', 'En progreso', 'Resuelto', 'Cerrado');

CREATE OR REPLACE TABLE support_tickets_validated AS
SELECT *, CAST(array() AS ARRAY<STRING>) AS _warnings
FROM support_tickets_silver_fallback
WHERE ticket_id IS NOT NULL AND trim(ticket_id) <> ''
  AND category IS NOT NULL AND trim(category) <> ''
  AND product_name IS NOT NULL AND trim(product_name) <> ''
  AND ticket_status IN ('Abierto', 'En progreso', 'Resuelto', 'Cerrado');

-- COMMAND ----------

CREATE OR REPLACE TABLE network_alarms_quarantine AS
SELECT *, array_compact(array(
  CASE WHEN alarm_id IS NULL OR trim(alarm_id) = '' THEN 'alarm_id_required' END,
  CASE WHEN site_id IS NULL OR trim(site_id) = '' THEN 'cell_resolved' END,
  CASE WHEN region IS NULL OR trim(region) = '' THEN 'region_resolved' END,
  CASE WHEN severity IS NULL OR severity NOT IN ('Crítica', 'Alta', 'Media', 'Baja') THEN 'severity_allowed' END
)) AS failed_rules
FROM network_alarms_silver_fallback
WHERE alarm_id IS NULL OR trim(alarm_id) = ''
   OR site_id IS NULL OR trim(site_id) = ''
   OR region IS NULL OR trim(region) = ''
   OR severity IS NULL OR severity NOT IN ('Crítica', 'Alta', 'Media', 'Baja');

CREATE OR REPLACE TABLE network_alarms_validated AS
SELECT *, CAST(array() AS ARRAY<STRING>) AS _warnings
FROM network_alarms_silver_fallback
WHERE alarm_id IS NOT NULL AND trim(alarm_id) <> ''
  AND site_id IS NOT NULL AND trim(site_id) <> ''
  AND region IS NOT NULL AND trim(region) <> ''
  AND severity IN ('Crítica', 'Alta', 'Media', 'Baja');

-- COMMAND ----------

CREATE OR REPLACE TABLE customer_surveys_quarantine AS
SELECT *, array_compact(array(
  CASE WHEN survey_id IS NULL OR trim(survey_id) = '' THEN 'survey_id_required' END,
  CASE WHEN product_name IS NULL OR trim(product_name) = '' THEN 'subscription_resolved' END,
  CASE WHEN nps_score IS NULL OR nps_score NOT BETWEEN 0 AND 10 THEN 'nps_range' END,
  CASE WHEN csat_score IS NULL OR csat_score NOT BETWEEN 1 AND 5 THEN 'csat_range' END
)) AS failed_rules
FROM customer_surveys_silver_fallback
WHERE survey_id IS NULL OR trim(survey_id) = ''
   OR product_name IS NULL OR trim(product_name) = ''
   OR nps_score IS NULL OR nps_score NOT BETWEEN 0 AND 10
   OR csat_score IS NULL OR csat_score NOT BETWEEN 1 AND 5;

CREATE OR REPLACE TABLE customer_surveys_validated AS
SELECT *, CAST(array() AS ARRAY<STRING>) AS _warnings
FROM customer_surveys_silver_fallback
WHERE survey_id IS NOT NULL AND trim(survey_id) <> ''
  AND product_name IS NOT NULL AND trim(product_name) <> ''
  AND nps_score BETWEEN 0 AND 10 AND csat_score BETWEEN 1 AND 5;

-- COMMAND ----------

SELECT 'cell_tower_metrics' AS dataset, count(*) AS quarantine_rows FROM cell_tower_metrics_quarantine
UNION ALL SELECT 'subscriptions', count(*) FROM subscriptions_quarantine
UNION ALL SELECT 'usage_daily', count(*) FROM usage_daily_quarantine
UNION ALL SELECT 'support_tickets', count(*) FROM support_tickets_quarantine
UNION ALL SELECT 'network_alarms', count(*) FROM network_alarms_quarantine
UNION ALL SELECT 'customer_surveys', count(*) FROM customer_surveys_quarantine
ORDER BY dataset;
