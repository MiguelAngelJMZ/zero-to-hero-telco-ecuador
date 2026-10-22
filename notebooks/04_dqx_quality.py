# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Calidad de datos con DQX
# MAGIC
# MAGIC DQX es un proyecto open source de Databricks Labs, sin SLA de soporte. Este
# MAGIC notebook usa `DQEngine`, reglas declarativas y cuarentena para seis fuentes
# MAGIC críticas. Las impurezas fueron generadas deliberadamente con seed 42.
# MAGIC
# MAGIC Para el ejercicio guiado, ejecute primero `cell_tower_metrics`. El paquete
# MAGIC completo puede validar las seis fuentes antes de crear Gold.

# COMMAND ----------

# MAGIC %pip install "databricks-labs-dqx>=0.9,<1" "pyyaml>=6,<7"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

dbutils.widgets.dropdown("scope", "all", ["all", "guided"], "Alcance")
dbutils.widgets.text("min_downlink", "1.0", "Downlink mínimo para advertencia")

scope = dbutils.widgets.get("scope")
min_downlink = float(dbutils.widgets.get("min_downlink"))

# COMMAND ----------

import yaml
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
from pyspark.sql import functions as F


def parse_checks(text):
    checks = yaml.safe_load(text)
    DQEngine.validate_checks(checks)
    return checks


CHECKS_BY_DATASET = {
    "cell_tower_metrics": parse_checks(
        f"""
- name: measurement_id_required
  criticality: error
  check:
    function: is_not_null_and_not_empty
    arguments: {{column: measurement_id}}
- name: cell_id_required
  criticality: error
  check:
    function: is_not_null_and_not_empty
    arguments: {{column: cell_id}}
- name: cell_resolved
  criticality: error
  check:
    function: is_not_null_and_not_empty
    arguments: {{column: site_id}}
- name: region_resolved
  criticality: error
  check:
    function: is_not_null_and_not_empty
    arguments: {{column: region}}
- name: availability_range
  criticality: error
  check:
    function: is_in_range
    arguments: {{column: availability_pct, min_limit: 0, max_limit: 100}}
- name: signal_range
  criticality: error
  check:
    function: is_in_range
    arguments: {{column: signal_strength_dbm, min_limit: -120, max_limit: -40}}
- name: latency_range
  criticality: error
  check:
    function: is_in_range
    arguments: {{column: latency_ms, min_limit: 0, max_limit: 500}}
- name: downlink_required
  criticality: error
  check:
    function: is_not_null
    arguments: {{column: downlink_mbps}}
- name: downlink_workshop_sla
  criticality: warn
  check:
    function: is_in_range
    arguments: {{column: downlink_mbps, min_limit: {min_downlink}, max_limit: 1000}}
- name: technology_allowed
  criticality: error
  check:
    function: is_in_list
    arguments:
      column: technology
      allowed: ["'4G'", "'5G'"]
"""
    ),
    "subscriptions": parse_checks(
        """
- name: subscription_id_required
  criticality: error
  check:
    function: is_not_null_and_not_empty
    arguments: {column: subscription_id}
- name: customer_resolved
  criticality: error
  check:
    function: is_not_null_and_not_empty
    arguments: {column: customer_segment}
- name: product_resolved
  criticality: error
  check:
    function: is_not_null_and_not_empty
    arguments: {column: product_name}
- name: monthly_fee_non_negative
  criticality: error
  check:
    function: is_in_range
    arguments: {column: monthly_fee_usd, min_limit: 0, max_limit: 1000}
- name: status_allowed
  criticality: error
  check:
    function: is_in_list
    arguments:
      column: subscription_status
      allowed: ["'Activa'", "'Suspendida'", "'Baja'"]
"""
    ),
    "usage_daily": parse_checks(
        """
- name: usage_date_required
  criticality: error
  check:
    function: is_not_null
    arguments: {column: usage_date}
- name: subscription_resolved
  criticality: error
  check:
    function: is_not_null_and_not_empty
    arguments: {column: customer_id}
- name: cell_resolved
  criticality: error
  check:
    function: is_not_null_and_not_empty
    arguments: {column: site_id}
- name: network_region_resolved
  criticality: error
  check:
    function: is_not_null_and_not_empty
    arguments: {column: network_region}
- name: data_non_negative
  criticality: error
  check:
    function: is_in_range
    arguments: {column: data_gb, min_limit: 0, max_limit: 1000}
- name: data_5g_range
  criticality: error
  check:
    function: is_in_range
    arguments: {column: data_5g_pct, min_limit: 0, max_limit: 100}
"""
    ),
    "support_tickets": parse_checks(
        """
- name: ticket_id_required
  criticality: error
  check:
    function: is_not_null_and_not_empty
    arguments: {column: ticket_id}
- name: category_required
  criticality: error
  check:
    function: is_not_null_and_not_empty
    arguments: {column: category}
- name: subscription_resolved
  criticality: error
  check:
    function: is_not_null_and_not_empty
    arguments: {column: product_name}
- name: status_allowed
  criticality: error
  check:
    function: is_in_list
    arguments:
      column: ticket_status
      allowed: ["'Abierto'", "'En progreso'", "'Resuelto'", "'Cerrado'"]
"""
    ),
    "network_alarms": parse_checks(
        """
- name: alarm_id_required
  criticality: error
  check:
    function: is_not_null_and_not_empty
    arguments: {column: alarm_id}
- name: cell_resolved
  criticality: error
  check:
    function: is_not_null_and_not_empty
    arguments: {column: site_id}
- name: region_resolved
  criticality: error
  check:
    function: is_not_null_and_not_empty
    arguments: {column: region}
- name: severity_allowed
  criticality: error
  check:
    function: is_in_list
    arguments:
      column: severity
      allowed: ["'Crítica'", "'Alta'", "'Media'", "'Baja'"]
"""
    ),
    "customer_surveys": parse_checks(
        """
- name: survey_id_required
  criticality: error
  check:
    function: is_not_null_and_not_empty
    arguments: {column: survey_id}
- name: subscription_resolved
  criticality: error
  check:
    function: is_not_null_and_not_empty
    arguments: {column: product_name}
- name: nps_range
  criticality: error
  check:
    function: is_in_range
    arguments: {column: nps_score, min_limit: 0, max_limit: 10}
- name: csat_range
  criticality: error
  check:
    function: is_in_range
    arguments: {column: csat_score, min_limit: 1, max_limit: 5}
"""
    ),
}

# COMMAND ----------

dq = DQEngine(WorkspaceClient())
datasets_to_run = ["cell_tower_metrics", "subscriptions", "support_tickets"] if scope == "guided" else list(CHECKS_BY_DATASET)

summary_frames = []
for dataset_name in datasets_to_run:
    source_table = f"{catalog}.{schema}.{dataset_name}_silver"
    source_df = spark.table(source_table)
    checks = CHECKS_BY_DATASET[dataset_name]

    valid_df, quarantine_df = dq.apply_checks_by_metadata_and_split(source_df, checks)
    valid_table = f"{catalog}.{schema}.{dataset_name}_validated"
    quarantine_table = f"{catalog}.{schema}.{dataset_name}_quarantine"

    valid_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(valid_table)
    quarantine_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(quarantine_table)

    evaluated = dq.apply_checks_by_metadata(source_df, checks)
    error_count = F.coalesce(F.size("_errors"), F.lit(0))
    warning_count = F.coalesce(F.size("_warnings"), F.lit(0))
    summary_frames.append(
        evaluated.agg(
            F.count("*").alias("input_rows"),
            F.sum(F.when(error_count > 0, 1).otherwise(0)).alias("rows_with_errors"),
            F.sum(F.when(warning_count > 0, 1).otherwise(0)).alias("rows_with_warnings"),
            F.sum(
                F.when((error_count == 0) & (warning_count == 0), 1).otherwise(0)
            ).alias("rows_without_findings"),
        ).withColumn("dataset", F.lit(dataset_name))
    )
    print(f"DQX completado: {dataset_name} → validated + quarantine")

summary = summary_frames[0]
for frame in summary_frames[1:]:
    summary = summary.unionByName(frame)

summary = summary.select(
    "dataset", "input_rows", "rows_with_errors", "rows_with_warnings", "rows_without_findings"
).withColumn("evaluated_at", F.current_timestamp())

summary.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{catalog}.{schema}.dqx_quality_summary"
)
display(summary.orderBy("dataset"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio guiado
# MAGIC
# MAGIC 1. Seleccione `scope=guided` y ejecute con `min_downlink=1.0`.
# MAGIC 2. Cambie `min_downlink` a `20.0` y vuelva a ejecutar.
# MAGIC 3. Compare `rows_with_warnings`: los errores van a cuarentena; una advertencia
# MAGIC    permanece en `cell_tower_metrics_validated` acompañada por `_warnings`.
# MAGIC 4. Inspeccione tres impurezas en cada tabla de cuarentena.

# COMMAND ----------

display(
    spark.table(f"{catalog}.{schema}.cell_tower_metrics_quarantine")
    .select("measurement_id", "cell_id", "event_ts", "_errors", "_warnings")
    .limit(20)
)
