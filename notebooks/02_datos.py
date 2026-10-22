# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Datos sintéticos con un Lakeflow Job
# MAGIC
# MAGIC Los datos no se generan dentro de este notebook: se crea un **job** con el SDK y
# MAGIC usted lo ejecuta desde la interfaz de **Jobs & Pipelines** para ver la corrida, sus
# MAGIC parámetros, el tiempo de cada tarea y la salida del notebook.
# MAGIC
# MAGIC 1. Ejecute la sección 1: crea (o actualiza) su job y muestra el enlace.
# MAGIC 2. Abra el enlace, pulse **Run now** y espere a que termine (2–4 minutos).
# MAGIC 3. Vuelva aquí y ejecute la sección 3 para validar los CSV.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Crear el job con el SDK
# MAGIC Observe la definición: una tarea de tipo notebook, compute serverless (no se
# MAGIC declara cluster) y parámetros con su catálogo y schema.

# COMMAND ----------

job_id = workshop.upsert_data_job(w, cfg, seed=42)
job = w.jobs.get(job_id)
task = job.settings.tasks[0]
print(f"Job:       {job.settings.name} ({job_id})")
print(f"Tarea:     {task.task_key} → {task.notebook_task.notebook_path}")
print(f"Parámetros {task.notebook_task.base_parameters}")
show_link("Abrir el job y pulsar Run now", workshop.workspace_url(w, f"jobs/{job_id}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Estado de la última corrida
# MAGIC Puede volver a ejecutar esta celda mientras el job avanza.

# COMMAND ----------

last_run = next(iter(w.jobs.list_runs(job_id=job_id, limit=1)), None)
if last_run is None:
    print("Todavía no hay corridas: abra el enlace y pulse Run now.")
else:
    state = last_run.state
    print(f"Run {last_run.run_id}: {state.life_cycle_state.value} {state.result_state.value if state.result_state else ''}")
    show_link("Ver la corrida", last_run.run_page_url)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Leer y validar las once fuentes CSV
# MAGIC Lectura batch con PySpark antes de que Lakeflow realice la ingesta incremental
# MAGIC con Auto Loader. Todos los datos de clientes son seudónimos y no contienen PII.

# COMMAND ----------

volume_root = cfg.volume_root
landed = {entry.name.rstrip("/") for entry in dbutils.fs.ls(volume_root)}
assert landed, f"{volume_root} está vacío: ejecute primero el job de la sección 1."

DATASET_COUNTS = {
    "network_sites": 60,
    "radio_cells": 180,
    "cell_tower_metrics": 30_240,
    "network_alarms": 1_200,
    "maintenance_orders": 400,
    "products": 18,
    "customers": 3_000,
    "subscriptions": 4_200,
    "usage_daily": 58_800,
    "support_tickets": 1_500,
    "customer_surveys": 800,
}

datasets = {}
for dataset_name in DATASET_COUNTS:
    dataset_path = f"{volume_root}/{dataset_name}"
    datasets[dataset_name] = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(dataset_path)
    )
    print(f"{dataset_name:24s} → {dataset_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Verificar cantidades y schemas
# MAGIC
# MAGIC `inferSchema=true` es intencional en esta introducción. El pipeline usa schemas
# MAGIC explícitos para evitar que un archivo defectuoso modifique el contrato de datos.

# COMMAND ----------

actual_counts = []
for dataset_name, dataframe in datasets.items():
    count = dataframe.count()
    expected = DATASET_COUNTS[dataset_name]
    actual_counts.append((dataset_name, count, expected, count == expected))
    assert count == expected, f"{dataset_name}: se esperaban {expected}; se obtuvieron {count}"

display(
    spark.createDataFrame(
        actual_counts,
        "dataset string, filas long, esperado long, valido boolean",
    ).orderBy("dataset")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Explorar continuidad entre dominios
# MAGIC
# MAGIC Una suscripción conecta cliente y producto; el consumo conecta la suscripción con
# MAGIC una celda. Más adelante, Silver resolverá estas relaciones de manera gobernada.

# COMMAND ----------

customers_df = datasets["customers"]
products_df = datasets["products"]
subscriptions_df = datasets["subscriptions"]
usage_df = datasets["usage_daily"]

customer_product_sample = (
    subscriptions_df.alias("s")
    .join(customers_df.alias("c"), "customer_id", "left")
    .join(products_df.alias("p"), "product_id", "left")
    .join(usage_df.alias("u"), "subscription_id", "left")
    .select(
        "customer_id",
        "subscription_id",
        "line_id",
        "customer_segment",
        "product_name",
        "usage_date",
        "data_gb",
        "primary_cell_id",
    )
)
display(customer_product_sample.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Observar las impurezas preparadas para DQX
# MAGIC
# MAGIC Las anomalías son deliberadas. No las corrija aquí: Lakeflow las conserva y DQX
# MAGIC las separa en registros validados y cuarentena.

# COMMAND ----------

from pyspark.sql import functions as F

metrics_df = datasets["cell_tower_metrics"]
quality_preview = metrics_df.agg(
    F.count("*").alias("filas"),
    F.sum(F.col("cell_id").isNull().cast("int")).alias("cell_id_nulo"),
    F.sum((~F.col("latency_ms").between(0, 500)).cast("int")).alias("latencia_fuera_de_rango"),
    F.sum(F.col("downlink_mbps").isNull().cast("int")).alias("downlink_nulo"),
    (F.count("*") - F.countDistinct("measurement_id")).alias("ids_duplicados"),
)
display(quality_preview)

# COMMAND ----------

metrics_df.createOrReplaceTempView("csv_cell_tower_metrics")
subscriptions_df.createOrReplaceTempView("csv_subscriptions")
usage_df.createOrReplaceTempView("csv_usage_daily")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   date_trunc('hour', event_ts) AS hora,
# MAGIC   count(*) AS mediciones,
# MAGIC   round(avg(latency_ms), 1) AS latencia_media_ms,
# MAGIC   round(avg(downlink_mbps), 1) AS downlink_medio_mbps
# MAGIC FROM csv_cell_tower_metrics
# MAGIC GROUP BY date_trunc('hour', event_ts)
# MAGIC ORDER BY hora
# MAGIC LIMIT 24;
