# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Lakeflow Spark Declarative Pipelines
# MAGIC
# MAGIC Este notebook **crea** su pipeline con el SDK, pero **no lo ejecuta**. Usted lo
# MAGIC inicia desde el editor de Lakeflow para ver cómo se construye el DAG
# MAGIC Bronze → Silver → Gold, las expectations y las métricas de cada tabla.
# MAGIC
# MAGIC El código del pipeline está en `pipeline/transformations/telco_pipeline.py`.
# MAGIC Ábralo en paralelo: es un archivo Python con decoradores `@dp.table` y
# MAGIC `@dp.materialized_view`; SDP infiere las dependencias a partir de las lecturas.

# COMMAND ----------

# MAGIC %pip install --upgrade --quiet databricks-sdk

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Definición del pipeline
# MAGIC - `catalog` y `schema`: destino por defecto de todas las tablas del pipeline.
# MAGIC - `serverless=True`: no hay clusters que configurar.
# MAGIC - `development=True`: modo desarrollo, ideal para iterar.
# MAGIC - `continuous=False`: se ejecuta por demanda (*triggered*), no queda encendido.
# MAGIC - `configuration`: parámetros que el código lee con `spark.conf.get(...)`.

# COMMAND ----------

pipeline_id = workshop.upsert_pipeline(w, cfg)
pipeline = w.pipelines.get(pipeline_id)
spec = pipeline.spec
print(f"Pipeline:   {pipeline.name} ({pipeline_id})")
print(f"Destino:    {spec.catalog}.{spec.schema}")
print(f"Serverless: {spec.serverless} | Desarrollo: {spec.development} | Continuo: {spec.continuous}")
print(f"Código:     {spec.libraries[0].file.path}")
print(f"Estado:     {pipeline.state.value if pipeline.state else 'sin ejecuciones'}")
show_link("Abrir el pipeline y pulsar Run pipeline", workshop.workspace_url(w, f"pipelines/{pipeline_id}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ejecutarlo desde la interfaz
# MAGIC 1. Abra el enlace y pulse **Run pipeline** (la primera vez equivale a *full refresh*).
# MAGIC 2. Observe el DAG: once tablas Bronze (*streaming tables*), once Silver
# MAGIC    (*materialized views*) y `gold_network_hourly_pipeline`.
# MAGIC 3. Seleccione `cell_tower_metrics_silver` y revise **Data quality**: las
# MAGIC    expectations registran anomalías sin eliminar filas.
# MAGIC 4. Vuelva aquí y ejecute la sección 3.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verificar el resultado

# COMMAND ----------

latest = next(iter(w.pipelines.list_updates(pipeline_id=pipeline_id, max_results=1).updates or []), None)
print(f"Último update: {latest.state.value if latest else 'ninguno todavía'}")

tables = spark.sql(f"SHOW TABLES IN `{catalog}`.`{schema}`").where(
    "tableName LIKE '%\\_bronze' OR tableName LIKE '%\\_silver' OR tableName = 'gold_network_hourly_pipeline'"
)
display(tables)
print(f"Objetos del pipeline: {tables.count()} (esperado: 23)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Para practicar
# MAGIC Cambie un umbral de una expectation en `telco_pipeline.py` (por ejemplo
# MAGIC `latency_range`), guarde y vuelva a ejecutar el pipeline desde la interfaz. Observe
# MAGIC qué tablas se recalculan. Continúe con `04_dqx_quality`.
