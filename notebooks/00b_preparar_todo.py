# Databricks notebook source
# MAGIC %md
# MAGIC # 00b — Preparar todo en una corrida (formato de 80 minutos)
# MAGIC
# MAGIC En el formato corto, los módulos 01 a 06 se ejecutan como **un solo job** que usted
# MAGIC lanza con **Run all**. Mientras corre (unos 15 minutos), el instructor explica Unity
# MAGIC Catalog, Lakeflow, DQX y Metric Views, y usted sigue el avance de **su** job y de
# MAGIC **su** pipeline en la interfaz.
# MAGIC
# MAGIC | Tarea del job | Equivale a |
# MAGIC |---|---|
# MAGIC | `datos_sinteticos` | `02_datos` |
# MAGIC | `pipeline_lakeflow` | `03_pipeline` + **Run pipeline** |
# MAGIC | `calidad_dqx` | `04_dqx_quality` |
# MAGIC | `vistas_gold` | `05_sql_queries` |
# MAGIC | `metric_views` | `06_metric_views` |
# MAGIC
# MAGIC Los notebooks 01 a 06 siguen en el repositorio para estudiarlos después del taller.
# MAGIC
# MAGIC **Requisito:** haber ejecutado `00_configuracion`. **Compute:** Serverless.

# COMMAND ----------

# MAGIC %pip install --upgrade --quiet databricks-sdk

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Schema y volume
# MAGIC Lo mismo que hace `01_unity_catalog`: su schema y el volume `raw_data` donde
# MAGIC aterrizan los CSV.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}` COMMENT 'Workshop Zero to Hero - {cfg.slug}'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`raw_data` COMMENT 'CSV sintéticos del workshop'")
print(f"Schema listo: {cfg.namespace} | Volume: {cfg.volume_root}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Crear el pipeline y el job orquestado
# MAGIC Ambos se crean con el SDK. Si vuelve a ejecutar la celda, se actualizan en lugar
# MAGIC de duplicarse.

# COMMAND ----------

pipeline_id = workshop.upsert_pipeline(w, cfg)
job_id = workshop.upsert_prepare_job(w, cfg, pipeline_id)

for task in w.jobs.get(job_id).settings.tasks:
    after = ", ".join(dependency.task_key for dependency in task.depends_on or []) or "inicio"
    print(f"{task.task_key:20s} ← {after}")

show_link("Abrir el pipeline (verá el DAG cuando llegue su tarea)", workshop.workspace_url(w, f"pipelines/{pipeline_id}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Lanzar el job
# MAGIC Si ya hay una corrida en curso, no se lanza otra: se muestra la existente.

# COMMAND ----------

active_run = next(iter(w.jobs.list_runs(job_id=job_id, active_only=True)), None)
run_id = active_run.run_id if active_run else w.jobs.run_now(job_id=job_id).response.run_id
show_link("Abrir su corrida y seguir el avance", w.jobs.get_run(run_id).run_page_url)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Estado de la corrida
# MAGIC Vuelva a ejecutar esta celda cuando quiera. Termina en unos 15 minutos.

# COMMAND ----------

run = w.jobs.get_run(run_id)
for task in run.tasks or []:
    state = task.state
    result = state.result_state.value if state and state.result_state else ""
    print(f"{task.task_key:20s} {state.life_cycle_state.value if state else '':12s} {result}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Verificación
# MAGIC Cuando las cinco tareas estén en `SUCCESS`, vuelva a ejecutar esta celda. Debe ver
# MAGIC 23 objetos del pipeline, 6 tablas validated, 5 Gold y 2 Metric Views.
# MAGIC Luego continúe con `07_dashboard`.

# COMMAND ----------

objects = spark.sql(f"SHOW TABLES IN `{catalog}`.`{schema}`").select("tableName").toPandas()["tableName"]
summary = [
    ("Pipeline (Bronze, Silver, Gold preliminar)", 23,
     int(objects.str.endswith(("_bronze", "_silver")).sum() + (objects == "gold_network_hourly_pipeline").sum())),
    ("DQX validated", 6, int(objects.str.endswith("_validated").sum())),
    ("Gold", 5, int(objects.isin(workshop.GOLD_OBJECTS).sum())),
    ("Metric Views", 2, int(objects.isin(["mv_network_quality", "mv_customer_product_experience"]).sum())),
]
display(spark.createDataFrame(
    [(name, expected, found, "OK" if found == expected else "pendiente") for name, expected, found in summary],
    "etapa string, esperado int, encontrado int, estado string",
))
