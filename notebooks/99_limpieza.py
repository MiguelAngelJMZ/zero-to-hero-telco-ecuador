# Databricks notebook source
# MAGIC %md
# MAGIC # 99 — Limpieza
# MAGIC
# MAGIC Elimina **solo sus assets**: App (y su código desplegado), Genie Space, dashboard de referencia, pipeline,
# MAGIC job y su schema (con todas sus tablas y el volume). No toca el catálogo ni los
# MAGIC objetos de otros participantes, ni el Git folder. También borra su configuración (`.z2h`).
# MAGIC Escriba `BORRAR` en el widget para confirmar.

# COMMAND ----------

# MAGIC %pip install --upgrade --quiet databricks-sdk

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

dbutils.widgets.text("confirmar", "", "Escriba BORRAR")
assert dbutils.widgets.get("confirmar") == "BORRAR", "Escriba BORRAR en el widget 'confirmar' para continuar."

# COMMAND ----------

from databricks.sdk.errors import NotFound

results = []


def remove(asset, identifier, delete):
    if not identifier:
        results.append((asset, "no existía"))
        return
    try:
        delete(identifier)
        results.append((asset, "eliminado"))
    except NotFound:
        results.append((asset, "no existía"))


try:
    app_name = w.apps.get(cfg.app_name).name
except NotFound:
    app_name = None
remove(f"App {cfg.app_name}", app_name, w.apps.delete)

remove(f"Genie {cfg.genie_title}", workshop.find_genie_space_id(w, cfg.genie_title), w.genie.trash_space)

dashboard_id = next(
    (item.dashboard_id for item in w.lakeview.list() if item.display_name == cfg.dashboard_name), None
)
remove(f"Dashboard {cfg.dashboard_name}", dashboard_id, w.lakeview.trash)

remove(f"Pipeline {cfg.pipeline_name}", workshop.find_pipeline_id(w, cfg.pipeline_name), w.pipelines.delete)

job_id = next((job.job_id for job in w.jobs.list(name=cfg.job_name)), None)
remove(f"Job {cfg.job_name}", job_id, w.jobs.delete)

spark.sql(f"DROP SCHEMA IF EXISTS `{catalog}`.`{schema}` CASCADE")
results.append((f"Schema {cfg.namespace}", "eliminado"))

# Código desplegado de la App y configuración personal (`00_configuracion` la recrea).
for path in (cfg.app_deploy_path, workshop.config_path(cfg.user_name).rsplit("/", 1)[0]):
    remove(f"Carpeta {path}", path, lambda folder: w.workspace.delete(folder, recursive=True))

display(spark.createDataFrame(results, "asset string, resultado string"))
