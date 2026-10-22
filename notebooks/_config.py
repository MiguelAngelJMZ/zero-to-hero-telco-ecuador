# Databricks notebook source
# MAGIC %md
# MAGIC # Configuración compartida (no lo ejecute directamente)
# MAGIC
# MAGIC Cada notebook del workshop lo carga con `%run ./_config`. Lee la configuración que
# MAGIC guardó `00_configuracion` y deja disponibles:
# MAGIC
# MAGIC - `w`: `WorkspaceClient()` autenticado con la identidad del notebook.
# MAGIC - `cfg`: catálogo, schema, warehouse y nombres de los assets del participante.
# MAGIC - `catalog`, `schema`, `warehouse_id`: atajos para SQL y PySpark.
# MAGIC - `show_link(texto, url)`: muestra un enlace al asset creado.

# COMMAND ----------

import sys
from pathlib import Path

_repo_root = next(
    candidate
    for candidate in (Path.cwd(), *Path.cwd().parents)
    if (candidate / "pyproject.toml").exists() and (candidate / "notebooks").is_dir()
)
if str(_repo_root / "src") not in sys.path:
    sys.path.insert(0, str(_repo_root / "src"))

from databricks.sdk import WorkspaceClient

from telco_workshop import workshop

w = WorkspaceClient()
cfg = workshop.load_config(w, spark.sql("SELECT current_user()").first()[0])
catalog, schema, warehouse_id = cfg.catalog, cfg.schema, cfg.warehouse_id

spark.sql(f"USE CATALOG `{catalog}`")
if spark.catalog.databaseExists(f"{catalog}.{schema}"):
    spark.sql(f"USE SCHEMA `{schema}`")


def show_link(label, url):
    displayHTML(f'<p style="font-size:16px">👉 <a href="{url}" target="_blank">{label}</a></p>')


print(f"Participante: {cfg.slug} | Schema: {cfg.namespace} | Warehouse: {warehouse_id}")
