# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Configuración del participante
# MAGIC
# MAGIC Este es el único lugar donde escribe algo: **el catálogo asignado**. El resto se
# MAGIC calcula a partir de su usuario, para que cada participante tenga su propio schema,
# MAGIC job, pipeline, dashboard, Genie Space y App sin chocar con los demás.
# MAGIC
# MAGIC Todo el workshop usa el **SDK de Databricks para Python** desde notebooks. La
# MAGIC autenticación es la del propio notebook: no se necesitan CLI, tokens ni perfiles.
# MAGIC
# MAGIC **Compute:** use *Serverless* en todos los notebooks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Actualizar el SDK
# MAGIC La documentación recomienda instalar la versión más reciente y reiniciar Python.
# MAGIC Las APIs de Genie y Apps que usaremos requieren un SDK reciente.

# COMMAND ----------

# MAGIC %pip install --upgrade --quiet databricks-sdk

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Parámetros
# MAGIC Complete **catalog** en el widget superior. Deje `warehouse_id` vacío para elegir
# MAGIC automáticamente un SQL Warehouse PRO o Serverless.

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Catálogo asignado")
dbutils.widgets.text("schema", "", "Schema (vacío = automático)")
dbutils.widgets.text("warehouse_id", "", "SQL Warehouse ID (vacío = automático)")

# COMMAND ----------

import sys
from pathlib import Path

import databricks.sdk
from databricks.sdk import WorkspaceClient

repo_root = next(
    candidate
    for candidate in (Path.cwd(), *Path.cwd().parents)
    if (candidate / "pyproject.toml").exists() and (candidate / "notebooks").is_dir()
)
sys.path.insert(0, str(repo_root / "src"))

from telco_workshop import workshop

w = WorkspaceClient()
user_name = w.current_user.me().user_name
slug = workshop.slugify(user_name)

catalog = workshop.validate_identifier(dbutils.widgets.get("catalog").strip(), "Catálogo")
schema = workshop.validate_identifier(
    dbutils.widgets.get("schema").strip() or f"{workshop.PREFIX}_{slug}", "Schema"
)
warehouse = workshop.choose_warehouse(w, dbutils.widgets.get("warehouse_id").strip())

print(f"SDK de Databricks: {databricks.sdk.version.__version__}")
print(f"Usuario:           {user_name}")
print(f"Repositorio:       {repo_root}")
print(f"Warehouse:         {warehouse.name} ({warehouse.id}) - {warehouse.state.value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Validar permisos y servicios
# MAGIC Si una verificación falla, avise al instructor antes de continuar.

# COMMAND ----------

checks = []


def check(name, function):
    try:
        detail = function()
        checks.append((name, "OK", str(detail or "")))
    except Exception as error:  # noqa: BLE001 - se muestra el motivo al participante
        checks.append((name, "FALLA", str(error).splitlines()[0][:200]))


check("Catálogo visible", lambda: w.catalogs.get(catalog).name)


def schema_permission():
    catalog_info = w.catalogs.get(catalog)
    if catalog_info.owner == user_name:
        return "owner del catálogo"
    effective = w.grants.get_effective("catalog", catalog, principal=user_name)
    privileges = {
        item.privilege.value
        for assignment in effective.privilege_assignments or []
        for item in assignment.privileges or []
    }
    granted = sorted(privileges & {"ALL_PRIVILEGES", "USE_CATALOG", "CREATE_SCHEMA"})
    if "ALL_PRIVILEGES" not in granted and not {"USE_CATALOG", "CREATE_SCHEMA"} <= set(granted):
        raise PermissionError(f"Faltan USE CATALOG y CREATE SCHEMA (tiene: {granted or 'ninguno'})")
    return ", ".join(granted)


check("Permiso para crear schemas", schema_permission)
check("SQL Warehouse PRO/Serverless", lambda: f"{warehouse.name}")
check("API de Pipelines", lambda: next(iter(w.pipelines.list_pipelines(max_results=1)), True) and "")
check("API de Databricks Apps", lambda: next(iter(w.apps.list(page_size=1)), True) and "")
check("API de Genie", lambda: w.genie.list_spaces(page_size=1) and "")
check("API de AI/BI Dashboards", lambda: next(iter(w.lakeview.list(page_size=1)), True) and "")

display(spark.createDataFrame(checks, "verificacion string, estado string, detalle string"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Guardar la configuración
# MAGIC Se guarda en su carpeta personal (`.z2h/config.json`). Los demás notebooks la leen
# MAGIC con `%run ./_config`. Si cambia el catálogo, vuelva a ejecutar este notebook.

# COMMAND ----------

cfg = workshop.WorkshopConfig(
    user_name=user_name,
    slug=slug,
    catalog=catalog,
    schema=schema,
    warehouse_id=warehouse.id,
    repo_root=str(repo_root),
)
saved_path = workshop.save_config(w, cfg)

display(
    spark.createDataFrame(
        [
            ("Schema", cfg.namespace),
            ("Volume", cfg.volume_root),
            ("Job de datos", cfg.job_name),
            ("Pipeline", cfg.pipeline_name),
            ("Dashboard de referencia", cfg.dashboard_name),
            ("Genie Space", cfg.genie_title),
            ("App", cfg.app_name),
            ("Configuración", saved_path),
        ],
        "asset string, nombre string",
    )
)
