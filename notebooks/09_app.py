# Databricks notebook source
# MAGIC %md
# MAGIC # 09 — Databricks App inicial
# MAGIC
# MAGIC La App Streamlit de `apps/network-monitor` consulta sus tablas Gold a través del
# MAGIC SQL Warehouse. Este notebook la crea y la despliega con el SDK:
# MAGIC
# MAGIC 1. **Recursos**: la App recibe un *service principal* propio. Declarar el warehouse
# MAGIC    (`CAN_USE`) y las cinco Gold (`SELECT`) como recursos le concede esos permisos.
# MAGIC 2. **Código**: se copia a su carpeta personal junto con un `app.yaml` generado con
# MAGIC    su catálogo y schema.
# MAGIC 3. **Deployment**: `deploy_and_wait` instala dependencias y arranca Streamlit.
# MAGIC
# MAGIC ⚠️ Una App consume compute mientras está encendida. Deténgala al terminar.

# COMMAND ----------

# MAGIC %pip install --upgrade --quiet databricks-sdk

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Crear la App con sus recursos (2–4 minutos la primera vez)

# COMMAND ----------

for resource in workshop.app_resources(cfg):
    target = resource.sql_warehouse or resource.uc_securable
    print(f"{resource.name:30s} → {getattr(target, 'securable_full_name', None) or target.id}")

app = workshop.upsert_app(w, cfg)
print(f"\nApp: {app.name} | compute: {app.compute_status.state.value}")
print(f"Service principal: {app.service_principal_client_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Preparar el código y desplegar
# MAGIC Observe el `app.yaml` generado: `valueFrom: sql-warehouse` toma el ID del recurso
# MAGIC declarado; catálogo y schema llegan como variables de entorno.

# COMMAND ----------

source_code_path = workshop.stage_app_source(w, cfg)
print(workshop.app_yaml(cfg))

deployment = workshop.deploy_app(w, cfg, source_code_path)
print(f"Deployment {deployment.deployment_id}: {deployment.status.state.value} - {deployment.status.message}")
show_link("Abrir la App", w.apps.get(cfg.app_name).url)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Modificar y redesplegar
# MAGIC 1. Abra `apps/network-monitor/app.py` en el Git folder.
# MAGIC 2. Cambie el título de `st.set_page_config` o agregue una cuarta métrica.
# MAGIC 3. Vuelva a ejecutar la sección 2 y recargue la App.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Detener la App al terminar

# COMMAND ----------

# w.apps.stop_and_wait(cfg.app_name)
