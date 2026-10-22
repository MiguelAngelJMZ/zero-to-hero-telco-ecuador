# Databricks notebook source
# MAGIC %md
# MAGIC # 07 — AI/BI Dashboard
# MAGIC
# MAGIC El dashboard es la **principal construcción visual** del workshop. Hay tres caminos:
# MAGIC
# MAGIC | Camino | Cuándo usarlo |
# MAGIC |---|---|
# MAGIC | **A. Construirlo en la interfaz** con `docs/DASHBOARD_GUIDE.md` | Ejercicio principal |
# MAGIC | **B. Pedírselo a Genie Code** con el prompt de la sección 2 | Mostrar desarrollo asistido por IA |
# MAGIC | **C. Crear el de referencia con el SDK** (sección 3) | Contingencia o punto de comparación |
# MAGIC
# MAGIC Requisito: `05_sql_queries` y `06_metric_views` terminaron correctamente.

# COMMAND ----------

# MAGIC %pip install --upgrade --quiet databricks-sdk

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Verificar los objetos de origen

# COMMAND ----------

required = [*workshop.GOLD_OBJECTS, "mv_network_quality", "mv_customer_product_experience"]
existing = {row.tableName for row in spark.sql(f"SHOW TABLES IN `{catalog}`.`{schema}`").collect()}
missing = [name for name in required if name not in existing]
assert not missing, f"Faltan objetos; ejecute 05 y 06: {missing}"
print("Objetos Gold y Metric Views disponibles.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Camino B — pedírselo a Genie Code
# MAGIC El prompt no incluye SQL ni diseño de widgets: describe **qué preguntas de negocio**
# MAGIC debe responder el dashboard y **qué datos son confiables**. Genie Code explora las
# MAGIC tablas, propone los datasets y arma el canvas.
# MAGIC
# MAGIC 1. Abra **+ New → Dashboard** y el panel de **Genie Code** dentro del editor.
# MAGIC 2. Copie el prompt de abajo (ya contiene su catálogo y schema) y envíelo.
# MAGIC 3. Lea el plan que propone antes de que lo construya; revise el SQL de cada dataset.
# MAGIC 4. Itere con las preguntas de seguimiento o con las suyas.
# MAGIC 5. Compare el resultado con el dashboard de referencia de la sección 3.

# COMMAND ----------

from telco_workshop.dashboard import GENIE_CODE_FOLLOW_UPS, build_serialized_dashboard, genie_code_prompt

print(genie_code_prompt(cfg.namespace, f"Experiencia móvil 360 - {cfg.slug}", warehouse_id))
print("Preguntas de seguimiento:")
for follow_up in GENIE_CODE_FOLLOW_UPS:
    print(f"- {follow_up}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Camino C — dashboard de referencia con el SDK
# MAGIC `w.lakeview.create(...)` recibe la definición serializada del dashboard (datasets,
# MAGIC widgets y posiciones) y `w.lakeview.publish(...)` lo publica con las credenciales
# MAGIC del autor. Si ya existe, se actualiza.

# COMMAND ----------

serialized = build_serialized_dashboard(catalog, schema)
print(f"Datasets: {[dataset['displayName'] for dataset in serialized['datasets']]}")
print(f"Widgets:  {len(serialized['pages'][0]['layout'])}")

dashboard_id = workshop.upsert_dashboard(w, cfg, serialized, publish=True)
dashboard_url = workshop.workspace_url(w, f"dashboardsv3/{dashboard_id}")
show_link("Abrir el dashboard de referencia (borrador)", dashboard_url)
show_link("Abrir la versión publicada", f"{dashboard_url}/published")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificación
# MAGIC - Cambiar la provincia actualiza KPI, gráficos y tabla aplicables.
# MAGIC - Las medidas coinciden con una consulta `MEASURE()` en el SQL Editor.
# MAGIC - El dashboard no consulta Bronze, CSV ni cuarentena.
