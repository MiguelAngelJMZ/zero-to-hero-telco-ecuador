# Databricks notebook source
# MAGIC %md
# MAGIC # 08 — Genie Space
# MAGIC
# MAGIC Un Genie Space responde preguntas en lenguaje natural generando SQL sobre datos
# MAGIC **curados**: fuentes confiables, instrucciones, sinónimos, ejemplos SQL y
# MAGIC benchmarks. Aquí crea **su propio** Genie Space sobre su schema con el SDK
# MAGIC (`w.genie.create_space`) y después lo mejora.
# MAGIC
# MAGIC Requisitos: SQL Warehouse PRO o Serverless y las Metric Views del módulo 06.

# COMMAND ----------

# MAGIC %pip install --upgrade --quiet databricks-sdk

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Revisar la configuración del espacio
# MAGIC `serialized_space` es un JSON versionado. Observe sus partes antes de crearlo.

# COMMAND ----------

import json

from telco_workshop.genie import build_serialized_space

space = build_serialized_space(catalog, schema)

print("Fuentes (Metric Views):", [item["identifier"] for item in space["data_sources"]["metric_views"]])
print("Fuentes (Gold):        ", [item["identifier"] for item in space["data_sources"]["tables"]])
print("Preguntas de ejemplo:  ", [item["question"][0] for item in space["config"]["sample_questions"]])
print("Ejemplos SQL:          ", len(space["instructions"]["example_question_sqls"]))
print("Benchmarks:            ", len(space["benchmarks"]["questions"]))
print("\nInstrucciones:\n", space["instructions"]["text_instructions"][0]["content"][0])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Crear (o actualizar) el Genie Space

# COMMAND ----------

space_id = workshop.upsert_genie_space(w, cfg, space)
genie_url = workshop.workspace_url(w, f"genie/rooms/{space_id}")
print(f"Genie Space: {cfg.genie_title} ({space_id})")
show_link("Abrir el Genie Space", genie_url)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Probar en modo Chat
# MAGIC En el Genie Space, haga estas dos preguntas y pulse **Show code** para revisar el SQL:
# MAGIC
# MAGIC 1. *Compara disponibilidad, latencia, throughput y cumplimiento del SLA por provincia y
# MAGIC    tecnología durante los siete días de telemetría. Muéstrame las tres combinaciones
# MAGIC    con peor desempeño.*
# MAGIC 2. *¿Qué cinco productos tienen mayor ARPU y cómo se comparan en consumo de datos,
# MAGIC    tickets por cada 100 líneas activas, NPS y cumplimiento del SLA de red?*
# MAGIC
# MAGIC Una respuesta fluida no exime de verificar el SQL. Si el modo **Deep Research** está
# MAGIC habilitado en su workspace, pruebe las preguntas de `config/genie_space_instructions.md`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Mejorar el espacio con el SDK
# MAGIC Agregue una instrucción propia y vuelva a ejecutar la celda: `update_space`
# MAGIC reemplaza la configuración. Después repita una pregunta y compare la respuesta.

# COMMAND ----------

mi_instruccion = "Cuando muestres montos en USD, usa el símbolo $ y dos decimales."

improved = json.loads(json.dumps(space))
improved["instructions"]["text_instructions"][0]["content"][0] += " " + mi_instruccion
workshop.upsert_genie_space(w, cfg, improved, space_id=space_id)

current = w.genie.get_space(space_id, include_serialized_space=True)
print(json.loads(current.serialized_space)["instructions"]["text_instructions"][0]["content"][0][-200:])
show_link("Volver al Genie Space", genie_url)
