# Databricks notebook source
# MAGIC %md
# MAGIC # 08 — Genie Space
# MAGIC
# MAGIC Un Genie Space responde preguntas en lenguaje natural generando SQL sobre datos
# MAGIC **curados**. Aquí crea **su propio** Genie Space sobre su schema con el SDK
# MAGIC (`w.genie.create_space`) y después lo mejora.
# MAGIC
# MAGIC **Cómo se cura un Genie Space (buenas prácticas de Databricks):** el significado de
# MAGIC los datos se define en superficies estructuradas y las instrucciones de texto son el
# MAGIC último recurso.
# MAGIC
# MAGIC | Qué necesita saber Genie | Dónde se define en este espacio |
# MAGIC |---|---|
# MAGIC | Para qué sirve cada tabla o Metric View | Descripción de la fuente |
# MAGIC | Cómo llama el negocio a cada columna ("SLA", "provincia", "churn") | Sinónimos por columna |
# MAGIC | Cómo se calcula un KPI | Medidas de las Metric Views (módulo 06) |
# MAGIC | Cómo se escribe una consulta típica | SQL de ejemplo |
# MAGIC | Cómo comportarse: período, ambigüedades, privacidad, formato | Instrucciones de texto (breves) |
# MAGIC | Si responde bien | Benchmarks |
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
# MAGIC `serialized_space` es un JSON versionado. Observe cada superficie antes de crearlo.

# COMMAND ----------

import json

from telco_workshop.genie import build_serialized_space

space = build_serialized_space(catalog, schema)
sources = space["data_sources"]["metric_views"] + space["data_sources"]["tables"]

print("FUENTES Y SINÓNIMOS")
for item in sources:
    print(f"\n• {item['identifier'].split('.')[-1]}: {item['description'][0]}")
    for column in item["column_configs"]:
        print(f"    {column['column_name']:32s} ← {', '.join(column.get('synonyms', []))}")

print("\nSQL DE EJEMPLO")
for example in space["instructions"]["example_question_sqls"]:
    print(f"  - {example['question'][0]}")

print("\nBENCHMARKS")
for benchmark in space["benchmarks"]["questions"]:
    print(f"  - {benchmark['question'][0]}")

instructions = space["instructions"]["text_instructions"][0]["content"][0]
print(f"\nINSTRUCCIONES DE TEXTO ({len(instructions)} caracteres)\n")
print(instructions)

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
# MAGIC Una respuesta fluida no exime de verificar el SQL. En **Benchmarks** puede ejecutar
# MAGIC ambas preguntas y comparar con el SQL verificado. Si el modo **Deep Research** está
# MAGIC habilitado en su workspace, pruebe las preguntas de `config/genie_space_instructions.md`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Mejorar el espacio (reto 6)
# MAGIC Cuando encuentre una respuesta con un error de interpretación, agregue una regla a
# MAGIC `instrucciones_extra` y vuelva a ejecutar la celda. Las reglas se insertan en la
# MAGIC sección `DISAMBIGUATION`, antes del bloque de resúmenes, que debe ir siempre al final.
# MAGIC
# MAGIC Buenas reglas: una idea por regla, concretas y con el término que confunde a Genie.
# MAGIC Si la regla define un cálculo o una columna, es mejor resolverla con un sinónimo o un
# MAGIC SQL de ejemplo (también puede editarlos en la interfaz: **Configure → Data**).

# COMMAND ----------

instrucciones_extra = [
    # Ejemplo de formato (reemplácelo por la regla que necesite su reto):
    # "Cuando pregunten por 'la red' sin indicar tecnología, incluye 4G y 5G juntos.",
]

improved = build_serialized_space(catalog, schema, instrucciones_extra)
workshop.upsert_genie_space(w, cfg, improved, space_id=space_id)

current = w.genie.get_space(space_id, include_serialized_space=True)
saved = json.loads(current.serialized_space)["instructions"]["text_instructions"][0]["content"][0]
print(saved.split("## DATA QUALITY NOTES")[0])
show_link("Volver al Genie Space y repetir la pregunta", genie_url)
