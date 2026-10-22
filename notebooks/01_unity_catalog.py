# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Unity Catalog
# MAGIC Jerarquía **metastore → catálogo → schema → tabla / volume**, permisos y
# MAGIC descubrimiento. Cada participante crea **su propio schema** dentro del catálogo
# MAGIC asignado; el catálogo ya existe y no se crea desde el workshop.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. El catálogo asignado

# COMMAND ----------

display(spark.sql(f"DESCRIBE CATALOG EXTENDED `{catalog}`"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Crear su schema y el volume de aterrizaje
# MAGIC El volume `raw_data` recibirá los CSV que generará el job del módulo 02.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}` COMMENT 'Workshop Zero to Hero - {cfg.slug}'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`raw_data` COMMENT 'CSV sintéticos del workshop'")
spark.sql(f"USE SCHEMA `{schema}`")

display(spark.sql(f"SHOW SCHEMAS IN `{catalog}` LIKE '{schema}'"))
display(spark.sql("SHOW VOLUMES"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Permisos efectivos
# MAGIC Unity Catalog hereda permisos de arriba hacia abajo. Observe qué recibe en su schema.

# COMMAND ----------

display(spark.sql(f"SHOW GRANTS ON SCHEMA `{catalog}`.`{schema}`"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Mismo resultado con el SDK
# MAGIC SQL y SDK operan sobre los mismos objetos de Unity Catalog.

# COMMAND ----------

schema_info = w.schemas.get(cfg.namespace)
volume_info = w.volumes.read(f"{cfg.namespace}.raw_data")
print(f"Schema: {schema_info.full_name} | owner: {schema_info.owner}")
print(f"Volume: {volume_info.full_name} | tipo: {volume_info.volume_type.value}")
show_link("Abrir el schema en Catalog Explorer", workshop.workspace_url(w, f"explore/data/{catalog}/{schema}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificación
# MAGIC En Catalog Explorer debe ver `catálogo > su schema > volume raw_data`.
# MAGIC Continúe con `02_datos`.
