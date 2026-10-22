# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — muestra comparativa con PySpark imperativo
# MAGIC
# MAGIC Este notebook **no es una contingencia del flujo principal**. Su único objetivo
# MAGIC es comparar una transformación Spark imperativa con Lakeflow Spark Declarative
# MAGIC Pipelines (SDP) usando una muestra pequeña y determinista.
# MAGIC
# MAGIC Crea solamente tres tablas descartables con prefijo `demo_spark_`. DQX, Gold,
# MAGIC Metric Views, Dashboard, Genie y App no consumen estos objetos. Si SDP no está
# MAGIC disponible, el instructor utiliza checkpoints preparados; nunca continúa el
# MAGIC workshop desde estas tablas de demostración.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

dbutils.widgets.text("network_sample_rows", "2000", "Filas de red")
dbutils.widgets.text("customer_sample_rows", "3000", "Filas de cliente/producto")

network_sample_rows = max(100, min(int(dbutils.widgets.get("network_sample_rows")), 5000))
customer_sample_rows = max(100, min(int(dbutils.widgets.get("customer_sample_rows")), 5000))
raw_root = f"/Volumes/{catalog}/{schema}/raw_data"

DEMO_TABLES = {
    "network": f"{catalog}.{schema}.demo_spark_network_sample",
    "customer_product": f"{catalog}.{schema}.demo_spark_customer_product_sample",
    "kpis": f"{catalog}.{schema}.demo_spark_kpis",
}

# COMMAND ----------

from pyspark.sql import functions as F


def read_csv(dataset_name: str):
    """Lee una fuente solo para esta comparación; no materializa Bronze."""

    return (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(f"{raw_root}/{dataset_name}")
    )


sites = read_csv("network_sites").dropDuplicates(["site_id"])
cells = read_csv("radio_cells").dropDuplicates(["cell_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Muestra 1 — red
# MAGIC
# MAGIC El autor controla explícitamente lectura, límite, orden, joins, columnas y
# MAGIC escritura. SDP declara estas relaciones y las presenta como un DAG observable.

# COMMAND ----------

network_sample = (
    read_csv("cell_tower_metrics")
    .orderBy("event_ts", "measurement_id")
    .limit(network_sample_rows)
    .dropDuplicates(["measurement_id"])
    .alias("m")
    .join(cells.alias("c"), F.col("m.cell_id") == F.col("c.cell_id"), "left")
    .join(sites.alias("s"), F.col("c.site_id") == F.col("s.site_id"), "left")
    .select(
        "m.measurement_id",
        "m.cell_id",
        "m.event_ts",
        F.to_date("m.event_ts").alias("event_date"),
        "m.availability_pct",
        "m.latency_ms",
        "m.downlink_mbps",
        "m.active_users",
        "m.data_traffic_gb",
        F.col("c.site_id").alias("site_id"),
        "c.technology",
        "c.frequency_band",
        "s.region",
        "s.canton",
        "s.environment",
        "s.natural_region",
    )
    .withColumn(
        "network_status",
        F.when(
            (F.col("availability_pct") < 97)
            | (F.col("latency_ms") > 120)
            | (F.col("downlink_mbps") < 15),
            "Crítico",
        )
        .when(
            (F.col("availability_pct") < 99)
            | (F.col("latency_ms") > 80)
            | (F.col("downlink_mbps") < 30),
            "Atención",
        )
        .otherwise("Saludable"),
    )
)

network_sample.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(
    DEMO_TABLES["network"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Muestra 2 — cliente y producto
# MAGIC
# MAGIC Se usan únicamente IDs sintéticos y atributos agregables. No hay nombres,
# MAGIC teléfonos, cédulas, correos ni direcciones.

# COMMAND ----------

customers = read_csv("customers").dropDuplicates(["customer_id"])
products = read_csv("products").dropDuplicates(["product_id"])
subscriptions = read_csv("subscriptions").dropDuplicates(["subscription_id"])

customer_product_sample = (
    read_csv("usage_daily")
    .orderBy("usage_date", "subscription_id")
    .limit(customer_sample_rows)
    .dropDuplicates(["usage_date", "subscription_id"])
    .alias("u")
    .join(subscriptions.alias("sub"), "subscription_id", "left")
    .join(customers.alias("cus"), "customer_id", "left")
    .join(products.alias("pro"), "product_id", "left")
    .join(cells.alias("c"), F.col("u.primary_cell_id") == F.col("c.cell_id"), "left")
    .join(sites.alias("s"), F.col("c.site_id") == F.col("s.site_id"), "left")
    .select(
        "u.usage_date",
        "sub.customer_id",
        "u.subscription_id",
        "sub.product_id",
        "pro.product_name",
        "pro.product_family",
        "cus.customer_type",
        "cus.customer_segment",
        F.col("cus.region").alias("customer_region"),
        "u.data_gb",
        "u.voice_minutes",
        "u.billed_amount_usd",
        "u.recharge_amount_usd",
        F.col("c.site_id").alias("primary_site_id"),
        "c.technology",
        "c.frequency_band",
        F.col("s.region").alias("network_region"),
    )
)

customer_product_sample.write.mode("overwrite").option(
    "overwriteSchema", True
).saveAsTable(DEMO_TABLES["customer_product"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Muestra 3 — KPI comparativos
# MAGIC
# MAGIC Esta tabla permite verificar rápidamente el resultado del código imperativo.
# MAGIC No representa la capa Gold certificada.

# COMMAND ----------

network_kpis = (
    network_sample.groupBy("region")
    .agg(
        F.count("*").alias("sample_records"),
        F.round(F.avg("latency_ms"), 2).alias("avg_latency_ms"),
        F.round(F.sum("data_traffic_gb"), 2).alias("total_data_gb"),
    )
    .select(
        F.lit("Red").alias("domain"),
        F.col("region"),
        "sample_records",
        "avg_latency_ms",
        "total_data_gb",
        F.lit(None).cast("double").alias("billed_revenue_usd"),
    )
)

customer_kpis = (
    customer_product_sample.groupBy("customer_region")
    .agg(
        F.count("*").alias("sample_records"),
        F.round(F.sum("data_gb"), 2).alias("total_data_gb"),
        F.round(F.sum("billed_amount_usd"), 2).alias("billed_revenue_usd"),
    )
    .select(
        F.lit("Cliente y producto").alias("domain"),
        F.col("customer_region").alias("region"),
        "sample_records",
        F.lit(None).cast("double").alias("avg_latency_ms"),
        "total_data_gb",
        "billed_revenue_usd",
    )
)

demo_kpis = network_kpis.unionByName(customer_kpis)
demo_kpis.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(
    DEMO_TABLES["kpis"]
)

display(demo_kpis.orderBy("domain", "region"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recapitulación
# MAGIC
# MAGIC | Capacidad | SDP | PySpark imperativo de esta muestra |
# MAGIC |---|---|---|
# MAGIC | DAG y orden | Derivados de dependencias declaradas | Coordinados por el autor |
# MAGIC | Ingesta incremental | Auto Loader administrado | Lectura batch limitada |
# MAGIC | Calidad | Expectations y métricas del pipeline | No implementada en la muestra |
# MAGIC | Reintentos | Por flujo y dataset | Rerun completo |
# MAGIC | Lineage | Automático en Unity Catalog | Limitado a tres escrituras |
# MAGIC | Observabilidad | Event log y UI del pipeline | Resultado de celdas |
# MAGIC | Uso downstream | Flujo oficial del workshop | **Ninguno** |
# MAGIC
# MAGIC Objetos creados:
# MAGIC
# MAGIC - `demo_spark_network_sample`
# MAGIC - `demo_spark_customer_product_sample`
# MAGIC - `demo_spark_kpis`
# MAGIC
# MAGIC Son descartables y pueden eliminarse después de la comparación. No modifique
# MAGIC DQX ni SQL para que los consuman.
