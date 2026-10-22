"""03 — Lakeflow Spark Declarative Pipelines.

Archivo fuente del pipeline; no es un notebook interactivo. `notebooks/03_pipeline`
crea el pipeline que lo usa y el participante lo ejecuta desde el editor de
Lakeflow. El DAG ingiere once fuentes con Auto Loader, construye Bronze y Silver y
publica una agregación Gold preliminar.

Observe qué administra SDP: dependencias, orden, incrementalidad, estado de Auto
Loader, reintentos, lineage, expectations y observabilidad. PySpark sigue siendo el
motor; SDP elimina gran parte de la coordinación imperativa.

`catalog` y `schema` llegan desde la configuración del pipeline; las tablas sin
calificar se publican en ese mismo catálogo y schema.
"""

from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

catalog = spark.conf.get("telco.catalog", "telco_workshop")
schema = spark.conf.get("telco.schema", "red_calidad")
raw_root = f"/Volumes/{catalog}/{schema}/raw_data"


def fields(*items):
    return StructType([StructField(name, data_type, True) for name, data_type in items])


def keep_latest(frame, keys, *ordering):
    """Deduplica de forma determinista para que SDP y el checkpoint produzcan lo mismo."""

    window = Window.partitionBy(*keys).orderBy(*ordering)
    return frame.withColumn("_dedup_rank", F.row_number().over(window)).where("_dedup_rank = 1").drop("_dedup_rank")


DATASET_SCHEMAS = {
    "network_sites": fields(
        ("site_id", StringType()), ("region", StringType()), ("canton", StringType()),
        ("latitude", DoubleType()), ("longitude", DoubleType()), ("environment", StringType()),
        ("natural_region", StringType()),
        ("site_type", StringType()), ("ownership_type", StringType()), ("commissioned_date", DateType()),
        ("backup_power_hours", DoubleType()), ("site_status", StringType()),
    ),
    "radio_cells": fields(
        ("cell_id", StringType()), ("site_id", StringType()), ("sector_number", IntegerType()),
        ("azimuth_degrees", IntegerType()), ("technology", StringType()), ("frequency_band", StringType()),
        ("bandwidth_mhz", IntegerType()), ("capacity_users", IntegerType()),
        ("vendor_family", StringType()), ("cell_status", StringType()),
    ),
    "cell_tower_metrics": fields(
        ("measurement_id", StringType()), ("cell_id", StringType()), ("event_ts", TimestampType()),
        ("availability_pct", DoubleType()), ("signal_strength_dbm", DoubleType()),
        ("sinr_db", DoubleType()), ("latency_ms", DoubleType()), ("downlink_mbps", DoubleType()),
        ("uplink_mbps", DoubleType()), ("packet_loss_pct", DoubleType()),
        ("dropped_calls", IntegerType()), ("active_users", IntegerType()),
        ("data_traffic_gb", DoubleType()), ("ingestion_batch_id", StringType()),
    ),
    "network_alarms": fields(
        ("alarm_id", StringType()), ("cell_id", StringType()), ("opened_at", TimestampType()),
        ("closed_at", TimestampType()), ("alarm_type", StringType()), ("severity", StringType()),
        ("alarm_status", StringType()), ("probable_cause", StringType()),
        ("service_impact", BooleanType()), ("affected_users_est", IntegerType()),
        ("source_system", StringType()),
    ),
    "maintenance_orders": fields(
        ("work_order_id", StringType()), ("site_id", StringType()), ("alarm_id", StringType()),
        ("created_at", TimestampType()), ("scheduled_at", TimestampType()),
        ("completed_at", TimestampType()), ("order_type", StringType()), ("priority", StringType()),
        ("order_status", StringType()), ("provider_code", StringType()),
        ("downtime_minutes", IntegerType()), ("cost_usd", DecimalType(12, 2)),
        ("sla_target_hours", IntegerType()),
    ),
    "products": fields(
        ("product_id", StringType()), ("product_name", StringType()), ("product_family", StringType()),
        ("customer_type", StringType()), ("data_allowance_gb", DoubleType()),
        ("unlimited_data", BooleanType()), ("voice_minutes", IntegerType()),
        ("unlimited_voice", BooleanType()), ("sms_allowance", IntegerType()),
        ("monthly_price_usd", DecimalType(10, 2)), ("overage_price_per_gb_usd", DecimalType(10, 2)),
        ("is_5g_enabled", BooleanType()), ("product_status", StringType()),
        ("valid_from", DateType()), ("valid_to", DateType()),
    ),
    "customers": fields(
        ("customer_id", StringType()), ("customer_type", StringType()),
        ("customer_segment", StringType()), ("region", StringType()), ("canton", StringType()),
        ("age_band", StringType()), ("created_date", DateType()),
        ("acquisition_channel", StringType()), ("preferred_channel", StringType()),
        ("digital_engagement_score", IntegerType()), ("analytics_consent", BooleanType()),
        ("customer_status", StringType()),
    ),
    "subscriptions": fields(
        ("subscription_id", StringType()), ("customer_id", StringType()), ("product_id", StringType()),
        ("line_id", StringType()), ("start_date", DateType()), ("end_date", DateType()),
        ("subscription_status", StringType()), ("contract_type", StringType()),
        ("is_port_in", BooleanType()), ("is_esim", BooleanType()),
        ("autopay_enabled", BooleanType()), ("monthly_fee_usd", DecimalType(10, 2)),
        ("billing_day", IntegerType()),
    ),
    "usage_daily": fields(
        ("usage_date", DateType()), ("subscription_id", StringType()),
        ("primary_cell_id", StringType()), ("data_gb", DoubleType()),
        ("voice_minutes", DoubleType()), ("sms_count", IntegerType()),
        ("data_5g_pct", DoubleType()), ("roaming_data_mb", DoubleType()),
        ("dropped_calls", IntegerType()), ("recharge_amount_usd", DecimalType(10, 2)),
        ("billed_amount_usd", DecimalType(10, 2)), ("ingestion_batch_id", StringType()),
    ),
    "support_tickets": fields(
        ("ticket_id", StringType()), ("customer_id", StringType()),
        ("subscription_id", StringType()), ("cell_id", StringType()),
        ("created_at", TimestampType()), ("resolved_at", TimestampType()),
        ("category", StringType()), ("severity", StringType()), ("ticket_status", StringType()),
        ("channel", StringType()), ("first_contact_resolution", BooleanType()),
        ("sla_target_hours", IntegerType()), ("resolution_hours", DoubleType()),
        ("post_service_csat", IntegerType()),
    ),
    "customer_surveys": fields(
        ("survey_id", StringType()), ("customer_id", StringType()),
        ("subscription_id", StringType()), ("response_date", DateType()),
        ("survey_trigger", StringType()), ("nps_score", IntegerType()),
        ("csat_score", IntegerType()), ("experience_area", StringType()),
        ("feedback_category", StringType()), ("survey_channel", StringType()),
    ),
}


def register_bronze(dataset_name, dataset_schema):
    @dp.table(
        name=f"{dataset_name}_bronze",
        comment=f"Bronze de {dataset_name}: CSV incremental con trazabilidad de archivo y datos rescatados",
    )
    def bronze_table():
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("rescuedDataColumn", "_rescued_data")
            .schema(dataset_schema)
            .load(f"{raw_root}/{dataset_name}")
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.col("_metadata.file_path"))
        )

    return bronze_table


for source_name, source_schema in DATASET_SCHEMAS.items():
    register_bronze(source_name, source_schema)


# ## Silver — dimensiones gobernadas
# Las expectations observan las anomalías sin eliminarlas. DQX, en el módulo
# siguiente, materializa los registros validados y la cuarentena.


@dp.materialized_view(name="network_sites_silver", comment="Sitios deduplicados y normalizados")
@dp.expect_all({
    "site_id_required": "site_id IS NOT NULL",
    "backup_power_non_negative": "backup_power_hours >= 0",
    "coordinates_valid": "latitude BETWEEN -5.1 AND 1.7 AND longitude BETWEEN -92.1 AND -75.1",
})
def network_sites_silver():
    return keep_latest(spark.read.table("network_sites_bronze"), ["site_id"], F.col("commissioned_date").desc())


@dp.materialized_view(name="radio_cells_silver", comment="Celdas deduplicadas con contrato tecnológico")
@dp.expect_all({
    "cell_id_required": "cell_id IS NOT NULL",
    "known_site_format": "site_id RLIKE '^SIT-[0-9]{4}$' AND site_id <> 'SIT-9999'",
    "technology_allowed": "technology IN ('4G', '5G')",
    "technology_band_consistent": "(technology = '4G' AND frequency_band LIKE 'B%') OR (technology = '5G' AND frequency_band LIKE 'n%')",
})
def radio_cells_silver():
    return keep_latest(spark.read.table("radio_cells_bronze"), ["cell_id"], F.col("site_id").asc())


@dp.materialized_view(name="products_silver", comment="Catálogo de productos deduplicado")
@dp.expect_all({
    "product_id_required": "product_id IS NOT NULL",
    "price_non_negative": "monthly_price_usd >= 0",
    "unlimited_data_consistent": "(unlimited_data AND data_allowance_gb IS NULL) OR (NOT unlimited_data AND data_allowance_gb IS NOT NULL)",
})
def products_silver():
    return keep_latest(spark.read.table("products_bronze"), ["product_id"], F.col("valid_from").desc())


@dp.materialized_view(name="customers_silver", comment="Clientes seudónimos deduplicados, sin PII")
@dp.expect_all({
    "customer_id_required": "customer_id IS NOT NULL",
    "segment_required": "customer_segment IS NOT NULL",
    "engagement_range": "digital_engagement_score BETWEEN 0 AND 100",
})
def customers_silver():
    return (
        keep_latest(spark.read.table("customers_bronze"), ["customer_id"], F.col("created_date").desc())
        .withColumn("tenure_months", F.floor(F.months_between(F.lit("2026-08-14"), "created_date")).cast("int"))
    )


# ## Silver — hechos enriquecidos


@dp.materialized_view(name="cell_tower_metrics_silver", comment="Telemetría enriquecida con celda y sitio")
@dp.expect_all({
    "measurement_id_required": "measurement_id IS NOT NULL",
    "cell_id_required": "cell_id IS NOT NULL",
    "cell_resolved": "site_id IS NOT NULL",
    "availability_range": "availability_pct BETWEEN 0 AND 100",
    "signal_range": "signal_strength_dbm BETWEEN -120 AND -40",
    "latency_range": "latency_ms BETWEEN 0 AND 500",
    "downlink_required": "downlink_mbps IS NOT NULL AND downlink_mbps >= 0",
})
def cell_tower_metrics_silver():
    metrics = keep_latest(
        spark.read.table("cell_tower_metrics_bronze"), ["measurement_id"], F.col("event_ts").desc()
    ).alias("m")
    cells = spark.read.table("radio_cells_silver").alias("c")
    sites = spark.read.table("network_sites_silver").alias("s")
    return (
        metrics.join(cells, F.col("m.cell_id") == F.col("c.cell_id"), "left")
        .join(sites, F.col("c.site_id") == F.col("s.site_id"), "left")
        .select(
            "m.*", F.col("c.site_id").alias("site_id"), "c.technology", "c.frequency_band",
            "c.bandwidth_mhz", "c.capacity_users", "s.region", "s.canton", "s.environment", "s.natural_region",
            "s.latitude", "s.longitude",
        )
        .withColumn("event_date", F.to_date("event_ts"))
        .withColumn("event_hour", F.date_trunc("hour", "event_ts"))
        .withColumn(
            "network_status",
            F.when((F.col("availability_pct") < 99) | (F.col("latency_ms") > 120) | (F.col("downlink_mbps") < 10), "Crítico")
            .when((F.col("latency_ms") > 80) | (F.col("downlink_mbps") < 25), "Atención")
            .otherwise("Saludable"),
        )
    )


@dp.materialized_view(name="network_alarms_silver", comment="Alarmas enriquecidas con contexto de red")
@dp.expect_all({
    "alarm_id_required": "alarm_id IS NOT NULL",
    "cell_resolved": "site_id IS NOT NULL",
    "severity_allowed": "severity IN ('Crítica', 'Alta', 'Media', 'Baja')",
    "dates_consistent": "closed_at IS NULL OR closed_at >= opened_at",
})
def network_alarms_silver():
    alarms = keep_latest(
        spark.read.table("network_alarms_bronze"), ["alarm_id"], F.col("opened_at").desc()
    ).alias("a")
    cells = spark.read.table("radio_cells_silver").alias("c")
    sites = spark.read.table("network_sites_silver").alias("s")
    return (
        alarms.join(cells, F.col("a.cell_id") == F.col("c.cell_id"), "left")
        .join(sites, F.col("c.site_id") == F.col("s.site_id"), "left")
        .select("a.*", F.col("c.site_id").alias("site_id"), "c.technology", "s.region", "s.canton")
        .withColumn("opened_date", F.to_date("opened_at"))
    )


@dp.materialized_view(name="maintenance_orders_silver", comment="Órdenes de mantenimiento enriquecidas con sitio")
@dp.expect_all({
    "work_order_required": "work_order_id IS NOT NULL",
    "site_resolved": "region IS NOT NULL",
    "priority_allowed": "priority IN ('P1', 'P2', 'P3', 'P4')",
    "cost_non_negative": "cost_usd >= 0",
    "dates_consistent": "completed_at IS NULL OR completed_at >= created_at",
})
def maintenance_orders_silver():
    orders = keep_latest(
        spark.read.table("maintenance_orders_bronze"), ["work_order_id"], F.col("created_at").desc()
    ).alias("o")
    sites = spark.read.table("network_sites_silver").alias("s")
    return (
        orders.join(sites, F.col("o.site_id") == F.col("s.site_id"), "left")
        .select("o.*", "s.region", "s.canton", "s.environment", "s.natural_region")
        .withColumn("created_date", F.to_date("created_at"))
    )


@dp.materialized_view(name="subscriptions_silver", comment="Suscripciones enriquecidas con cliente y producto")
@dp.expect_all({
    "subscription_required": "subscription_id IS NOT NULL",
    "customer_resolved": "customer_segment IS NOT NULL",
    "product_resolved": "product_name IS NOT NULL",
    "dates_consistent": "end_date IS NULL OR end_date >= start_date",
    "monthly_fee_non_negative": "monthly_fee_usd >= 0",
})
def subscriptions_silver():
    subscriptions = keep_latest(
        spark.read.table("subscriptions_bronze"), ["subscription_id"], F.col("start_date").desc()
    ).alias("sub")
    customers = spark.read.table("customers_silver").alias("cus")
    products = spark.read.table("products_silver").alias("pro")
    return (
        subscriptions.join(customers, F.col("sub.customer_id") == F.col("cus.customer_id"), "left")
        .join(products, F.col("sub.product_id") == F.col("pro.product_id"), "left")
        .select(
            "sub.*", "cus.customer_type", "cus.customer_segment", "cus.region", "cus.canton",
            "cus.age_band", "cus.digital_engagement_score", "cus.customer_status",
            "pro.product_name", "pro.product_family", "pro.data_allowance_gb",
            "pro.unlimited_data", "pro.is_5g_enabled", "pro.product_status",
        )
    )


@dp.materialized_view(name="usage_daily_silver", comment="Consumo diario enriquecido con cliente, producto y red")
@dp.expect_all({
    "subscription_resolved": "customer_id IS NOT NULL",
    "cell_resolved": "site_id IS NOT NULL",
    "data_non_negative": "data_gb >= 0",
    "data_5g_range": "data_5g_pct BETWEEN 0 AND 100",
})
def usage_daily_silver():
    usage = keep_latest(
        spark.read.table("usage_daily_bronze"),
        ["usage_date", "subscription_id"],
        F.col("ingestion_batch_id").desc(),
    ).alias("u")
    subscriptions = spark.read.table("subscriptions_silver").alias("sub")
    cells = spark.read.table("radio_cells_silver").alias("c")
    sites = spark.read.table("network_sites_silver").alias("s")
    return (
        usage.join(subscriptions, F.col("u.subscription_id") == F.col("sub.subscription_id"), "left")
        .join(cells, F.col("u.primary_cell_id") == F.col("c.cell_id"), "left")
        .join(sites, F.col("c.site_id") == F.col("s.site_id"), "left")
        .select(
            "u.*", "sub.customer_id", "sub.product_id", "sub.line_id", "sub.subscription_status",
            "sub.contract_type", "sub.monthly_fee_usd", "sub.customer_type", "sub.customer_segment",
            "sub.digital_engagement_score", "sub.customer_status",
            F.col("sub.region").alias("customer_region"), F.col("sub.canton").alias("customer_canton"),
            "sub.product_name", "sub.product_family", "sub.data_allowance_gb", "sub.unlimited_data",
            F.col("c.site_id").alias("site_id"), "c.technology", "c.frequency_band",
            F.col("s.region").alias("network_region"), F.col("s.canton").alias("network_canton"),
        )
    )


@dp.materialized_view(name="support_tickets_silver", comment="Tickets enriquecidos con cliente, producto y red")
@dp.expect_all({
    "ticket_required": "ticket_id IS NOT NULL",
    "category_required": "category IS NOT NULL",
    "subscription_resolved": "product_name IS NOT NULL",
    "dates_consistent": "resolved_at IS NULL OR resolved_at >= created_at",
    "resolution_non_negative": "resolution_hours IS NULL OR resolution_hours >= 0",
})
def support_tickets_silver():
    tickets = keep_latest(
        spark.read.table("support_tickets_bronze"), ["ticket_id"], F.col("created_at").desc()
    ).alias("t")
    subscriptions = spark.read.table("subscriptions_silver").alias("sub")
    cells = spark.read.table("radio_cells_silver").alias("c")
    sites = spark.read.table("network_sites_silver").alias("s")
    return (
        tickets.join(subscriptions, F.col("t.subscription_id") == F.col("sub.subscription_id"), "left")
        .join(cells, F.col("t.cell_id") == F.col("c.cell_id"), "left")
        .join(sites, F.col("c.site_id") == F.col("s.site_id"), "left")
        .select(
            "t.*", "sub.product_id", "sub.product_name", "sub.product_family",
            "sub.customer_type", "sub.customer_segment", "sub.contract_type",
            F.col("c.site_id").alias("site_id"), "c.technology",
            F.coalesce(F.col("s.region"), F.col("sub.region")).alias("region"),
            F.coalesce(F.col("s.canton"), F.col("sub.canton")).alias("canton"),
        )
        .withColumn("created_date", F.to_date("created_at"))
        .withColumn("sla_met", F.col("resolution_hours").isNotNull() & (F.col("resolution_hours") <= F.col("sla_target_hours")))
    )


@dp.materialized_view(name="customer_surveys_silver", comment="Encuestas NPS/CSAT enriquecidas")
@dp.expect_all({
    "survey_required": "survey_id IS NOT NULL",
    "subscription_resolved": "product_name IS NOT NULL",
    "nps_range": "nps_score BETWEEN 0 AND 10",
    "csat_range": "csat_score BETWEEN 1 AND 5",
})
def customer_surveys_silver():
    surveys = keep_latest(
        spark.read.table("customer_surveys_bronze"), ["survey_id"], F.col("response_date").desc()
    ).alias("e")
    subscriptions = spark.read.table("subscriptions_silver").alias("sub")
    return (
        surveys.join(subscriptions, F.col("e.subscription_id") == F.col("sub.subscription_id"), "left")
        .select(
            "e.*", "sub.product_id", "sub.product_name", "sub.product_family",
            "sub.customer_type", "sub.customer_segment", "sub.region", "sub.canton",
        )
        .withColumn(
            "nps_class",
            F.when(F.col("nps_score") >= 9, "Promotor").when(F.col("nps_score") >= 7, "Pasivo").otherwise("Detractor"),
        )
    )


# ## Gold preliminar administrado por SDP
# Esta vista materializada ilustra agregación declarativa e incrementalización.
# El módulo SQL crea el resto de Gold después de la validación DQX.


@dp.materialized_view(name="gold_network_hourly_pipeline", comment="KPIs horarios preliminares creados por SDP")
def gold_network_hourly_pipeline():
    return (
        spark.read.table("cell_tower_metrics_silver")
        .where(
            F.col("cell_id").isNotNull()
            & F.col("site_id").isNotNull()
            & F.col("availability_pct").between(0, 100)
            & F.col("signal_strength_dbm").between(-120, -40)
            & F.col("latency_ms").between(0, 500)
            & F.col("downlink_mbps").isNotNull()
        )
        .groupBy(
            "event_hour", "event_date", "region", "canton", "site_id",
            "technology", "frequency_band", "environment", "natural_region",
        )
        .agg(
            F.countDistinct("cell_id").alias("active_cells"),
            F.round(F.avg("availability_pct"), 3).alias("avg_availability_pct"),
            F.round(F.avg("signal_strength_dbm"), 2).alias("avg_signal_dbm"),
            F.round(F.avg("sinr_db"), 2).alias("avg_sinr_db"),
            F.round(F.avg("latency_ms"), 2).alias("avg_latency_ms"),
            F.round(F.avg("downlink_mbps"), 2).alias("avg_downlink_mbps"),
            F.round(F.avg("uplink_mbps"), 2).alias("avg_uplink_mbps"),
            F.round(F.avg("packet_loss_pct"), 3).alias("avg_packet_loss_pct"),
            F.sum("dropped_calls").alias("dropped_calls"),
            F.sum("data_traffic_gb").alias("data_traffic_gb"),
            F.sum("active_users").alias("active_users"),
            F.round(
                F.avg(
                    (
                        (F.col("availability_pct") >= 99.0)
                        & (F.col("latency_ms") <= 80)
                        & (F.col("downlink_mbps") >= 20)
                    ).cast("double")
                ) * 100,
                2,
            ).alias("network_sla_compliance_pct"),
        )
    )
