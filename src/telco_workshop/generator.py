"""Generador PySpark determinista para el workshop de telecomunicaciones.

Plan aprobado: once fuentes, cerca de 100.000 filas y seed 42. El modelo
cubre red, operaciones, clientes, productos y experiencia sin generar nombres,
cédulas, teléfonos, correos, direcciones, IMSI, IMEI ni texto libre.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from pyspark.sql import DataFrame, SparkSession
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
)

SEED = 42
START_TS = "2026-08-01 00:00:00"

DATASET_COUNTS = {
    "network_sites": 60,
    "radio_cells": 180,
    "cell_tower_metrics": 30_240,
    "network_alarms": 1_200,
    "maintenance_orders": 400,
    "products": 18,
    "customers": 3_000,
    "subscriptions": 4_200,
    "usage_daily": 58_800,
    "support_tickets": 1_500,
    "customer_surveys": 800,
}

LOCATIONS = [
    # (provincia, cantón, latitud, longitud, entorno, región natural)
    ("Pichincha", "Quito", -0.1807, -78.4678, "Urbano denso", "Sierra"),
    ("Pichincha", "Rumiñahui", -0.3126, -78.4454, "Periurbano", "Sierra"),
    ("Guayas", "Guayaquil", -2.1710, -79.9224, "Urbano denso", "Costa"),
    ("Guayas", "Samborondón", -2.1300, -79.8700, "Urbano", "Costa"),
    ("Azuay", "Cuenca", -2.9006, -79.0045, "Urbano", "Sierra"),
    ("Manabí", "Manta", -0.9677, -80.7089, "Costero", "Costa"),
    ("Manabí", "Portoviejo", -1.0546, -80.4545, "Urbano", "Costa"),
    ("El Oro", "Machala", -3.2581, -79.9554, "Costero", "Costa"),
    ("Tungurahua", "Ambato", -1.2491, -78.6168, "Andino", "Sierra"),
    ("Loja", "Loja", -3.9931, -79.2042, "Andino", "Sierra"),
    ("Orellana", "Francisco de Orellana", -0.4663, -76.9872, "Amazónico", "Amazonía"),
    ("Galápagos", "Santa Cruz", -0.7436, -90.3140, "Insular", "Insular"),
]


def _mod(column: F.Column | str, divisor: int) -> F.Column:
    return F.pmod(F.col(column) if isinstance(column, str) else column, F.lit(divisor))


def _lookup(values: list[object], index_zero_based: F.Column) -> F.Column:
    return F.element_at(F.array(*[F.lit(value) for value in values]), index_zero_based.cast("int") + 1)


def _timestamp_from_seconds(seconds: F.Column) -> F.Column:
    return F.to_timestamp(F.from_unixtime(F.unix_timestamp(F.lit(START_TS)) + seconds.cast("long")))


def _timestamp_add_hours(timestamp: F.Column, hours: F.Column) -> F.Column:
    return F.to_timestamp(F.from_unixtime(F.unix_timestamp(timestamp) + hours.cast("long") * 3_600))


def build_network_sites(spark: SparkSession, rows: int = 60, seed: int = SEED) -> DataFrame:
    """Crea la dimensión física de sitios e inyecta dos impurezas controladas."""
    if rows != 60:
        raise ValueError("network_sites usa 60 filas para mantener las relaciones del workshop")

    loc = _mod("row_id", len(LOCATIONS))
    df = (
        spark.range(rows)
        .withColumnRenamed("id", "row_id")
        .withColumn("site_number", (F.col("row_id") + 1).cast("int"))
        .withColumn("site_id", F.format_string("SIT-%04d", F.col("site_number")))
        .withColumn("region", _lookup([x[0] for x in LOCATIONS], loc))
        .withColumn("canton", _lookup([x[1] for x in LOCATIONS], loc))
        .withColumn("environment", _lookup([x[4] for x in LOCATIONS], loc))
        .withColumn("natural_region", _lookup([x[5] for x in LOCATIONS], loc))
        .withColumn(
            "latitude",
            F.round(_lookup([x[2] for x in LOCATIONS], loc) + (_mod("site_number", 5) - 2) * 0.008, 5),
        )
        .withColumn(
            "longitude",
            F.round(_lookup([x[3] for x in LOCATIONS], loc) + (2 - _mod("site_number", 5)) * 0.010, 5),
        )
        .withColumn(
            "site_type",
            F.when(F.col("environment") == "Urbano denso", "Azotea")
            .when(F.col("environment") == "Costero", "Torre")
            .when(_mod("site_number", 7) == 0, "Interior")
            .when(_mod("site_number", 3) == 0, "Poste")
            .otherwise("Torre"),
        )
        .withColumn("ownership_type", F.when(_mod("site_number", 4) == 0, "Compartido").otherwise("Propio"))
        .withColumn(
            "commissioned_date",
            F.date_add(F.lit("2017-01-01").cast("date"), _mod(F.col("site_number") * 137, 3_460).cast("int")),
        )
        .withColumn(
            "backup_power_hours",
            F.round(
                F.when(F.col("environment").isin("Amazónico", "Insular", "Periurbano"), 6.0).otherwise(2.0)
                + F.rand(seed) * 5.0,
                1,
            ),
        )
        .withColumn(
            "site_status",
            F.when(_mod("site_number", 31) == 0, "Fuera de servicio")
            .when(_mod("site_number", 17) == 0, "Mantenimiento")
            .otherwise("Activo"),
        )
        .withColumn(
            "backup_power_hours",
            F.when(F.col("row_id") == rows - 2, -2.0).otherwise(F.col("backup_power_hours")),
        )
        .withColumn("site_id", F.when(F.col("row_id") == rows - 1, "SIT-0001").otherwise(F.col("site_id")))
    )
    return df.select(
        "site_id", "region", "canton", "latitude", "longitude", "environment", "natural_region",
        "site_type", "ownership_type", "commissioned_date", "backup_power_hours", "site_status",
    )


def build_radio_cells(spark: SparkSession, rows: int = 180) -> DataFrame:
    """Crea tres sectores por sitio con 4G/5G y bandas coherentes."""
    if rows != 180:
        raise ValueError("radio_cells usa 180 filas para mantener tres celdas por sitio")

    df = (
        spark.range(rows)
        .withColumnRenamed("id", "row_id")
        .withColumn("cell_number", (F.col("row_id") + 1).cast("int"))
        .withColumn("site_number", (F.floor(F.col("row_id") / 3) + 1).cast("int"))
        .withColumn("sector_number", (_mod("row_id", 3) + 1).cast("int"))
        .withColumn("cell_id", F.format_string("CEL-%05d", F.col("cell_number")))
        .withColumn("site_id", F.format_string("SIT-%04d", F.col("site_number")))
        .withColumn(
            "technology",
            F.when((_mod("site_number", 3) != 0) & (F.col("sector_number") >= 2), "5G").otherwise("4G"),
        )
        .withColumn(
            "frequency_band",
            F.when((F.col("technology") == "5G") & (_mod("site_number", 4) == 0), "n28-700")
            .when(F.col("technology") == "5G", "n78-3500")
            .when(_mod("site_number", 5) == 0, "B28-700")
            .when(F.col("sector_number") == 1, "B3-1800")
            .otherwise("B7-2600"),
        )
        .withColumn(
            "bandwidth_mhz",
            F.when(F.col("frequency_band") == "n78-3500", 100)
            .when(F.col("frequency_band") == "n28-700", 40)
            .when(F.col("frequency_band") == "B7-2600", 20)
            .when(F.col("frequency_band") == "B3-1800", 15)
            .otherwise(10),
        )
        .withColumn(
            "capacity_users",
            (
                F.when(F.col("technology") == "5G", 2_200).otherwise(1_100)
                + F.col("bandwidth_mhz") * 9
                + _mod(F.col("cell_number") * 37, 500)
            ).cast("int"),
        )
        .withColumn("azimuth_degrees", ((F.col("sector_number") - 1) * 120 + _mod("site_number", 11)).cast("int"))
        .withColumn("vendor_family", _lookup(["Proveedor A", "Proveedor B", "Proveedor C"], _mod("site_number", 3)))
        .withColumn(
            "cell_status",
            F.when(_mod("cell_number", 89) == 0, "Fuera de servicio")
            .when(_mod("cell_number", 47) == 0, "Mantenimiento")
            .otherwise("Activa"),
        )
        .withColumn("site_id", F.when(F.col("row_id") == rows - 2, "SIT-9999").otherwise(F.col("site_id")))
        .withColumn("frequency_band", F.when(F.col("row_id") == rows - 1, "B7-2600").otherwise(F.col("frequency_band")))
        .withColumn("technology", F.when(F.col("row_id") == rows - 1, "5G").otherwise(F.col("technology")))
    )
    return df.select(
        "cell_id", "site_id", "sector_number", "azimuth_degrees", "technology",
        "frequency_band", "bandwidth_mhz", "capacity_users", "vendor_family", "cell_status",
    )


def build_tower_metrics(
    spark: SparkSession,
    rows: int = 30_240,
    cells: int = 180,
    seed: int = SEED,
    inject_quality_issues: bool = True,
) -> DataFrame:
    """Crea siete días de telemetría horaria correlacionada por celda."""
    if rows <= 0 or cells <= 0 or rows % cells or rows // cells != 168:
        raise ValueError("cell_tower_metrics espera 180 celdas por 168 horas")

    df = (
        spark.range(rows)
        .withColumnRenamed("id", "row_id")
        .withColumn("cell_number", (_mod("row_id", cells) + 1).cast("int"))
        .withColumn("hour_index", F.floor(F.col("row_id") / cells).cast("int"))
        .withColumn("event_ts", _timestamp_from_seconds(F.col("hour_index") * 3_600))
        .withColumn("cell_id", F.format_string("CEL-%05d", F.col("cell_number")))
        .withColumn(
            "is_5g",
            (_mod(F.ceil(F.col("cell_number") / 3), 3) != 0) & ((_mod(F.col("cell_number") - 1, 3) + 1) >= 2),
        )
        .withColumn(
            "load_factor",
            F.greatest(
                F.lit(0.18),
                F.lit(0.50)
                + 0.33 * F.sin((_mod("hour_index", 24) - 8) * F.lit(3.1415926535 / 12))
                + _mod("cell_number", 9) * 0.01,
            ),
        )
        .withColumn(
            "active_users",
            F.round(F.when(F.col("is_5g"), 1_350).otherwise(850) * F.col("load_factor") + F.rand(seed) * 240).cast("int"),
        )
        .withColumn("congestion", F.least(F.lit(1.0), F.col("active_users") / 1_600.0))
        .withColumn(
            "signal_strength_dbm",
            F.round(
                F.when(F.col("is_5g"), -77.0).otherwise(-82.0)
                - _mod("cell_number", 13) * 0.9
                - F.col("congestion") * 8.0
                + F.randn(seed + 1) * 3.5,
                2,
            ),
        )
        .withColumn(
            "sinr_db",
            F.round(
                F.greatest(
                    F.lit(-5.0),
                    24.0 - F.col("congestion") * 13.0
                    - F.greatest(F.lit(0.0), -90.0 - F.col("signal_strength_dbm")) * 0.6
                    + F.randn(seed + 2) * 2.0,
                ),
                2,
            ),
        )
        .withColumn(
            "latency_ms",
            F.round(
                F.when(F.col("is_5g"), 18.0).otherwise(34.0)
                + F.col("congestion") * 85.0
                + F.greatest(F.lit(0.0), -88.0 - F.col("signal_strength_dbm")) * 1.9
                + F.abs(F.randn(seed + 3)) * 6.0,
                2,
            ),
        )
        .withColumn(
            "downlink_mbps",
            F.round(
                F.greatest(
                    F.lit(0.3),
                    F.when(F.col("is_5g"), 210.0).otherwise(88.0)
                    - F.col("congestion") * F.when(F.col("is_5g"), 115.0).otherwise(55.0)
                    - F.greatest(F.lit(0.0), -88.0 - F.col("signal_strength_dbm")) * 1.6
                    + F.randn(seed + 4) * 6.0,
                ),
                2,
            ),
        )
        .withColumn("uplink_mbps", F.round(F.greatest(F.lit(0.2), F.col("downlink_mbps") * 0.18 + F.randn(seed + 5) * 2), 2))
        .withColumn(
            "packet_loss_pct",
            F.round(
                F.greatest(
                    F.lit(0.0),
                    F.col("congestion") * 2.1
                    + F.greatest(F.lit(0.0), -95.0 - F.col("signal_strength_dbm")) * 0.16
                    + F.randn(seed + 6) * 0.25,
                ),
                2,
            ),
        )
        .withColumn(
            "dropped_calls",
            F.greatest(
                F.lit(0),
                F.round(
                    F.col("congestion") * 6.0
                    + F.greatest(F.lit(0.0), -94.0 - F.col("signal_strength_dbm")) * 0.45
                    + F.randn(seed + 7) * 1.2
                ).cast("int"),
            ),
        )
        .withColumn(
            "availability_pct",
            F.round(
                F.least(
                    F.lit(100.0),
                    99.98 - F.when(_mod("cell_number", 37) == 0, 4.5).otherwise(0.0)
                    - F.col("congestion") * 0.25 + F.randn(seed + 8) * 0.05,
                ),
                3,
            ),
        )
        .withColumn("data_traffic_gb", F.round(F.col("active_users") * (0.06 + F.rand(seed + 9) * 0.08), 2))
        .withColumn("measurement_id", F.format_string("MED-%08d", F.col("row_id") + 1))
        .withColumn("ingestion_batch_id", F.date_format("event_ts", "'BATCH-'yyyyMMdd-HH"))
    )
    if inject_quality_issues:
        df = (
            df.withColumn(
                "measurement_id",
                F.when((F.col("row_id") > 0) & (_mod("row_id", 101) == 0), F.format_string("MED-%08d", F.col("row_id")))
                .otherwise(F.col("measurement_id")),
            )
            .withColumn(
                "cell_id",
                F.when(_mod("row_id", 337) == 0, F.lit(None).cast("string"))
                .when(_mod("row_id", 331) == 0, "CEL-99999").otherwise(F.col("cell_id")),
            )
            .withColumn(
                "latency_ms",
                F.when(_mod("row_id", 251) == 0, 650.0)
                .when(_mod("row_id", 509) == 0, -10.0).otherwise(F.col("latency_ms")),
            )
            .withColumn("signal_strength_dbm", F.when(_mod("row_id", 241) == 0, -145.0).otherwise(F.col("signal_strength_dbm")))
            .withColumn("downlink_mbps", F.when(_mod("row_id", 199) == 0, F.lit(None).cast("double")).otherwise(F.col("downlink_mbps")))
            .withColumn("availability_pct", F.when(_mod("row_id", 307) == 0, 105.0).otherwise(F.col("availability_pct")))
        )
    return df.select(
        "measurement_id", "cell_id", "event_ts", "availability_pct", "signal_strength_dbm",
        "sinr_db", "latency_ms", "downlink_mbps", "uplink_mbps", "packet_loss_pct",
        "dropped_calls", "active_users", "data_traffic_gb", "ingestion_batch_id",
    )


def build_network_alarms(spark: SparkSession, rows: int = 1_200) -> DataFrame:
    """Crea alarmas operativas correlacionables con celdas."""
    df = (
        spark.range(rows).withColumnRenamed("id", "row_id")
        .withColumn("alarm_number", (F.col("row_id") + 1).cast("int"))
        .withColumn("cell_number", (_mod(F.col("row_id") * 17, 180) + 1).cast("int"))
        .withColumn("alarm_id", F.format_string("ALM-%06d", F.col("alarm_number")))
        .withColumn("cell_id", F.format_string("CEL-%05d", F.col("cell_number")))
        .withColumn("opened_at", _timestamp_from_seconds(_mod(F.col("row_id") * 791, 604_800)))
        .withColumn("severity", _lookup(["Crítica", "Alta", "Alta", "Media", "Media", "Baja"], _mod("row_id", 6)))
        .withColumn(
            "alarm_status",
            F.when(_mod("row_id", 10) < 7, "Cerrada")
            .when(_mod("row_id", 10) < 9, "Reconocida").otherwise("Abierta"),
        )
        .withColumn(
            "closed_at",
            F.when(
                F.col("alarm_status") == "Cerrada",
                _timestamp_add_hours(F.col("opened_at"), (_mod(F.col("row_id") * 13, 72) + 1).cast("int")),
            ).cast("timestamp"),
        )
        .withColumn("alarm_type", _lookup(["Energía", "Transporte", "Radio", "Saturación", "Hardware", "Software"], _mod("row_id", 6)))
        .withColumn(
            "probable_cause",
            _lookup(["Falla de suministro", "Pérdida de enlace", "Interferencia", "Alta demanda", "Módulo degradado", "Error de configuración"], _mod("row_id", 6)),
        )
        .withColumn("service_impact", F.col("severity").isin("Crítica", "Alta") | (_mod("row_id", 7) == 0))
        .withColumn(
            "affected_users_est",
            F.when(F.col("severity") == "Crítica", 900)
            .when(F.col("severity") == "Alta", 420)
            .when(F.col("severity") == "Media", 120).otherwise(25)
            + _mod(F.col("row_id") * 29, 200),
        )
        .withColumn("source_system", _lookup(["NMS", "RAN Monitor", "Energy Monitor"], _mod("row_id", 3)))
        .withColumn(
            "alarm_id",
            F.when((F.col("row_id") > 0) & (_mod("row_id", 211) == 0), F.format_string("ALM-%06d", F.col("row_id")))
            .otherwise(F.col("alarm_id")),
        )
        .withColumn("cell_id", F.when(_mod("row_id", 157) == 0, "CEL-99999").otherwise(F.col("cell_id")))
        .withColumn(
            "closed_at",
            F.when(_mod("row_id", 199) == 0, _timestamp_add_hours(F.col("opened_at"), F.lit(-2)))
            .otherwise(F.col("closed_at")),
        )
        .withColumn("severity", F.when(_mod("row_id", 223) == 0, "Urgente").otherwise(F.col("severity")))
    )
    return df.select(
        "alarm_id", "cell_id", "opened_at", "closed_at", "alarm_type", "severity",
        "alarm_status", "probable_cause", "service_impact", "affected_users_est", "source_system",
    )


def build_maintenance_orders(spark: SparkSession, rows: int = 400, seed: int = SEED) -> DataFrame:
    """Crea órdenes de mantenimiento relacionadas con sitios y alarmas."""
    df = (
        spark.range(rows).withColumnRenamed("id", "row_id")
        .withColumn("work_order_id", F.format_string("OT-%05d", F.col("row_id") + 1))
        .withColumn("site_id", F.format_string("SIT-%04d", _mod(F.col("row_id") * 7, 60) + 1))
        .withColumn(
            "alarm_id",
            F.when(_mod("row_id", 20) < 13, F.format_string("ALM-%06d", _mod(F.col("row_id") * 3, 1_200) + 1)),
        )
        .withColumn("created_at", _timestamp_from_seconds(_mod(F.col("row_id") * 1_229, 1_209_600)))
        .withColumn("scheduled_at", _timestamp_add_hours(F.col("created_at"), F.lit(6)))
        .withColumn("order_status", _lookup(["Creada", "Asignada", "En campo", "Completada", "Completada", "Cancelada"], _mod("row_id", 6)))
        .withColumn(
            "completed_at",
            F.when(F.col("order_status") == "Completada", _timestamp_add_hours(F.col("scheduled_at"), _mod(F.col("row_id") * 11, 48) + 1)).cast("timestamp"),
        )
        .withColumn("order_type", _lookup(["Preventiva", "Correctiva", "Correctiva", "Emergencia"], _mod("row_id", 4)))
        .withColumn("priority", _lookup(["P1", "P2", "P2", "P3", "P3", "P4"], _mod("row_id", 6)))
        .withColumn("provider_code", F.format_string("PRV-%02d", _mod("row_id", 5) + 1))
        .withColumn("downtime_minutes", _mod(F.col("row_id") * 47, 721).cast("int"))
        .withColumn("cost_usd", F.round(85.0 + F.exp(F.rand(seed) * 4.2) * 52.0, 2).cast("decimal(12,2)"))
        .withColumn(
            "sla_target_hours",
            F.when(F.col("priority") == "P1", 2).when(F.col("priority") == "P2", 8)
            .when(F.col("priority") == "P3", 24).otherwise(72),
        )
        .withColumn("site_id", F.when(_mod("row_id", 197) == 0, "SIT-9999").otherwise(F.col("site_id")))
        .withColumn(
            "completed_at",
            F.when(_mod("row_id", 193) == 0, _timestamp_add_hours(F.col("created_at"), F.lit(-1)))
            .otherwise(F.col("completed_at")),
        )
        .withColumn("cost_usd", F.when(_mod("row_id", 191) == 0, F.lit(-25).cast("decimal(12,2)")).otherwise(F.col("cost_usd")))
        .withColumn("priority", F.when(_mod("row_id", 181) == 0, "PX").otherwise(F.col("priority")))
    )
    return df.select(
        "work_order_id", "site_id", "alarm_id", "created_at", "scheduled_at", "completed_at",
        "order_type", "priority", "order_status", "provider_code", "downtime_minutes", "cost_usd", "sla_target_hours",
    )


def build_products(spark: SparkSession) -> DataFrame:
    """Crea un catálogo ficticio de planes móviles y dos filas inválidas."""
    schema = StructType([
        StructField("product_id", StringType(), False), StructField("product_name", StringType(), False),
        StructField("product_family", StringType(), True), StructField("customer_type", StringType(), False),
        StructField("data_allowance_gb", DoubleType(), True), StructField("unlimited_data", BooleanType(), False),
        StructField("voice_minutes", IntegerType(), True), StructField("unlimited_voice", BooleanType(), False),
        StructField("sms_allowance", IntegerType(), False), StructField("monthly_price_usd", DecimalType(10, 2), False),
        StructField("overage_price_per_gb_usd", DecimalType(10, 2), False), StructField("is_5g_enabled", BooleanType(), False),
        StructField("product_status", StringType(), False), StructField("valid_from", DateType(), False),
        StructField("valid_to", DateType(), True),
    ])
    definitions = [
        ("Prepago 10 GB", "Prepago", "Persona", 10.0, False, 300, False, 100, 5, 2.5, False),
        ("Prepago 25 GB", "Prepago", "Persona", 25.0, False, 500, False, 200, 8, 2.2, True),
        ("Prepago 50 GB", "Prepago", "Persona", 50.0, False, 800, False, 300, 10, 1.9, True),
        ("Prepago 80 GB", "Prepago", "Persona", 80.0, False, 1200, False, 500, 15, 1.5, True),
        ("Plan Móvil 20 GB", "Postpago", "Persona", 20.0, False, None, True, 1000, 14.99, 1.8, False),
        ("Plan Móvil 40 GB", "Postpago", "Persona", 40.0, False, None, True, 1000, 19.99, 1.5, True),
        ("Plan Móvil 80 GB", "Postpago", "Persona", 80.0, False, None, True, 1500, 24.99, 1.2, True),
        ("Plan Móvil 120 GB", "Postpago", "Persona", 120.0, False, None, True, 1500, 29.99, 1.0, True),
        ("Plan Móvil 150 GB", "Postpago", "Persona", 150.0, False, None, True, 2000, 34.99, 0.8, True),
        ("Plan Móvil 200 GB", "Postpago", "Persona", 200.0, False, None, True, 2000, 39.99, 0.6, True),
        ("Plan Móvil 300 GB", "Postpago", "Persona", 300.0, False, None, True, 2000, 44.99, 0.5, True),
        ("Plan Móvil Ilimitado", "Postpago", "Persona", None, True, None, True, 2000, 49.99, 0, True),
        ("Empresa 100 GB", "Empresa", "Empresa", 100.0, False, None, True, 2000, 29.99, 0.9, True),
        ("Empresa 200 GB", "Empresa", "Empresa", 200.0, False, None, True, 2000, 39.99, 0.6, True),
        ("Empresa 500 GB", "Empresa", "Empresa", 500.0, False, None, True, 2000, 59.99, 0.3, True),
        ("Empresa Datos Ilimitados", "Empresa", "Empresa", None, True, None, True, 2000, 89.99, 0, True),
    ]
    rows = []
    for idx, item in enumerate(definitions, 1):
        name, family, customer_type, data_gb, unlim_data, voice, unlim_voice, sms, price, overage, enabled = item
        rows.append((
            f"PRD-{idx:03d}", name, family, customer_type, data_gb, unlim_data, voice, unlim_voice, sms,
            Decimal(str(price)), Decimal(str(overage)), enabled, "Activo", date(2024, 1, 1), None,
        ))
    rows.extend([
        ("PRD-017", "Plan Histórico 15 GB", "Postpago", "Persona", 15.0, False, 500, False, 500, Decimal("-14.99"), Decimal("1.50"), False, "Retirado", date(2022, 1, 1), date(2024, 12, 31)),
        ("PRD-018", "Plan Histórico Ilimitado", "Postpago", "Persona", 30.0, True, None, True, 1000, Decimal("24.99"), Decimal("0"), False, "Retirado", date(2022, 1, 1), date(2025, 6, 30)),
    ])
    return spark.createDataFrame(rows, schema)


def build_customers(spark: SparkSession, rows: int = 3_000) -> DataFrame:
    """Crea clientes seudónimos sin PII."""
    loc = _mod(F.col("row_id") * 7, len(LOCATIONS))
    df = (
        spark.range(rows).withColumnRenamed("id", "row_id")
        .withColumn("customer_number", (F.col("row_id") + 1).cast("int"))
        .withColumn("customer_id", F.format_string("CLI-%06d", F.col("customer_number")))
        .withColumn("customer_type", F.when(_mod("customer_number", 10) == 0, "Empresa").otherwise("Persona"))
        .withColumn(
            "customer_segment",
            F.when(F.col("customer_type") == "Empresa", F.when(_mod("row_id", 3) == 0, "Corporativo").otherwise("Pyme"))
            .when(_mod("row_id", 5) < 2, "Prepago").otherwise("Postpago"),
        )
        .withColumn("region", _lookup([x[0] for x in LOCATIONS], loc))
        .withColumn("canton", _lookup([x[1] for x in LOCATIONS], loc))
        .withColumn(
            "age_band",
            F.when(F.col("customer_type") == "Empresa", F.lit(None).cast("string"))
            .otherwise(_lookup(["18-24", "25-34", "25-34", "35-44", "45-54", "55+"], _mod("row_id", 6))),
        )
        .withColumn("created_date", F.date_add(F.lit("2018-01-01").cast("date"), _mod(F.col("row_id") * 37, 3_130).cast("int")))
        .withColumn("acquisition_channel", _lookup(["Digital", "Digital", "Tienda", "Call center", "Distribuidor"], _mod("row_id", 5)))
        .withColumn("preferred_channel", _lookup(["App", "App", "Web", "WhatsApp", "Call center", "Tienda"], _mod(F.col("row_id") * 5, 6)))
        .withColumn("digital_engagement_score", _mod(F.col("row_id") * 17 + 29, 101).cast("int"))
        .withColumn("analytics_consent", _mod("row_id", 10) < 8)
        .withColumn(
            "customer_status",
            F.when(_mod("row_id", 97) == 0, "Baja").when(_mod("row_id", 43) == 0, "Suspendido").otherwise("Activo"),
        )
        .withColumn(
            "customer_id",
            F.when((F.col("row_id") > 0) & (_mod("row_id", 211) == 0), F.format_string("CLI-%06d", F.col("row_id")))
            .otherwise(F.col("customer_id")),
        )
        .withColumn("customer_segment", F.when(_mod("row_id", 223) == 0, F.lit(None).cast("string")).otherwise(F.col("customer_segment")))
        .withColumn("digital_engagement_score", F.when(_mod("row_id", 251) == 0, 130).otherwise(F.col("digital_engagement_score")))
        .withColumn("canton", F.when(_mod("row_id", 257) == 0, "Cantón desconocido").otherwise(F.col("canton")))
    )
    return df.select(
        "customer_id", "customer_type", "customer_segment", "region", "canton", "age_band",
        "created_date", "acquisition_channel", "preferred_channel", "digital_engagement_score",
        "analytics_consent", "customer_status",
    )


def _subscription_product_number(subscription_number: F.Column) -> F.Column:
    customer_number = _mod(subscription_number - 1, 3_000) + 1
    return F.when(_mod(customer_number, 10) == 0, 13 + _mod(subscription_number, 4)).otherwise(1 + _mod(subscription_number * 7, 12))


PRODUCT_PRICES = [5, 8, 10, 15, 14.99, 19.99, 24.99, 29.99, 34.99, 39.99, 44.99, 49.99, 29.99, 39.99, 59.99, 89.99]
PRODUCT_ALLOWANCES = [10, 25, 50, 80, 20, 40, 80, 120, 150, 200, 300, 450, 100, 200, 500, 800]


def build_subscriptions(spark: SparkSession, rows: int = 4_200) -> DataFrame:
    """Crea relaciones cliente-producto con líneas totalmente ficticias."""
    df = (
        spark.range(rows).withColumnRenamed("id", "row_id")
        .withColumn("subscription_number", (F.col("row_id") + 1).cast("int"))
        .withColumn("customer_number", (_mod("row_id", 3_000) + 1).cast("int"))
        .withColumn("product_number", _subscription_product_number(F.col("subscription_number")).cast("int"))
        .withColumn("subscription_id", F.format_string("SUB-%06d", F.col("subscription_number")))
        .withColumn("customer_id", F.format_string("CLI-%06d", F.col("customer_number")))
        .withColumn("product_id", F.format_string("PRD-%03d", F.col("product_number")))
        .withColumn("line_id", F.format_string("LIN-%06d", F.col("subscription_number")))
        .withColumn("start_date", F.date_add(F.lit("2019-01-01").cast("date"), _mod(F.col("row_id") * 29, 2_760).cast("int")))
        .withColumn(
            "subscription_status",
            F.when(_mod("row_id", 83) == 0, "Baja").when(_mod("row_id", 47) == 0, "Suspendida").otherwise("Activa"),
        )
        .withColumn(
            "end_date",
            F.when(F.col("subscription_status") == "Baja", F.date_add(F.col("start_date"), (_mod(F.col("row_id") * 17, 1_200) + 30).cast("int"))).cast("date"),
        )
        .withColumn(
            "contract_type",
            F.when(F.col("product_number") <= 4, "Prepago")
            .when(_mod("row_id", 4) == 0, "24 meses").when(_mod("row_id", 4) == 1, "12 meses").otherwise("Sin permanencia"),
        )
        .withColumn("is_port_in", _mod("row_id", 10) < 4)
        .withColumn("is_esim", (F.col("product_number") >= 6) & (_mod("row_id", 10) < 3))
        .withColumn("autopay_enabled", (F.col("product_number") > 4) & (_mod("row_id", 10) < 6))
        .withColumn("base_price", _lookup(PRODUCT_PRICES, F.col("product_number") - 1).cast("double"))
        .withColumn("monthly_fee_usd", F.round(F.col("base_price") * (1.0 - _mod("row_id", 3) * 0.05), 2).cast("decimal(10,2)"))
        .withColumn("billing_day", F.when(F.col("product_number") > 4, _mod(F.col("row_id") * 7, 28) + 1).cast("int"))
        .withColumn(
            "subscription_id",
            F.when((F.col("row_id") > 0) & (_mod("row_id", 191) == 0), F.format_string("SUB-%06d", F.col("row_id")))
            .otherwise(F.col("subscription_id")),
        )
        .withColumn("customer_id", F.when(_mod("row_id", 233) == 0, "CLI-999999").otherwise(F.col("customer_id")))
        .withColumn("product_id", F.when(_mod("row_id", 263) == 0, "PRD-999").otherwise(F.col("product_id")))
        .withColumn("end_date", F.when(_mod("row_id", 277) == 0, F.date_sub(F.col("start_date"), 10)).otherwise(F.col("end_date")))
        .withColumn("monthly_fee_usd", F.when(_mod("row_id", 281) == 0, F.lit(-14.99).cast("decimal(10,2)")).otherwise(F.col("monthly_fee_usd")))
    )
    return df.select(
        "subscription_id", "customer_id", "product_id", "line_id", "start_date", "end_date",
        "subscription_status", "contract_type", "is_port_in", "is_esim", "autopay_enabled",
        "monthly_fee_usd", "billing_day",
    )


def build_usage_daily(spark: SparkSession, rows: int = 58_800, days: int = 14, seed: int = SEED) -> DataFrame:
    """Crea consumo diario de 4.200 suscripciones durante catorce días."""
    if rows // days != 4_200 or rows % days:
        raise ValueError("usage_daily espera 4.200 suscripciones por 14 días")
    df = (
        spark.range(rows).withColumnRenamed("id", "row_id")
        .withColumn("subscription_number", (F.floor(F.col("row_id") / days) + 1).cast("int"))
        .withColumn("day_index", _mod("row_id", days).cast("int"))
        .withColumn("product_number", _subscription_product_number(F.col("subscription_number")).cast("int"))
        .withColumn("usage_date", F.date_add(F.lit("2026-08-01").cast("date"), F.col("day_index")))
        .withColumn("subscription_id", F.format_string("SUB-%06d", F.col("subscription_number")))
        .withColumn("primary_cell_number", (_mod(F.col("subscription_number") * 7 + F.col("day_index") * 11, 180) + 1).cast("int"))
        .withColumn("primary_cell_id", F.format_string("CEL-%05d", F.col("primary_cell_number")))
        .withColumn("allowance_gb", _lookup(PRODUCT_ALLOWANCES, F.col("product_number") - 1).cast("double"))
        .withColumn(
            "data_gb",
            F.round(
                F.greatest(
                    F.lit(0.0), F.col("allowance_gb") / 30 * (0.45 + F.rand(seed) * 1.35)
                    * F.when(F.dayofweek("usage_date").isin(1, 7), 1.25).otherwise(1.0),
                ),
                3,
            ),
        )
        .withColumn("voice_minutes", F.round(F.abs(F.randn(seed + 1)) * 9 + _mod("subscription_number", 7), 2))
        .withColumn("sms_count", _mod(F.col("row_id") * 13, 18).cast("int"))
        .withColumn(
            "data_5g_pct",
            F.when(F.col("product_number").isin(1, 5), 0.0).otherwise(F.round(F.least(F.lit(100.0), 25.0 + F.rand(seed + 2) * 75), 2)),
        )
        .withColumn("roaming_data_mb", F.when(_mod("row_id", 29) == 0, F.round(F.rand(seed + 3) * 850, 2)).otherwise(0.0))
        .withColumn(
            "dropped_calls",
            F.greatest(F.lit(0), F.round(_mod("primary_cell_number", 17) / 8 + F.randn(seed + 4) * 0.8).cast("int")),
        )
        .withColumn(
            "recharge_amount_usd",
            F.when(
                # Un paquete prepago es mensual: una recarga cada 30 días por el precio del paquete.
                (F.col("product_number") <= 4) & (_mod(F.col("day_index") + F.col("subscription_number"), 30) == 0),
                _lookup(PRODUCT_PRICES, F.col("product_number") - 1),
            ).cast("decimal(10,2)"),
        )
        .withColumn("monthly_price", _lookup(PRODUCT_PRICES, F.col("product_number") - 1).cast("double"))
        .withColumn(
            "billed_amount_usd",
            F.round(
                F.when(F.col("product_number") <= 4, F.coalesce(F.col("recharge_amount_usd"), F.lit(0)))
                .otherwise(
                    F.col("monthly_price") / 30
                    # Exceso de datos a USD 0,10 por GB; los planes ilimitados (12 y 16) no lo pagan.
                    + F.when(F.col("product_number").isin(12, 16), F.lit(0.0))
                    .otherwise(F.greatest(F.lit(0.0), F.col("data_gb") - F.col("allowance_gb") / 30) * 0.10)
                ),
                2,
            ).cast("decimal(10,2)"),
        )
        .withColumn("ingestion_batch_id", F.date_format("usage_date", "'USAGE-'yyyyMMdd"))
        .withColumn("subscription_id", F.when(_mod("row_id", 251) == 0, "SUB-999999").otherwise(F.col("subscription_id")))
        .withColumn(
            "primary_cell_id",
            F.when(_mod("row_id", 211) == 0, F.lit(None).cast("string"))
            .when(_mod("row_id", 223) == 0, "CEL-99999").otherwise(F.col("primary_cell_id")),
        )
        .withColumn("data_gb", F.when(_mod("row_id", 257) == 0, -1.5).otherwise(F.col("data_gb")))
        .withColumn("data_5g_pct", F.when(_mod("row_id", 307) == 0, 130.0).otherwise(F.col("data_5g_pct")))
        .withColumn(
            "usage_date",
            F.when((F.col("row_id") > 0) & (_mod("row_id", 101) == 0), F.date_sub(F.col("usage_date"), 1)).otherwise(F.col("usage_date")),
        )
    )
    return df.select(
        "usage_date", "subscription_id", "primary_cell_id", "data_gb", "voice_minutes",
        "sms_count", "data_5g_pct", "roaming_data_mb", "dropped_calls", "recharge_amount_usd",
        "billed_amount_usd", "ingestion_batch_id",
    )


def build_support_tickets(spark: SparkSession, rows: int = 1_500, seed: int = SEED) -> DataFrame:
    """Crea tickets de red, producto y atención correlacionables."""
    df = (
        spark.range(rows).withColumnRenamed("id", "row_id")
        .withColumn("ticket_number", (F.col("row_id") + 1).cast("int"))
        .withColumn("subscription_number", (_mod(F.col("row_id") * 17, 4_200) + 1).cast("int"))
        .withColumn("customer_number", (_mod(F.col("subscription_number") - 1, 3_000) + 1).cast("int"))
        .withColumn("cell_number", (_mod(F.col("subscription_number") * 7, 180) + 1).cast("int"))
        .withColumn("ticket_id", F.format_string("TKT-%06d", F.col("ticket_number")))
        .withColumn("customer_id", F.format_string("CLI-%06d", F.col("customer_number")))
        .withColumn("subscription_id", F.format_string("SUB-%06d", F.col("subscription_number")))
        .withColumn("category", _lookup(["Cobertura", "Velocidad", "Llamadas", "Facturación", "Plan", "Portabilidad"], _mod("row_id", 6)))
        .withColumn("cell_id", F.when(F.col("category").isin("Cobertura", "Velocidad", "Llamadas"), F.format_string("CEL-%05d", F.col("cell_number"))))
        .withColumn("created_at", _timestamp_from_seconds(_mod(F.col("row_id") * 743, 1_209_600)))
        .withColumn("severity", _lookup(["Crítica", "Alta", "Alta", "Media", "Media", "Baja"], _mod("row_id", 6)))
        .withColumn(
            "ticket_status",
            F.when(_mod("row_id", 10) < 5, "Cerrado").when(_mod("row_id", 10) < 7, "Resuelto")
            .when(_mod("row_id", 10) < 9, "En progreso").otherwise("Abierto"),
        )
        .withColumn("channel", _lookup(["App", "Web", "Call center", "Tienda", "WhatsApp"], _mod("row_id", 5)))
        .withColumn(
            "sla_target_hours",
            F.when(F.col("severity") == "Crítica", 4).when(F.col("severity") == "Alta", 8)
            .when(F.col("severity") == "Media", 24).otherwise(72),
        )
        .withColumn("resolution_hours", F.when(F.col("ticket_status").isin("Cerrado", "Resuelto"), F.round(2.0 + F.rand(seed) * 70, 2)))
        .withColumn(
            "resolved_at",
            F.when(F.col("resolution_hours").isNotNull(), _timestamp_add_hours(F.col("created_at"), F.ceil(F.col("resolution_hours")))).cast("timestamp"),
        )
        .withColumn("first_contact_resolution", F.when(F.col("ticket_status").isin("Cerrado", "Resuelto"), _mod("row_id", 10) < 6))
        .withColumn(
            "post_service_csat",
            F.when(
                F.col("ticket_status").isin("Cerrado", "Resuelto") & (_mod("row_id", 4) != 0),
                (F.lit(5) - F.least(F.lit(4), F.floor(F.col("resolution_hours") / 18))).cast("int"),
            ),
        )
        .withColumn(
            "ticket_id",
            F.when((F.col("row_id") > 0) & (_mod("row_id", 211) == 0), F.format_string("TKT-%06d", F.col("row_id")))
            .otherwise(F.col("ticket_id")),
        )
        .withColumn("customer_id", F.when(_mod("row_id", 307) == 0, "CLI-999999").otherwise(F.col("customer_id")))
        .withColumn("subscription_id", F.when(_mod("row_id", 293) == 0, "SUB-999999").otherwise(F.col("subscription_id")))
        .withColumn("cell_id", F.when(_mod("row_id", 283) == 0, "CEL-99999").otherwise(F.col("cell_id")))
        .withColumn("category", F.when(_mod("row_id", 271) == 0, F.lit(None).cast("string")).otherwise(F.col("category")))
        .withColumn("resolved_at", F.when(_mod("row_id", 263) == 0, _timestamp_add_hours(F.col("created_at"), F.lit(-1))).otherwise(F.col("resolved_at")))
        .withColumn("resolution_hours", F.when(_mod("row_id", 257) == 0, -3.0).otherwise(F.col("resolution_hours")))
    )
    return df.select(
        "ticket_id", "customer_id", "subscription_id", "cell_id", "created_at", "resolved_at",
        "category", "severity", "ticket_status", "channel", "first_contact_resolution",
        "sla_target_hours", "resolution_hours", "post_service_csat",
    )


def build_customer_surveys(spark: SparkSession, rows: int = 800) -> DataFrame:
    """Crea encuestas NPS/CSAT sin texto libre ni PII."""
    df = (
        spark.range(rows).withColumnRenamed("id", "row_id")
        .withColumn("survey_number", (F.col("row_id") + 1).cast("int"))
        .withColumn("subscription_number", (_mod(F.col("row_id") * 23, 4_200) + 1).cast("int"))
        .withColumn("customer_number", (_mod(F.col("subscription_number") - 1, 3_000) + 1).cast("int"))
        .withColumn("survey_id", F.format_string("ENC-%06d", F.col("survey_number")))
        .withColumn("customer_id", F.format_string("CLI-%06d", F.col("customer_number")))
        .withColumn("subscription_id", F.format_string("SUB-%06d", F.col("subscription_number")))
        .withColumn("response_date", F.date_add(F.lit("2026-08-01").cast("date"), _mod(F.col("row_id") * 3, 14).cast("int")))
        .withColumn("survey_trigger", _lookup(["Periódica", "Cierre de ticket", "Alta", "Cambio de plan"], _mod("row_id", 4)))
        .withColumn("nps_score", _mod(F.col("row_id") * 7 + _mod("subscription_number", 5), 11).cast("int"))
        .withColumn("csat_score", F.greatest(F.lit(1), F.ceil((F.col("nps_score") + 1) / 2.2)).cast("int"))
        .withColumn("experience_area", _lookup(["Red", "Red", "Atención", "Producto", "Facturación", "App"], _mod("row_id", 6)))
        .withColumn(
            "feedback_category",
            _lookup(["Cobertura insuficiente", "Velocidad variable", "Atención satisfactoria", "Plan adecuado", "Cobro claro", "App fácil de usar"], _mod("row_id", 6)),
        )
        .withColumn("survey_channel", _lookup(["App", "Web", "SMS"], _mod("row_id", 3)))
        .withColumn(
            "survey_id",
            F.when((F.col("row_id") > 0) & (_mod("row_id", 211) == 0), F.format_string("ENC-%06d", F.col("row_id")))
            .otherwise(F.col("survey_id")),
        )
        .withColumn("customer_id", F.when(_mod("row_id", 199) == 0, "CLI-999999").otherwise(F.col("customer_id")))
        .withColumn("subscription_id", F.when(_mod("row_id", 193) == 0, "SUB-999999").otherwise(F.col("subscription_id")))
        .withColumn("nps_score", F.when(_mod("row_id", 181) == 0, 12).otherwise(F.col("nps_score")))
        .withColumn("csat_score", F.when(_mod("row_id", 179) == 0, 7).otherwise(F.col("csat_score")))
    )
    return df.select(
        "survey_id", "customer_id", "subscription_id", "response_date", "survey_trigger",
        "nps_score", "csat_score", "experience_area", "feedback_category", "survey_channel",
    )


def build_all_datasets(spark: SparkSession, seed: int = SEED) -> dict[str, DataFrame]:
    """Construye las once fuentes aprobadas con cantidades exactas."""
    return {
        "network_sites": build_network_sites(spark, seed=seed),
        "radio_cells": build_radio_cells(spark),
        "cell_tower_metrics": build_tower_metrics(spark, seed=seed),
        "network_alarms": build_network_alarms(spark),
        "maintenance_orders": build_maintenance_orders(spark, seed=seed),
        "products": build_products(spark),
        "customers": build_customers(spark),
        "subscriptions": build_subscriptions(spark),
        "usage_daily": build_usage_daily(spark, seed=seed),
        "support_tickets": build_support_tickets(spark, seed=seed),
        "customer_surveys": build_customer_surveys(spark),
    }
