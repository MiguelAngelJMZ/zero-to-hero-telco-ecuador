"""Definición serializada del Genie Space del workshop.

La usa `notebooks/08_genie`. No contiene host,
warehouse, usuario ni credenciales; catálogo y schema se reciben como parámetros.

Sigue las buenas prácticas de curación de Genie: el significado de los datos vive en
superficies estructuradas (descripciones de fuentes, sinónimos por columna, Metric
Views y SQL de ejemplo) y las instrucciones de texto solo describen el comportamiento
esencial. Así el participante puede agregar reglas propias sin contradicciones.
"""

from __future__ import annotations

from typing import Any

TITLE = "Analista 360 de Red y Clientes"


def _id(number: int) -> str:
    """ID hexadecimal de 32 caracteres; el orden numérico conserva el orden requerido."""
    return f"{number:032x}"


def _qualified(catalog: str, schema: str, object_name: str) -> str:
    return f"{catalog}.{schema}.{object_name}"


def _column(name: str, synonyms: list[str] | None = None, description: str | None = None) -> dict[str, Any]:
    config: dict[str, Any] = {"column_name": name}
    if description:
        config["description"] = [description]
    if synonyms:
        config["synonyms"] = synonyms
    return config


# Fuentes del espacio: para qué sirve cada una y los términos de negocio de sus columnas.
METRIC_VIEWS = {
    "mv_network_quality": {
        "description": (
            "KPIs certificados de calidad de red por hora, sitio y tecnología. Fuente principal para "
            "disponibilidad, latencia, velocidad y cumplimiento de calidad de red. Consultar las medidas con MEASURE()."
        ),
        "columns": [
            _column(
                "Cumplimiento de calidad de red",
                ["calidad de red", "SLA de red", "SLA", "cumplimiento"],
                "Porcentaje de mediciones con disponibilidad >= 99 %, latencia <= 80 ms y downlink >= 20 Mbps a la vez. "
                "No mide tiempo de reparación.",
            ),
            _column("Disponibilidad media", ["disponibilidad", "uptime"]),
            _column("Downlink medio", ["velocidad", "bajada", "throughput", "velocidad de descarga"]),
            _column("Latencia media", ["latencia", "retardo", "RTT"], "RTT promedio en ms entre el dispositivo y un servidor de referencia."),
            _column("Provincia", ["provincia", "región administrativa"]),
        ],
    },
    "mv_customer_product_experience": {
        "description": (
            "KPIs certificados de clientes, productos, ingresos y experiencia por día. Fuente principal para "
            "ingresos, ARPU, consumo, tickets, NPS y riesgo de baja. Consultar las medidas con MEASURE()."
        ),
        "columns": [
            _column("Clientes de alto riesgo", ["clientes en riesgo", "riesgo de churn"]),
            _column(
                "Cumplimiento de calidad del cliente",
                ["calidad del cliente", "calidad por producto", "SLA del cliente", "SLA por producto"],
                "Calidad de red que experimentan las líneas del cliente o producto. La calidad global de la red "
                "está en mv_network_quality.",
            ),
            _column("Ingreso facturado", ["ingreso", "ingresos", "facturación", "revenue"]),
            _column("Ingreso por recargas", ["recargas"]),
            _column("Resolución en primer contacto", ["FCR"]),
            _column("Riesgo de baja", ["churn", "baja", "abandono"]),
            _column("Suscripciones activas", ["líneas", "líneas activas"]),
            _column("Tickets", ["reclamos", "casos", "incidencias de clientes"]),
        ],
    },
}

GOLD_TABLES = {
    "gold_customer_360": {
        "description": (
            "Una fila por cliente con su consumo, ingresos, atención, experiencia y riesgo de baja. Usar para "
            "detalle o listados de clientes; para KPIs agregados preferir mv_customer_product_experience."
        ),
        "columns": [
            _column("churn_risk_band", ["riesgo de baja", "churn"], "Banda de riesgo de baja: Alto, Medio o Bajo."),
            _column("region", ["provincia"], "Provincia del cliente."),
        ],
    },
    "gold_incident_impact": {
        "description": (
            "Una fila por alarma de red con su mantenimiento, clientes y líneas impactadas e ingreso en riesgo. "
            "Usar para preguntas sobre incidentes y su impacto de negocio."
        ),
        "columns": [
            _column("billed_revenue_at_risk_usd", ["ingreso en riesgo"], "Ingreso facturado de las líneas impactadas, en USD."),
            _column("region", ["provincia"], "Provincia del sitio afectado."),
            _column("severity", ["severidad", "gravedad"], "Severidad técnica de la alarma: Crítica, Alta, Media o Baja."),
        ],
    },
    "gold_site_daily_360": {
        "description": (
            "Una fila por sitio y día con red, alarmas, tickets y mantenimiento. Usar para detalle de sitios; "
            "para KPIs de red agregados preferir mv_network_quality."
        ),
        "columns": [
            _column(
                "network_sla_compliance_pct",
                ["calidad de red", "SLA de red"],
                "Cumplimiento de calidad de red del sitio en el día: % de mediciones con disponibilidad >= 99 %, "
                "latencia <= 80 ms y downlink >= 20 Mbps.",
            ),
            _column("region", ["provincia"], "Provincia del sitio."),
            _column("site_health_status", ["estado del sitio", "salud del sitio"], "Estado diario: Saludable, Atención o Crítico."),
        ],
    },
}


def text_instructions(extra_disambiguation: list[str] | None = None) -> str:
    """Instrucciones esenciales con los encabezados recomendados por Databricks.

    `extra_disambiguation` agrega reglas del participante dentro de DISAMBIGUATION, antes
    del bloque de resúmenes, que debe ir siempre al final.
    """

    disambiguation = [
        "Los datos son de agosto de 2026. 'Últimos días', 'esta semana' o 'recientemente' se refieren a "
        "ese período, nunca a la fecha actual.",
        "Si piden 'el mejor' o 'el peor' sin indicar la métrica, pregunta: '¿Quieres ordenar por calidad de red, "
        "disponibilidad, latencia o ingreso?'",
        *(extra_disambiguation or []),
    ]
    sections = [
        ("PURPOSE", [
            "Analizas la red y los clientes de Equinoccio Móvil, un operador móvil ecuatoriano ficticio con "
            "datos sintéticos.",
            "Los usuarios son equipos técnicos y de operaciones. Responde siempre en español.",
        ]),
        ("DISAMBIGUATION", disambiguation),
        ("DATA QUALITY NOTES", [
            "Algunas filas no tienen provincia o producto: son referencias inválidas intencionales del "
            "workshop. Muéstralas como 'Sin asignar' en lugar de omitirlas sin aviso.",
        ]),
        ("CONSTRAINTS", [
            "Los IDs CLI-*, SUB-* y LIN-* son seudónimos: no intentes inferir nombres, cédulas, teléfonos "
            "ni otros datos personales.",
            "No menciones operadoras ni empresas reales.",
        ]),
        ("Instructions you must follow when providing summaries", [
            "Indica el período, los filtros y las métricas utilizadas.",
            "Muestra montos en USD con dos decimales y porcentajes con un decimal.",
            "Distingue correlación de causalidad.",
        ]),
    ]
    return "\n\n".join(
        f"## {header}\n" + "\n".join(f"- {bullet}" for bullet in bullets) for header, bullets in sections
    )


def build_serialized_space(
    catalog: str, schema: str, extra_disambiguation: list[str] | None = None
) -> dict[str, Any]:
    namespace = f"{catalog}.{schema}"
    network_metric = _qualified(catalog, schema, "mv_network_quality")
    customer_metric = _qualified(catalog, schema, "mv_customer_product_experience")

    sample_questions = [
        {
            "id": _id(1),
            "question": [
                "Compara disponibilidad, latencia, throughput y calidad de red por provincia y tecnología. "
                "Muestra las tres combinaciones con peor desempeño."
            ],
        },
        {
            "id": _id(2),
            "question": [
                "¿Qué cinco productos tienen mayor ARPU y cómo se comparan en consumo, "
                "tickets por 100 líneas, NPS y calidad de red?"
            ],
        },
    ]

    # Ejemplos que enseñan patrones de consulta; no repiten las preguntas del benchmark.
    examples = [
        {
            "id": _id(10),
            "question": ["¿Cuál es la calidad de red por región natural?"],
            "sql": [
                "SELECT `Región natural`, ROUND(MEASURE(`Cumplimiento de calidad de red`), 1) AS calidad_red_pct\n"
                f"FROM {network_metric}\n"
                "GROUP BY ALL\nORDER BY calidad_red_pct"
            ],
            "usage_guidance": ["Patrón para KPIs de red: dimensiones de mv_network_quality y medidas con MEASURE()."],
        },
        {
            "id": _id(11),
            "question": ["¿Cuál es el ingreso facturado y el NPS por familia de producto?"],
            "sql": [
                "SELECT `Familia de producto`, ROUND(MEASURE(`Ingreso facturado`), 2) AS ingreso_usd, "
                "ROUND(MEASURE(`NPS`), 1) AS nps\n"
                f"FROM {customer_metric}\n"
                "GROUP BY ALL\nORDER BY ingreso_usd DESC"
            ],
            "usage_guidance": ["Patrón para KPIs de clientes y productos con mv_customer_product_experience."],
        },
        {
            "id": _id(12),
            "question": ["Muestra los incidentes con mayor impacto de negocio"],
            "sql": [
                "SELECT alarm_id, region AS provincia, canton, severity, impacted_customers, "
                "impacted_subscriptions, ROUND(billed_revenue_at_risk_usd, 2) AS ingreso_en_riesgo_usd, business_impact\n"
                f"FROM {namespace}.gold_incident_impact\n"
                "ORDER BY impacted_customers DESC, affected_users_est DESC\nLIMIT 20"
            ],
            "usage_guidance": ["Detalle de incidentes: usar gold_incident_impact."],
        },
        {
            "id": _id(13),
            "question": ["¿Qué sitios combinan baja calidad de red y tickets abiertos?"],
            "sql": [
                "SELECT event_date, site_id, region AS provincia, canton, "
                "ROUND(network_sla_compliance_pct, 1) AS calidad_red_pct, open_tickets, site_health_status\n"
                f"FROM {namespace}.gold_site_daily_360\n"
                "WHERE network_sla_compliance_pct < 55 AND open_tickets > 0\n"
                "ORDER BY network_sla_compliance_pct, open_tickets DESC"
            ],
            "usage_guidance": ["Detalle diario de sitios: usar gold_site_daily_360."],
        },
    ]

    # Benchmarks: las preguntas sugeridas, con SQL verificado y distinto de los ejemplos.
    benchmark_questions = [
        {
            "id": _id(20),
            "question": sample_questions[0]["question"],
            "answer": [{
                "format": "SQL",
                "content": [
                    "SELECT `Provincia`, `Tecnología`, ROUND(MEASURE(`Disponibilidad media`), 1) AS disponibilidad_pct, "
                    "ROUND(MEASURE(`Latencia media`), 1) AS latencia_ms, ROUND(MEASURE(`Downlink medio`), 1) AS downlink_mbps, "
                    "ROUND(MEASURE(`Cumplimiento de calidad de red`), 1) AS calidad_red_pct\n"
                    f"FROM {network_metric}\n"
                    "WHERE `Provincia` IS NOT NULL\n"
                    "GROUP BY ALL\nORDER BY calidad_red_pct\nLIMIT 3"
                ],
            }],
        },
        {
            "id": _id(21),
            "question": sample_questions[1]["question"],
            "answer": [{
                "format": "SQL",
                "content": [
                    "SELECT `Producto`, ROUND(MEASURE(`ARPU`), 2) AS arpu_usd, ROUND(MEASURE(`Consumo de datos`), 1) AS consumo_gb, "
                    "ROUND(MEASURE(`Tickets por 100 líneas`), 1) AS tickets_100_lineas, ROUND(MEASURE(`NPS`), 1) AS nps, "
                    "ROUND(MEASURE(`Cumplimiento de calidad del cliente`), 1) AS calidad_cliente_pct\n"
                    f"FROM {customer_metric}\n"
                    "WHERE `Producto` IS NOT NULL\n"
                    "GROUP BY ALL\nORDER BY arpu_usd DESC\nLIMIT 5"
                ],
            }],
        },
    ]

    def source(identifier: str, spec: dict[str, Any]) -> dict[str, Any]:
        return {
            "identifier": identifier,
            "description": [spec["description"]],
            "column_configs": sorted(spec["columns"], key=lambda column: column["column_name"]),
        }

    return {
        "version": 2,
        "config": {"sample_questions": sample_questions},
        "data_sources": {
            "tables": sorted(
                (source(_qualified(catalog, schema, name), spec) for name, spec in GOLD_TABLES.items()),
                key=lambda item: item["identifier"],
            ),
            "metric_views": sorted(
                (source(_qualified(catalog, schema, name), spec) for name, spec in METRIC_VIEWS.items()),
                key=lambda item: item["identifier"],
            ),
        },
        "instructions": {
            "text_instructions": [{"id": _id(5), "content": [text_instructions(extra_disambiguation)]}],
            "example_question_sqls": examples,
        },
        "benchmarks": {"questions": benchmark_questions},
    }
