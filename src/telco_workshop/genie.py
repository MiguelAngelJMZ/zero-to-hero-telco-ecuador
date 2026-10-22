"""Definición serializada del Genie Space del workshop.

La usa `notebooks/08_genie`. No contiene host,
warehouse, usuario ni credenciales; catálogo y schema se reciben como parámetros.
"""

from __future__ import annotations

from typing import Any

TITLE = "Analista 360 de Red y Clientes"


def _id(number: int) -> str:
    """ID hexadecimal de 32 caracteres; el orden numérico conserva el orden requerido."""
    return f"{number:032x}"


def _qualified(catalog: str, schema: str, object_name: str) -> str:
    return f"{catalog}.{schema}.{object_name}"


def _metric_sql(namespace: str, metric_view: str, select_clause: str, suffix: str) -> list[str]:
    return [f"SELECT {select_clause}\nFROM {namespace}.{metric_view}\n{suffix}"]


def build_serialized_space(catalog: str, schema: str) -> dict[str, Any]:
    namespace = f"{catalog}.{schema}"
    network_metric = _qualified(catalog, schema, "mv_network_quality")
    customer_metric = _qualified(catalog, schema, "mv_customer_product_experience")

    sample_questions = [
        {
            "id": _id(1),
            "question": [
                "Compara disponibilidad, latencia, throughput y SLA por provincia y tecnología. "
                "Muestra las tres combinaciones con peor desempeño."
            ],
        },
        {
            "id": _id(2),
            "question": [
                "¿Qué cinco productos tienen mayor ARPU y cómo se comparan en consumo, "
                "tickets por 100 líneas, NPS y SLA de red?"
            ],
        },
    ]

    instructions = (
        "Responde siempre en español. Analiza Equinoccio Móvil, un operador móvil ecuatoriano ficticio. "
        "Usa las Metric Views para KPIs certificados y las Gold solo para detalle. "
        "En SQL sobre Metric Views usa MEASURE(). Interpreta importes en dólares estadounidenses (USD). La columna region contiene la provincia; la región natural (Costa, Sierra, Amazonía, Insular) es `Región natural`. "
        "En mv_customer_product_experience, 'SLA del cliente' o 'SLA de red por producto' "
        "corresponde a MEASURE(`Cumplimiento SLA de cliente`). Para el SLA global de red usa "
        "MEASURE(`Cumplimiento SLA de red`) en mv_network_quality. "
        "El período sintético es agosto de 2026; no uses la fecha actual para 'últimos días'. "
        "Muestra filtros, período, dimensiones y medidas. Diferencia correlación de causalidad "
        "y declara supuestos y limitaciones. Los IDs CLI-*, LIN-* y SUB-* son ficticios. "
        "No infieras PII ni menciones empresas reales. "
        "Sinónimos de negocio: disponibilidad = `Disponibilidad media`; velocidad, bajada o "
        "throughput = `Downlink medio`; SLA de red = `Cumplimiento SLA de red`; SLA por cliente o "
        "producto = `Cumplimiento SLA de cliente`; ingreso o facturación = `Ingreso facturado`; "
        "recargas = `Ingreso por recargas`; líneas = `Suscripciones activas`; reclamos = `Tickets`; "
        "FCR = `Resolución en primer contacto`; riesgo, churn o baja = campo `Riesgo de baja` y "
        "medida `Clientes de alto riesgo`."
    )

    examples = [
        {
            "id": _id(10),
            "question": ["Compara calidad de red por provincia y tecnología"],
            "sql": _metric_sql(
                namespace,
                "mv_network_quality",
                "`Provincia`, `Tecnología`, "
                "MEASURE(`Disponibilidad media`) AS disponibilidad_pct, "
                "MEASURE(`Latencia media`) AS latencia_ms, "
                "MEASURE(`Downlink medio`) AS downlink_mbps, "
                "MEASURE(`Cumplimiento SLA de red`) AS sla_pct",
                "GROUP BY `Provincia`, `Tecnología` ORDER BY sla_pct",
            ),
        },
        {
            "id": _id(11),
            "question": ["Muestra los cinco productos con mayor ARPU y su NPS"],
            "sql": _metric_sql(
                namespace,
                "mv_customer_product_experience",
                "`Producto`, MEASURE(`ARPU`) AS arpu_usd, "
                "MEASURE(`Consumo de datos`) AS consumo_gb, "
                "MEASURE(`NPS`) AS nps, MEASURE(`Tickets por 100 líneas`) AS tickets_100, "
                "MEASURE(`Cumplimiento SLA de cliente`) AS sla_cliente_pct",
                "GROUP BY `Producto` ORDER BY arpu_usd DESC LIMIT 5",
            ),
        },
        {
            "id": _id(12),
            "question": ["¿Cuántos clientes de alto riesgo existen por provincia y segmento?"],
            "sql": [
                f"SELECT region, customer_segment, COUNT(DISTINCT customer_id) AS clientes_alto_riesgo\n"
                f"FROM {namespace}.gold_customer_360\n"
                "WHERE churn_risk_band = 'Alto'\n"
                "GROUP BY region, customer_segment\n"
                "ORDER BY clientes_alto_riesgo DESC"
            ],
        },
        {
            "id": _id(13),
            "question": ["Muestra los incidentes con mayor impacto de negocio"],
            "sql": [
                f"SELECT alarm_id, region, canton, severity, impacted_customers, "
                "impacted_subscriptions, billed_revenue_at_risk_usd, business_impact\n"
                f"FROM {namespace}.gold_incident_impact\n"
                "ORDER BY impacted_customers DESC, affected_users_est DESC\n"
                "LIMIT 20"
            ],
        },
        {
            "id": _id(14),
            "question": ["¿Qué sitios combinan SLA bajo y tickets abiertos?"],
            "sql": [
                f"SELECT event_date, site_id, region, canton, network_sla_compliance_pct, "
                "open_tickets, site_health_status\n"
                f"FROM {namespace}.gold_site_daily_360\n"
                "WHERE network_sla_compliance_pct < 95 AND open_tickets > 0\n"
                "ORDER BY network_sla_compliance_pct, open_tickets DESC"
            ],
        },
    ]

    benchmark_questions = [
        {
            "id": _id(20 + index),
            "question": example["question"],
            "answer": [{"format": "SQL", "content": example["sql"]}],
        }
        for index, example in enumerate(examples)
    ]

    table_identifiers = sorted(
        _qualified(catalog, schema, name)
        for name in ("gold_customer_360", "gold_incident_impact", "gold_site_daily_360")
    )
    metric_identifiers = sorted([customer_metric, network_metric])

    return {
        "version": 2,
        "config": {"sample_questions": sample_questions},
        "data_sources": {
            "tables": [
                {"identifier": identifier, "description": ["Tabla Gold sintética y gobernada del workshop"]}
                for identifier in table_identifiers
            ],
            "metric_views": [
                {"identifier": identifier, "description": ["Metric View certificada del workshop"]}
                for identifier in metric_identifiers
            ],
        },
        "instructions": {
            "text_instructions": [{"id": _id(5), "content": [instructions]}],
            "example_question_sqls": examples,
        },
        "benchmarks": {"questions": benchmark_questions},
    }

