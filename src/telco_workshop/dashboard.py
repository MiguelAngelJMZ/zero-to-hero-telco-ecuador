"""Definición serializada del AI/BI Dashboard de referencia del workshop.

Sigue el formato de AI/BI Dashboards: cada widget lee de una consulta llamada
`main_query`, las versiones de spec dependen del tipo de widget y las consultas usan
nombres de tabla simples; el catálogo y el schema se pasan al crear el dashboard
(`dataset_catalog` / `dataset_schema`). La usa `notebooks/07_dashboard`.
"""

from __future__ import annotations

from typing import Any

DEFAULT_TITLE = "Experiencia móvil 360"

# Paleta neutra: azul, verde, ámbar y rojo, más tonos de apoyo.
PALETTE = ["#2272B4", "#00A972", "#FFAB00", "#E5484D", "#8BCAE7", "#5C6B7A", "#99DDB4"]

PERCENT = {"type": "number-percent", "decimalPlaces": {"type": "max", "places": 1}}
USD = {
    "type": "number-currency",
    "currencyCode": "USD",
    "abbreviation": "compact",
    "decimalPlaces": {"type": "max", "places": 2},
}


def _dataset(name: str, display_name: str, sql: str) -> dict[str, Any]:
    # `queryLines` se concatena sin separador: una sola línea normalizada evita uniones
    # inválidas como `active_customersFROM`.
    normalized_sql = " ".join(line.strip() for line in sql.splitlines() if line.strip())
    return {"name": name, "displayName": display_name, "queryLines": [normalized_sql]}


def _field(name: str, expression: str | None = None) -> dict[str, str]:
    return {"name": name, "expression": expression or f"`{name}`"}


def _main_query(dataset: str, fields: list[dict[str, str]], disaggregated: bool) -> list[dict[str, Any]]:
    return [
        {
            "name": "main_query",
            "query": {"datasetName": dataset, "fields": fields, "disaggregated": disaggregated},
        }
    ]


def _frame(title: str) -> dict[str, Any]:
    return {"showTitle": True, "title": title}


def _counter(
    name: str, dataset: str, title: str, field: dict[str, str], number_format: dict | None
) -> dict[str, Any]:
    value = {"fieldName": field["name"], "displayName": title}
    if number_format:
        value["format"] = number_format
    return {
        "name": name,
        "queries": _main_query(dataset, [field], False),
        "spec": {"version": 2, "widgetType": "counter", "encodings": {"value": value}, "frame": _frame(title)},
    }


def _filter(name: str, title: str, field: str, datasets: list[str]) -> dict[str, Any]:
    queries = [
        {
            "name": f"{dataset}_{field}",
            "query": {"datasetName": dataset, "fields": [_field(field)], "disaggregated": False},
        }
        for dataset in datasets
    ]
    return {
        "name": name,
        "queries": queries,
        "spec": {
            "version": 2,
            "widgetType": "filter-multi-select",
            "encodings": {
                "fields": [
                    {"fieldName": field, "displayName": title, "queryName": query["name"]} for query in queries
                ]
            },
            "frame": _frame(title),
        },
    }


def build_serialized_dashboard(catalog: str, schema: str) -> dict[str, Any]:
    """`catalog` y `schema` se reciben por compatibilidad; las consultas usan nombres simples."""

    datasets = [
        _dataset(
            "network_daily",
            "Calidad de red diaria",
            """SELECT
  `Fecha` AS event_date,
  `Provincia` AS region,
  `Tecnología` AS technology,
  MEASURE(`Disponibilidad media`) / 100 AS availability_pct,
  MEASURE(`Latencia media`) AS latency_ms,
  MEASURE(`Downlink medio`) AS downlink_mbps,
  MEASURE(`Cumplimiento de calidad de red`) / 100 AS network_sla_pct
FROM mv_network_quality
GROUP BY ALL""",
        ),
        _dataset(
            "customers",
            "Clientes",
            """SELECT region, customer_segment, COUNT(DISTINCT customer_id) AS active_customers
FROM gold_customer_360
GROUP BY ALL""",
        ),
        _dataset(
            "customer_product",
            "Clientes y productos",
            """SELECT
  `Provincia` AS region,
  `Segmento` AS customer_segment,
  `Producto` AS product_name,
  MEASURE(`Clientes activos`) AS active_customers,
  MEASURE(`ARPU`) AS arpu_usd,
  MEASURE(`NPS`) AS nps,
  MEASURE(`Tickets por 100 líneas`) AS tickets_per_100_lines
FROM mv_customer_product_experience
WHERE `Provincia` IS NOT NULL AND `Segmento` IS NOT NULL
GROUP BY ALL""",
        ),
        _dataset(
            "priority_sites",
            "Sitios prioritarios",
            """SELECT event_date, region, canton, site_id, site_health_status,
  avg_availability_pct, avg_latency_ms, avg_downlink_mbps, open_tickets, impacted_subscriptions
FROM gold_site_daily_360
ORDER BY avg_latency_ms DESC, open_tickets DESC""",
        ),
        _dataset(
            "incidents",
            "Impacto de incidentes",
            """SELECT opened_date, region, canton, severity, business_impact,
  impacted_customers, impacted_subscriptions, billed_revenue_at_risk_usd
FROM gold_incident_impact""",
        ),
    ]

    header = {
        "name": "encabezado",
        "multilineTextboxSpec": {
            "lines": [
                "## Experiencia móvil 360\n",
                "\n",
                "Calidad de red, clientes e incidentes de un operador ficticio (datos sintéticos, agosto de 2026).",
            ]
        },
    }

    sla_bar = {
        "name": "sla-por-region",
        "queries": _main_query(
            "network_daily",
            [_field("region"), _field("technology"), _field("avg(network_sla_pct)", "AVG(`network_sla_pct`)")],
            False,
        ),
        "spec": {
            "version": 3,
            "widgetType": "bar",
            "encodings": {
                "x": {"fieldName": "region", "scale": {"type": "categorical"}, "displayName": "Provincia"},
                "y": {
                    "fieldName": "avg(network_sla_pct)",
                    "scale": {"type": "quantitative"},
                    "displayName": "Calidad de red",
                    "format": PERCENT,
                },
                "color": {"fieldName": "technology", "scale": {"type": "categorical"}, "displayName": "Tecnología"},
            },
            "mark": {"layout": "group"},
            "frame": _frame("Cumplimiento de calidad de red por provincia"),
        },
    }

    arpu_scatter = {
        "name": "arpu-vs-nps",
        "queries": _main_query(
            "customer_product",
            [
                _field("product_name"),
                _field("customer_segment"),
                _field("arpu_usd"),
                _field("nps"),
                _field("active_customers"),
            ],
            True,
        ),
        "spec": {
            "version": 3,
            "widgetType": "scatter",
            "encodings": {
                "x": {"fieldName": "arpu_usd", "scale": {"type": "quantitative"}, "displayName": "ARPU (USD)", "format": USD},
                "y": {"fieldName": "nps", "scale": {"type": "quantitative"}, "displayName": "NPS"},
                "size": {"fieldName": "active_customers", "scale": {"type": "quantitative"}, "displayName": "Clientes"},
                "color": {"fieldName": "customer_segment", "scale": {"type": "categorical"}, "displayName": "Segmento"},
            },
            "frame": _frame("ARPU frente a NPS"),
        },
    }

    site_columns = [
        ("site_id", "Sitio", None),
        ("region", "Provincia", None),
        ("canton", "Cantón", None),
        ("site_health_status", "Estado", None),
        ("avg_latency_ms", "Latencia (ms)", None),
        ("avg_downlink_mbps", "Downlink (Mbps)", None),
        ("open_tickets", "Tickets abiertos", None),
        ("impacted_subscriptions", "Suscripciones impactadas", None),
    ]
    sites_table = {
        "name": "sitios-prioritarios",
        "queries": _main_query("priority_sites", [_field(name) for name, _, _ in site_columns], True),
        "spec": {
            "version": 2,
            "widgetType": "table",
            "encodings": {"columns": [{"fieldName": name, "displayName": title} for name, title, _ in site_columns]},
            "frame": _frame("Sitios que requieren atención"),
        },
    }

    revenue_bar = {
        "name": "ingreso-en-riesgo",
        "queries": _main_query(
            "incidents",
            [
                _field("severity"),
                _field("business_impact"),
                _field("sum(billed_revenue_at_risk_usd)", "SUM(`billed_revenue_at_risk_usd`)"),
            ],
            False,
        ),
        "spec": {
            "version": 3,
            "widgetType": "bar",
            "encodings": {
                "x": {
                    "fieldName": "severity",
                    "scale": {
                        "type": "categorical",
                        "sort": {"by": "custom-order", "orderedValues": ["Crítica", "Alta", "Media", "Baja"]},
                    },
                    "displayName": "Severidad",
                },
                "y": {
                    "fieldName": "sum(billed_revenue_at_risk_usd)",
                    "scale": {"type": "quantitative"},
                    "displayName": "Ingreso en riesgo (USD)",
                    "format": USD,
                },
                "color": {
                    "fieldName": "business_impact",
                    "scale": {
                        "type": "categorical",
                        "mappings": [
                            {"value": "Crítico", "color": "#E5484D"},
                            {"value": "Alto", "color": "#FFAB00"},
                            {"value": "Moderado", "color": "#2272B4"},
                        ],
                    },
                    "displayName": "Impacto",
                },
            },
            "frame": _frame("Ingreso en riesgo por severidad"),
        },
    }

    canvas = [
        {"widget": header, "position": {"x": 0, "y": 0, "width": 12, "height": 2}},
        {
            "widget": _counter(
                "kpi-disponibilidad",
                "network_daily",
                "Disponibilidad media",
                _field("avg(availability_pct)", "AVG(`availability_pct`)"),
                {"type": "number-percent", "decimalPlaces": {"type": "max", "places": 2}},
            ),
            "position": {"x": 0, "y": 2, "width": 4, "height": 3},
        },
        {
            "widget": _counter(
                "kpi-latencia",
                "network_daily",
                "Latencia media (ms)",
                _field("avg(latency_ms)", "AVG(`latency_ms`)"),
                {"type": "number-plain", "decimalPlaces": {"type": "max", "places": 1}},
            ),
            "position": {"x": 4, "y": 2, "width": 4, "height": 3},
        },
        {
            "widget": _counter(
                "kpi-clientes",
                "customers",
                "Clientes activos",
                _field("sum(active_customers)", "SUM(`active_customers`)"),
                None,
            ),
            "position": {"x": 8, "y": 2, "width": 4, "height": 3},
        },
        {"widget": sla_bar, "position": {"x": 0, "y": 5, "width": 6, "height": 6}},
        {"widget": arpu_scatter, "position": {"x": 6, "y": 5, "width": 6, "height": 6}},
        {"widget": sites_table, "position": {"x": 0, "y": 11, "width": 12, "height": 7}},
        {"widget": revenue_bar, "position": {"x": 0, "y": 18, "width": 12, "height": 6}},
    ]

    filters = [
        {
            "widget": _filter(
                "filtro-region",
                "Provincia",
                "region",
                ["network_daily", "customers", "customer_product", "priority_sites", "incidents"],
            ),
            "position": {"x": 0, "y": 0, "width": 4, "height": 2},
        },
        {
            "widget": _filter("filtro-tecnologia", "Tecnología", "technology", ["network_daily"]),
            "position": {"x": 4, "y": 0, "width": 4, "height": 2},
        },
        {
            "widget": _filter("filtro-segmento", "Segmento", "customer_segment", ["customers", "customer_product"]),
            "position": {"x": 8, "y": 0, "width": 4, "height": 2},
        },
    ]

    return {
        "datasets": datasets,
        "pages": [
            {
                "name": "resumen",
                "displayName": "Resumen 360",
                "pageType": "PAGE_TYPE_CANVAS",
                "layoutVersion": "GRID_V1",
                "layout": canvas,
            },
            {
                "name": "filtros",
                "displayName": "Filtros",
                "pageType": "PAGE_TYPE_GLOBAL_FILTERS",
                "layoutVersion": "GRID_V1",
                "layout": filters,
            },
        ],
        "uiSettings": {
            "theme": {
                "canvasBackgroundColor": {"light": "#F7F9FA", "dark": "#1F272D"},
                "widgetBackgroundColor": {"light": "#FFFFFF", "dark": "#11171C"},
                "widgetBorderColor": {"light": "#FFFFFF", "dark": "#11171C"},
                "fontColor": {"light": "#11171C", "dark": "#E8ECF0"},
                "selectionColor": {"light": "#2272B4", "dark": "#8ACAFF"},
                "visualizationColors": PALETTE,
                "widgetHeaderAlignment": "LEFT",
            }
        },
    }


GENIE_CODE_PROMPT = """Soy analista de un operador móvil ecuatoriano ficticio, Equinoccio Móvil (datos sintéticos de agosto de 2026) y necesito un dashboard ejecutivo llamado "{title}" para la reunión semanal de operaciones. Usa el SQL Warehouse {warehouse_id}.

Quiero responder tres preguntas:
1. ¿Cómo está la calidad de la red y en qué provincias, regiones naturales y tecnologías estamos por debajo del umbral de calidad de red?
2. ¿Qué productos y segmentos de clientes generan más ingreso y cómo es su experiencia (NPS, tickets)?
3. ¿Qué sitios e incidentes debería atender primero el equipo de operaciones y cuánto ingreso está en riesgo?

Los datos están en el schema {namespace}. Prioriza las Metric Views (mv_network_quality y mv_customer_product_experience) para los indicadores y usa las vistas gold_* para el detalle. No uses tablas bronze, silver ni de cuarentena.

Empieza con unos pocos KPI clave, después los gráficos que mejor respondan cada pregunta y una tabla de detalle. Agrega filtros por provincia, región natural, tecnología y segmento. Títulos en español y formato de dólares estadounidenses (USD) y porcentajes. Antes de crear el dashboard, explora los datos y explícame brevemente qué vas a construir.
"""

GENIE_CODE_FOLLOW_UPS = [
    "Agrega una tendencia diaria de latencia y downlink por tecnología.",
    "Agrega las órdenes de mantenimiento y su costo por provincia, junto al ingreso en riesgo.",
    "Crea una segunda página enfocada en riesgo de baja de clientes por segmento y producto.",
]


def genie_code_prompt(namespace: str, title: str, warehouse_id: str) -> str:
    return GENIE_CODE_PROMPT.format(namespace=namespace, title=title, warehouse_id=warehouse_id)
