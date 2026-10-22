"""App inicial y deliberadamente simple para el workshop Databricks Zero to Hero.

El objetivo didáctico es que los participantes entiendan primero el acceso a las
tablas Gold. La experiencia visual moderna se construye después, como reto final.
"""

from __future__ import annotations

import os

import pandas as pd
import plotly.express as px
import streamlit as st
from data_access import load_local_gold, load_remote_gold, remote_configuration

st.set_page_config(page_title="Monitor de experiencia móvil", page_icon="📡", layout="wide")


def _mean(frame: pd.DataFrame, column: str) -> float:
    """Calcula una media segura para columnas numéricas."""

    if column not in frame:
        return 0.0
    values = pd.to_numeric(frame[column], errors="coerce").dropna()
    return float(values.mean()) if not values.empty else 0.0


def _format_integer(value: float) -> str:
    return f"{int(value):,}".replace(",", ".")


@st.cache_data(ttl=120, show_spinner="Consultando la capa Gold...")
def _load_remote(catalog: str, schema: str, warehouse_id: str) -> dict[str, pd.DataFrame]:
    return load_remote_gold(catalog, schema, warehouse_id)


@st.cache_data(show_spinner=False)
def _load_local() -> dict[str, pd.DataFrame]:
    return load_local_gold()


remote_enabled, missing_remote_variables = remote_configuration()
remote_error = ""

try:
    if remote_enabled:
        datasets = _load_remote(
            os.environ["TELCO_CATALOG"],
            os.environ["TELCO_SCHEMA"],
            os.environ["DATABRICKS_WAREHOUSE_ID"],
        )
        data_source = "tablas Gold gobernadas por Unity Catalog"
    else:
        datasets = _load_local()
        data_source = "datos sintéticos locales de contingencia"
except Exception as exc:  # noqa: BLE001 - la contingencia es intencional para el workshop.
    remote_error = str(exc)
    datasets = _load_local()
    data_source = "datos sintéticos locales de contingencia"


network = datasets["network_hourly"]
sites = datasets["site_daily"]
customers = datasets["customer_360"]
products = datasets["customer_product_daily"]


st.title("📡 Monitor de experiencia móvil")
st.caption(
    f"Aplicación inicial del workshop · Fuente: {data_source} · "
    "Solo contiene identificadores y datos sintéticos."
)

if remote_error:
    st.warning("La consulta remota no estuvo disponible; se activó la contingencia local.")
elif not remote_enabled and any(
    os.getenv(name) for name in ("DATABRICKS_WAREHOUSE_ID", "TELCO_CATALOG", "TELCO_SCHEMA")
):
    st.info(f"Configuración remota incompleta. Faltan: {', '.join(missing_remote_variables)}.")


region_series = [
    frame["region"].dropna().astype(str)
    for frame in (network, sites, customers, products)
    if "region" in frame
]
regions = sorted(pd.concat(region_series, ignore_index=True).unique()) if region_series else []
selected_region = st.selectbox("Provincia", ["Todas", *regions])


def _filter_region(frame: pd.DataFrame) -> pd.DataFrame:
    if selected_region == "Todas" or "region" not in frame:
        return frame.copy()
    return frame[frame["region"].astype(str) == selected_region].copy()


network_filtered = _filter_region(network)
sites_filtered = _filter_region(sites)
customers_filtered = _filter_region(customers)
products_filtered = _filter_region(products)


metric_1, metric_2, metric_3 = st.columns(3)
metric_1.metric("Disponibilidad media", f"{_mean(network_filtered, 'avg_availability_pct'):.2f}%")
metric_2.metric("Latencia media", f"{_mean(network_filtered, 'avg_latency_ms'):.1f} ms")
metric_3.metric("Clientes activos", _format_integer(customers_filtered["customer_id"].nunique()))


st.subheader("Ingresos observados por familia de producto")
product_summary = (
    products_filtered.groupby("product_family", as_index=False)
    .agg(ingresos_usd=("billed_revenue_usd", "sum"))
    .sort_values("ingresos_usd", ascending=False)
)
figure = px.bar(
    product_summary,
    x="product_family",
    y="ingresos_usd",
    labels={"product_family": "Familia", "ingresos_usd": "Ingresos observados (USD)"},
)
st.plotly_chart(figure, width="stretch")


st.subheader("Sitios que requieren atención")
site_columns = [
    column
    for column in (
        "site_id",
        "region",
        "canton",
        "health_status",
        "avg_availability_pct",
        "avg_latency_ms",
        "avg_downlink_mbps",
        "open_tickets",
    )
    if column in sites_filtered
]
sort_columns = [column for column in ("avg_latency_ms", "open_tickets") if column in sites_filtered]
attention_sites = sites_filtered.sort_values(sort_columns, ascending=False).drop_duplicates("site_id")
st.dataframe(attention_sites[site_columns].head(15), hide_index=True, width="stretch")


st.info(
    "Esta versión es intencionalmente simple. El reto final consiste en convertirla "
    "en una experiencia moderna con navegación, diseño visual y nuevas interacciones."
)
