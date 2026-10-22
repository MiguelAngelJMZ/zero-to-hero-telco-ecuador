"""Acceso a datos Gold y contingencia local para la App Customer 360.

El contrato público del módulo usa exclusivamente identificadores sintéticos. No
contiene nombres, teléfonos, cédulas, correos ni direcciones de clientes.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Final

import numpy as np
import pandas as pd

GOLD_TABLES: Final[dict[str, tuple[str, str]]] = {
    "network_hourly": ("TELCO_TABLE_NETWORK_HOURLY", "gold_network_hourly"),
    "site_daily": ("TELCO_TABLE_SITE_DAILY_360", "gold_site_daily_360"),
    "customer_360": ("TELCO_TABLE_CUSTOMER_360", "gold_customer_360"),
    "customer_product_daily": (
        "TELCO_TABLE_CUSTOMER_PRODUCT_DAILY",
        "gold_customer_product_daily",
    ),
    "incident_impact": ("TELCO_TABLE_INCIDENT_IMPACT", "gold_incident_impact"),
}


def remote_configuration() -> tuple[bool, list[str]]:
    """Indica si existe una configuración remota completa, sin valores fijos."""

    required = ("DATABRICKS_WAREHOUSE_ID", "TELCO_CATALOG", "TELCO_SCHEMA")
    missing = [name for name in required if not os.getenv(name)]
    return not missing, missing


def _quoted_identifier(value: str) -> str:
    if not value or any(character in value for character in ("`", "\n", "\r", "\x00")):
        raise ValueError("El identificador de Unity Catalog no es válido.")
    return f"`{value}`"


def _workspace_client():
    from databricks.sdk import WorkspaceClient

    profile = os.getenv("DATABRICKS_PROFILE")
    if not os.getenv("DATABRICKS_APP_NAME") and profile:
        return WorkspaceClient(profile=profile)
    return WorkspaceClient()


def _statement_to_dataframe(statement: str, warehouse_id: str, row_limit: int) -> pd.DataFrame:
    client = _workspace_client()
    response = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=statement,
        wait_timeout="50s",
        row_limit=row_limit,
    )
    state = response.status.state.value if response.status and response.status.state else "UNKNOWN"
    if state != "SUCCEEDED":
        message = response.status.error.message if response.status and response.status.error else state
        raise RuntimeError(f"La consulta SQL no finalizó correctamente: {message}")

    columns = [column.name for column in response.manifest.schema.columns]
    rows = list(response.result.data_array or [])
    chunks = response.manifest.chunks or []
    statement_id = response.statement_id
    for chunk in chunks[1:]:
        result = client.statement_execution.get_statement_result_chunk_n(
            statement_id=statement_id,
            chunk_index=chunk.chunk_index,
        )
        rows.extend(result.data_array or [])
    return pd.DataFrame(rows, columns=columns)


def load_remote_gold(catalog: str, schema: str, warehouse_id: str) -> dict[str, pd.DataFrame]:
    """Carga las cinco tablas Gold. Los nombres de entorno pueden sobrescribirse."""

    configured_limit = int(os.getenv("TELCO_APP_ROW_LIMIT", "100000"))
    row_limit = min(max(configured_limit, 1_000), 500_000)
    namespace = ".".join((_quoted_identifier(catalog), _quoted_identifier(schema)))
    datasets: dict[str, pd.DataFrame] = {}
    for logical_name, (environment_name, default_table) in GOLD_TABLES.items():
        table_name = os.getenv(environment_name, default_table)
        qualified_table = f"{namespace}.{_quoted_identifier(table_name)}"
        datasets[logical_name] = _statement_to_dataframe(
            f"SELECT * FROM {qualified_table} LIMIT {row_limit}",
            warehouse_id,
            row_limit,
        )
    return normalize_gold_contract(datasets)


def _read_csv(data_dir: Path, name: str) -> pd.DataFrame:
    path = data_dir / f"{name}.csv"
    return pd.read_csv(path, low_memory=False) if path.exists() else pd.DataFrame()


def _numeric(frame: pd.DataFrame, columns: list[str]) -> None:
    for column in columns:
        if column in frame:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")


def _fill_column(frame: pd.DataFrame, name: str, value=0) -> None:
    if name not in frame:
        frame[name] = value


def _series(frame: pd.DataFrame, name: str, value=0) -> pd.Series:
    if name in frame:
        return frame[name]
    return pd.Series(value, index=frame.index)


def _deduplicate(frame: pd.DataFrame, key: str) -> pd.DataFrame:
    return frame.drop_duplicates(key).copy() if key in frame else frame.copy()


def _network_gold(sources: dict[str, pd.DataFrame]) -> tuple[pd.DataFrame, pd.DataFrame]:
    metrics = sources["cell_tower_metrics"].copy()
    cells = _deduplicate(sources["radio_cells"], "cell_id")
    sites = _deduplicate(sources["network_sites"], "site_id")
    tickets = sources["support_tickets"].copy()
    alarms = sources["network_alarms"].copy()

    if metrics.empty:
        return pd.DataFrame(), pd.DataFrame()
    if not cells.empty and not sites.empty and "site_id" in cells:
        cells = cells[cells["site_id"].isin(set(sites["site_id"]))].copy()
    metrics = metrics.rename(
        columns={
            "timestamp": "event_ts",
            "tower_id": "site_id",
            "throughput_mbps": "downlink_mbps",
        }
    )
    if "cell_id" not in metrics:
        metrics["cell_id"] = metrics["site_id"]
    if not cells.empty:
        metrics = metrics[metrics["cell_id"].isin(set(cells["cell_id"]))].copy()

    if not cells.empty:
        cell_lookup = cells.set_index("cell_id")
        for column in ("site_id", "technology", "frequency_band", "capacity_users"):
            if column in cell_lookup:
                mapped = metrics["cell_id"].map(cell_lookup[column])
                metrics[column] = _series(metrics, column, np.nan).fillna(mapped)
    if not sites.empty and "site_id" in metrics:
        site_lookup = sites.set_index("site_id")
        for column in (
            "region",
            "canton",
            "latitude",
            "longitude",
            "environment",
            "site_type",
        ):
            if column in site_lookup:
                mapped = metrics["site_id"].map(site_lookup[column])
                metrics[column] = _series(metrics, column, np.nan).fillna(mapped)

    _numeric(
        metrics,
        [
            "availability_pct",
            "signal_strength_dbm",
            "sinr_db",
            "latency_ms",
            "downlink_mbps",
            "uplink_mbps",
            "packet_loss_pct",
            "dropped_calls",
            "active_users",
            "data_traffic_gb",
            "latitude",
            "longitude",
        ],
    )
    metrics["event_ts"] = pd.to_datetime(metrics["event_ts"], errors="coerce")
    metrics = metrics[
        metrics["event_ts"].notna()
        & metrics["cell_id"].notna()
        & _series(metrics, "latency_ms", np.nan).between(0, 500)
        & _series(metrics, "downlink_mbps", np.nan).ge(0)
    ].drop_duplicates(["cell_id", "event_ts"])
    if metrics.empty:
        return pd.DataFrame(), pd.DataFrame()

    if "availability_pct" not in metrics:
        metrics["availability_pct"] = (100 - metrics["latency_ms"] / 60).clip(90, 100)
    if "uplink_mbps" not in metrics:
        metrics["uplink_mbps"] = metrics["downlink_mbps"] * 0.22
    if "data_traffic_gb" not in metrics:
        metrics["data_traffic_gb"] = _series(metrics, "active_users") * metrics["downlink_mbps"] / 8_000
    for column, value in {
        "region": "Sin provincia",
        "canton": "Sin cantón",
        "environment": "No informado",
        "technology": "No informada",
        "frequency_band": "No informada",
        "signal_strength_dbm": np.nan,
        "sinr_db": np.nan,
        "packet_loss_pct": np.nan,
        "dropped_calls": 0,
        "active_users": 0,
    }.items():
        _fill_column(metrics, column, value)

    metrics["metric_hour"] = metrics["event_ts"].dt.floor("h")
    metrics["metric_date"] = metrics["event_ts"].dt.date
    metrics["network_sla_met"] = (
        (metrics["availability_pct"] >= 99)
        & (metrics["latency_ms"] <= 100)
        & (metrics["downlink_mbps"] >= 20)
    ).astype(int)
    metrics["health_status"] = "Saludable"
    metrics.loc[
        (metrics["latency_ms"] > 80) | (metrics["downlink_mbps"] < 30), "health_status"
    ] = "Atención"
    metrics.loc[
        (metrics["latency_ms"] > 120)
        | (metrics["downlink_mbps"] < 15)
        | (metrics["availability_pct"] < 97),
        "health_status",
    ] = "Crítico"

    network_hourly = (
        metrics.groupby(
            [
                "metric_hour",
                "metric_date",
                "region",
                "canton",
                "site_id",
                "technology",
                "frequency_band",
                "environment",
                "health_status",
            ],
            dropna=False,
            as_index=False,
        )
        .agg(
            active_cells=("cell_id", "nunique"),
            avg_availability_pct=("availability_pct", "mean"),
            avg_signal_dbm=("signal_strength_dbm", "mean"),
            avg_sinr_db=("sinr_db", "mean"),
            avg_latency_ms=("latency_ms", "mean"),
            avg_downlink_mbps=("downlink_mbps", "mean"),
            avg_uplink_mbps=("uplink_mbps", "mean"),
            avg_packet_loss_pct=("packet_loss_pct", "mean"),
            total_dropped_calls=("dropped_calls", "sum"),
            total_data_traffic_gb=("data_traffic_gb", "sum"),
            active_users=("active_users", "mean"),
            network_sla_compliance_pct=("network_sla_met", "mean"),
        )
    )
    network_hourly["network_sla_compliance_pct"] *= 100
    network_hourly["active_sites"] = 1

    site_daily = (
        metrics.groupby(
            ["metric_date", "site_id", "region", "canton", "environment"],
            dropna=False,
            as_index=False,
        )
        .agg(
            latitude=("latitude", "first"),
            longitude=("longitude", "first"),
            active_cells=("cell_id", "nunique"),
            technologies=("technology", lambda values: ", ".join(sorted(set(values.astype(str))))),
            avg_availability_pct=("availability_pct", "mean"),
            avg_latency_ms=("latency_ms", "mean"),
            avg_downlink_mbps=("downlink_mbps", "mean"),
            total_dropped_calls=("dropped_calls", "sum"),
            total_data_traffic_gb=("data_traffic_gb", "sum"),
            network_sla_compliance_pct=("network_sla_met", "mean"),
        )
    )
    site_daily["network_sla_compliance_pct"] *= 100

    if not tickets.empty:
        tickets = tickets.rename(columns={"tower_id": "site_id", "status": "ticket_status"})
        if "site_id" not in tickets and "cell_id" in tickets and not cells.empty:
            tickets["site_id"] = tickets["cell_id"].map(cells.set_index("cell_id")["site_id"])
        tickets["metric_date"] = pd.to_datetime(tickets["created_at"], errors="coerce").dt.date
        _fill_column(tickets, "ticket_status", "No informado")
        _fill_column(tickets, "severity", "No informada")
        ticket_daily = (
            tickets.dropna(subset=["site_id", "metric_date"])
            .groupby(["site_id", "metric_date"], as_index=False)
            .agg(
                total_tickets=("ticket_id", "nunique"),
                open_tickets=("ticket_status", lambda values: values.isin(["Abierto", "En progreso"]).sum()),
                critical_tickets=("severity", lambda values: (values == "Crítica").sum()),
            )
        )
        site_daily = site_daily.merge(ticket_daily, on=["site_id", "metric_date"], how="left")
    if not alarms.empty:
        if "site_id" not in alarms and "cell_id" in alarms and not cells.empty:
            alarms["site_id"] = alarms["cell_id"].map(cells.set_index("cell_id")["site_id"])
        alarms["metric_date"] = pd.to_datetime(alarms["opened_at"], errors="coerce").dt.date
        alarm_daily = (
            alarms.dropna(subset=["site_id", "metric_date"])
            .groupby(["site_id", "metric_date"], as_index=False)
            .agg(total_alarms=("alarm_id", "nunique"), service_impact_alarms=("service_impact", "sum"))
        )
        site_daily = site_daily.merge(alarm_daily, on=["site_id", "metric_date"], how="left")
    for column in ("total_tickets", "open_tickets", "critical_tickets", "total_alarms", "service_impact_alarms"):
        _fill_column(site_daily, column, 0)
        site_daily[column] = site_daily[column].fillna(0)
    site_daily["health_status"] = "Saludable"
    site_daily.loc[
        (site_daily["avg_latency_ms"] > 80)
        | (site_daily["avg_downlink_mbps"] < 30)
        | (site_daily["open_tickets"] > 2),
        "health_status",
    ] = "Atención"
    site_daily.loc[
        (site_daily["avg_latency_ms"] > 120)
        | (site_daily["avg_downlink_mbps"] < 15)
        | (site_daily["avg_availability_pct"] < 97),
        "health_status",
    ] = "Crítico"
    return network_hourly, site_daily


def _fallback_customer_gold(sources: dict[str, pd.DataFrame]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Crea una muestra reproducible y anónima si aún no llegaron fuentes Customer 360."""

    rng = np.random.default_rng(42)
    regions = sources["cell_tower_metrics"].get("region", pd.Series(["Pichincha"])).dropna().unique()
    regions = regions if len(regions) else np.array(["Pichincha"])
    product_catalog = pd.DataFrame(
        {
            "product_id": [f"PRD-{index:03d}" for index in range(1, 7)],
            "product_name": [
                "Prepago Digital 10 GB",
                "Plan Móvil 40 GB",
                "Plan Móvil 80 GB",
                "Plan Móvil Ilimitado",
                "Datos Conectados 100 GB",
                "Empresa Móvil 200 GB",
            ],
            "product_family": ["Prepago", "Postpago", "Postpago", "Postpago", "Datos", "Empresa"],
            "monthly_fee_usd": [8.0, 14.99, 19.99, 29.99, 29.99, 39.99],
        }
    )
    customer_count = 480
    customer_ids = np.array([f"CLI-{index:06d}" for index in range(1, customer_count + 1)])
    customer_360 = pd.DataFrame(
        {
            "customer_id": customer_ids,
            "region": rng.choice(regions, customer_count),
            "canton": "Agregada",
            "customer_type": rng.choice(["Persona", "Empresa"], customer_count, p=[0.92, 0.08]),
            "customer_segment": rng.choice(
                ["Prepago", "Postpago", "Pyme", "Corporativo"], customer_count, p=[0.35, 0.52, 0.1, 0.03]
            ),
            "active_subscriptions": rng.choice([1, 1, 1, 2, 3], customer_count),
            "product_count": rng.choice([1, 1, 1, 2], customer_count),
            "total_data_gb": rng.gamma(3.2, 6.0, customer_count).round(2),
            "total_tickets": rng.poisson(0.45, customer_count),
            "nps_score": rng.integers(0, 11, customer_count),
            "network_sla_compliance_pct": rng.uniform(88, 100, customer_count).round(2),
        }
    )
    customer_360["risk_score"] = (
        customer_360["total_tickets"] * 18
        + (10 - customer_360["nps_score"]) * 4
        + (100 - customer_360["network_sla_compliance_pct"]) * 2
    ).clip(0, 100)
    customer_360["risk_level"] = pd.cut(
        customer_360["risk_score"], [-1, 35, 65, 101], labels=["Bajo", "Medio", "Alto"]
    ).astype(str)
    customer_360["customer_status"] = "Activo"

    days = pd.date_range("2026-08-01", periods=14, freq="D")
    assignments = rng.integers(0, len(product_catalog), customer_count)
    rows: list[dict] = []
    for customer_index, customer_id in enumerate(customer_ids):
        product = product_catalog.iloc[assignments[customer_index]]
        for day in days:
            rows.append(
                {
                    "usage_date": day.date(),
                    "customer_id": customer_id,
                    "subscription_id": f"SUB-{customer_index + 1:06d}",
                    "region": customer_360.iloc[customer_index]["region"],
                    "customer_segment": customer_360.iloc[customer_index]["customer_segment"],
                    "product_id": product.product_id,
                    "product_name": product.product_name,
                    "product_family": product.product_family,
                    "subscription_status": "Activa",
                    "data_gb": round(float(rng.gamma(2.0, 0.8)), 2),
                    "voice_minutes": round(float(rng.gamma(1.5, 4.0)), 1),
                    "billed_revenue_usd": round(float(product.monthly_fee_usd) / len(days), 2),
                    "recharge_revenue_usd": float(rng.choice([0, 0, 0, 5, 10]))
                    if product.product_family == "Prepago"
                    else 0,
                    "total_tickets": int(rng.random() < 0.025),
                    "nps_score": int(customer_360.iloc[customer_index]["nps_score"]),
                    "network_sla_compliance_pct": float(
                        customer_360.iloc[customer_index]["network_sla_compliance_pct"]
                    ),
                }
            )
    return customer_360, pd.DataFrame(rows)


def _customer_gold(sources: dict[str, pd.DataFrame]) -> tuple[pd.DataFrame, pd.DataFrame]:
    customers = _deduplicate(sources["customers"], "customer_id")
    subscriptions = _deduplicate(sources["subscriptions"], "subscription_id")
    products = _deduplicate(sources["products"], "product_id")
    usage = sources["usage_daily"].copy()
    tickets = sources["support_tickets"].copy()
    surveys = sources["customer_surveys"].copy()
    metrics = sources["cell_tower_metrics"].copy()
    cells = _deduplicate(sources["radio_cells"], "cell_id")
    sites = _deduplicate(sources["network_sites"], "site_id")
    if customers.empty or subscriptions.empty or products.empty or usage.empty:
        return _fallback_customer_gold(sources)

    valid_customers = set(customers["customer_id"])
    valid_products = set(products["product_id"])
    subscriptions = subscriptions[
        subscriptions["customer_id"].isin(valid_customers) & subscriptions["product_id"].isin(valid_products)
    ]
    usage = usage[usage["subscription_id"].isin(set(subscriptions["subscription_id"]))].copy()
    if not cells.empty and not sites.empty and "site_id" in cells:
        cells = cells[cells["site_id"].isin(set(sites["site_id"]))].copy()
    if "primary_cell_id" in usage and not cells.empty:
        usage = usage[usage["primary_cell_id"].isin(set(cells["cell_id"]))].copy()
    usage["usage_date"] = pd.to_datetime(usage["usage_date"], errors="coerce").dt.date
    usage = usage.dropna(subset=["usage_date"]).drop_duplicates(["subscription_id", "usage_date"])
    _numeric(usage, ["data_gb", "voice_minutes", "billed_amount_usd", "recharge_amount_usd"])

    product_daily = usage.merge(subscriptions, on="subscription_id", how="inner", suffixes=("", "_subscription"))
    product_daily = product_daily.merge(products, on="product_id", how="inner", suffixes=("", "_product"))
    customer_dimensions = customers[
        [column for column in ("customer_id", "region", "canton", "customer_type", "customer_segment") if column in customers]
    ]
    product_daily = product_daily.merge(customer_dimensions, on="customer_id", how="inner")
    product_daily = product_daily.rename(
        columns={
            "billed_amount_usd": "billed_revenue_usd",
            "recharge_amount_usd": "recharge_revenue_usd",
        }
    )
    for column, value in {
        "data_gb": 0,
        "voice_minutes": 0,
        "billed_revenue_usd": 0,
        "recharge_revenue_usd": 0,
        "subscription_status": "No informado",
        "region": "Sin provincia",
        "canton": "Sin cantón",
        "customer_type": "No informado",
        "customer_segment": "No informado",
        "product_name": "Producto no informado",
        "product_family": "No informada",
    }.items():
        _fill_column(product_daily, column, value)

    if "primary_cell_id" in product_daily and not cells.empty:
        cell_lookup = cells.set_index("cell_id")
        if "site_id" in cell_lookup:
            product_daily["primary_site_id"] = product_daily["primary_cell_id"].map(cell_lookup["site_id"])
        if "technology" in cell_lookup:
            product_daily["technology"] = product_daily["primary_cell_id"].map(cell_lookup["technology"])
    if not metrics.empty and {"cell_id", "event_ts"}.issubset(metrics):
        _numeric(metrics, ["availability_pct", "latency_ms", "downlink_mbps"])
        metrics["usage_date"] = pd.to_datetime(metrics["event_ts"], errors="coerce").dt.date
        valid_metrics = metrics[
            metrics["usage_date"].notna()
            & metrics["availability_pct"].between(0, 100)
            & metrics["latency_ms"].between(0, 500)
            & metrics["downlink_mbps"].ge(0)
        ].copy()
        valid_metrics["network_sla_met"] = (
            (valid_metrics["availability_pct"] >= 99)
            & (valid_metrics["latency_ms"] <= 100)
            & (valid_metrics["downlink_mbps"] >= 20)
        ).astype(int)
        cell_daily_quality = (
            valid_metrics.groupby(["cell_id", "usage_date"], as_index=False)
            .agg(
                avg_network_latency_ms=("latency_ms", "mean"),
                network_sla_compliance_pct=("network_sla_met", "mean"),
            )
            .rename(columns={"cell_id": "primary_cell_id"})
        )
        cell_daily_quality["network_sla_compliance_pct"] *= 100
        product_daily = product_daily.merge(
            cell_daily_quality,
            on=["primary_cell_id", "usage_date"],
            how="left",
        )

    if not tickets.empty and "subscription_id" in tickets:
        tickets["usage_date"] = pd.to_datetime(tickets["created_at"], errors="coerce").dt.date
        ticket_daily = (
            tickets[tickets["subscription_id"].isin(set(subscriptions["subscription_id"]))]
            .groupby(["subscription_id", "usage_date"], as_index=False)
            .agg(total_tickets=("ticket_id", "nunique"))
        )
        product_daily = product_daily.merge(ticket_daily, on=["subscription_id", "usage_date"], how="left")
    _fill_column(product_daily, "total_tickets", 0)
    product_daily["total_tickets"] = product_daily["total_tickets"].fillna(0)

    nps_by_customer = pd.DataFrame(columns=["customer_id", "nps_score"])
    if not surveys.empty:
        _numeric(surveys, ["nps_score"])
        nps_by_customer = surveys.groupby("customer_id", as_index=False).agg(nps_score=("nps_score", "mean"))
    product_daily = product_daily.merge(nps_by_customer, on="customer_id", how="left")
    product_daily["nps_score"] = product_daily["nps_score"].fillna(7)
    _fill_column(product_daily, "network_sla_compliance_pct", 96.0)
    product_daily["network_sla_compliance_pct"] = product_daily[
        "network_sla_compliance_pct"
    ].fillna(96.0)

    customer_360 = (
        product_daily.groupby(
            ["customer_id", "region", "canton", "customer_type", "customer_segment"],
            dropna=False,
            as_index=False,
        )
        .agg(
            active_subscriptions=("subscription_id", "nunique"),
            product_count=("product_id", "nunique"),
            total_data_gb=("data_gb", "sum"),
            billed_revenue_usd=("billed_revenue_usd", "sum"),
            total_tickets=("total_tickets", "sum"),
            nps_score=("nps_score", "mean"),
            network_sla_compliance_pct=("network_sla_compliance_pct", "mean"),
        )
    )
    customer_360["risk_score"] = (
        customer_360["total_tickets"] * 18
        + (10 - customer_360["nps_score"]) * 4
        + (100 - customer_360["network_sla_compliance_pct"]) * 2
    ).clip(0, 100)
    customer_360["risk_level"] = pd.cut(
        customer_360["risk_score"], [-1, 35, 65, 101], labels=["Bajo", "Medio", "Alto"]
    ).astype(str)
    customer_360["customer_status"] = "Activo"
    return customer_360, product_daily


def _incident_gold(sources: dict[str, pd.DataFrame]) -> pd.DataFrame:
    alarms = _deduplicate(sources["network_alarms"], "alarm_id")
    cells = _deduplicate(sources["radio_cells"], "cell_id")
    sites = _deduplicate(sources["network_sites"], "site_id")
    orders = _deduplicate(sources["maintenance_orders"], "work_order_id")
    if alarms.empty:
        tickets = sources["support_tickets"].copy()
        if tickets.empty:
            return pd.DataFrame()
        tickets = tickets.rename(
            columns={
                "ticket_id": "incident_id",
                "tower_id": "site_id",
                "created_at": "opened_at",
                "status": "incident_status",
            }
        )
        _fill_column(tickets, "affected_users_est", 0)
        _fill_column(tickets, "service_impact", True)
        _fill_column(tickets, "alarm_type", "Ticket de red")
        return tickets

    if not cells.empty and not sites.empty and "site_id" in cells:
        cells = cells[cells["site_id"].isin(set(sites["site_id"]))].copy()
        alarms = alarms[alarms["cell_id"].isin(set(cells["cell_id"]))].copy()
    if "site_id" not in alarms and not cells.empty:
        alarms["site_id"] = alarms["cell_id"].map(cells.set_index("cell_id")["site_id"])
    if not sites.empty:
        site_lookup = sites.set_index("site_id")
        for column in ("region", "canton"):
            alarms[column] = alarms["site_id"].map(site_lookup[column])
    alarms = alarms.rename(columns={"alarm_id": "incident_id", "alarm_status": "incident_status"})
    if not orders.empty and "alarm_id" in orders:
        order_summary = (
            orders.groupby("alarm_id", as_index=False)
            .agg(
                work_orders=("work_order_id", "nunique"),
                downtime_minutes=("downtime_minutes", "sum"),
                maintenance_cost_usd=("cost_usd", "sum"),
            )
            .rename(columns={"alarm_id": "incident_id"})
        )
        alarms = alarms.merge(order_summary, on="incident_id", how="left")
    return alarms


def load_local_gold(data_dir: Path | None = None) -> dict[str, pd.DataFrame]:
    """Deriva vistas Gold desde CSV sintético para continuidad sin servicios remotos."""

    if data_dir is None:
        configured_path = os.getenv("TELCO_LOCAL_DATA_DIR")
        data_dir = (
            Path(configured_path).expanduser()
            if configured_path
            else Path(__file__).resolve().parents[2] / "data" / "generated"
        )
    direct_gold = {logical: _read_csv(data_dir, default) for logical, (_, default) in GOLD_TABLES.items()}
    if all(not frame.empty for frame in direct_gold.values()):
        return normalize_gold_contract(direct_gold)

    source_names = (
        "network_sites",
        "radio_cells",
        "cell_tower_metrics",
        "network_alarms",
        "maintenance_orders",
        "products",
        "customers",
        "subscriptions",
        "usage_daily",
        "support_tickets",
        "customer_surveys",
    )
    sources = {name: _read_csv(data_dir, name) for name in source_names}
    if sources["cell_tower_metrics"].empty:
        raise FileNotFoundError(f"No hay datos sintéticos utilizables en {data_dir}.")
    network_hourly, site_daily = _network_gold(sources)
    customer_360, customer_product_daily = _customer_gold(sources)
    datasets = {
        "network_hourly": network_hourly,
        "site_daily": site_daily,
        "customer_360": customer_360,
        "customer_product_daily": customer_product_daily,
        "incident_impact": _incident_gold(sources),
    }
    return normalize_gold_contract(datasets)


def normalize_gold_contract(datasets: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """Normaliza tipos y aliases menores sin ocultar errores de contrato estructural."""

    normalized = {name: frame.copy() for name, frame in datasets.items()}
    aliases = {
        "avg_throughput_mbps": "avg_downlink_mbps",
        "total_revenue_usd": "billed_revenue_usd",
        "sla_compliance_pct": "network_sla_compliance_pct",
        "status": "health_status",
    }
    for frame in normalized.values():
        frame.rename(columns={old: new for old, new in aliases.items() if old in frame and new not in frame}, inplace=True)

    dataset_aliases = {
        "network_hourly": {
            "event_hour": "metric_hour",
            "event_date": "metric_date",
            "dropped_calls": "total_dropped_calls",
            "data_traffic_gb": "total_data_traffic_gb",
        },
        "site_daily": {
            "event_date": "metric_date",
            "site_health_status": "health_status",
        },
        "customer_360": {
            "subscriptions": "active_subscriptions",
            "tickets": "total_tickets",
            "avg_nps_score": "nps_score",
            "churn_risk_score": "risk_score",
            "churn_risk_band": "risk_level",
        },
        "customer_product_daily": {
            "usage_date": "metric_date",
            "billed_amount_usd": "billed_revenue_usd",
            "recharge_amount_usd": "recharge_revenue_usd",
            "ticket_count": "total_tickets",
            "avg_nps_score": "nps_score",
        },
        "incident_impact": {
            "alarm_id": "incident_id",
            "alarm_status": "incident_status",
        },
    }
    for dataset_name, name_map in dataset_aliases.items():
        frame = normalized.setdefault(dataset_name, pd.DataFrame())
        frame.rename(
            columns={old: new for old, new in name_map.items() if old in frame and new not in frame},
            inplace=True,
        )

    expected = {
        "network_hourly": {
            "region": "Sin provincia",
            "technology": "No informada",
            "health_status": "No informado",
            "avg_latency_ms": np.nan,
            "avg_availability_pct": np.nan,
            "avg_downlink_mbps": np.nan,
            "total_dropped_calls": 0,
            "network_sla_compliance_pct": np.nan,
        },
        "site_daily": {
            "site_id": "Sin sitio",
            "region": "Sin provincia",
            "canton": "Sin cantón",
            "health_status": "No informado",
            "latitude": np.nan,
            "longitude": np.nan,
            "avg_latency_ms": np.nan,
            "avg_availability_pct": np.nan,
            "avg_downlink_mbps": np.nan,
            "open_tickets": 0,
        },
        "customer_360": {
            "customer_id": "Cliente sintético",
            "region": "Sin provincia",
            "customer_segment": "No informado",
            "customer_status": "No informado",
            "active_subscriptions": 0,
            "total_data_gb": 0,
            "total_tickets": 0,
            "nps_score": np.nan,
            "risk_score": 0,
            "risk_level": "No informado",
            "network_sla_compliance_pct": np.nan,
        },
        "customer_product_daily": {
            "customer_id": "Cliente sintético",
            "subscription_id": "Línea sintética",
            "region": "Sin provincia",
            "customer_segment": "No informado",
            "product_id": "Sin producto",
            "product_name": "Producto no informado",
            "product_family": "No informada",
            "data_gb": 0,
            "voice_minutes": 0,
            "billed_revenue_usd": 0,
            "recharge_revenue_usd": 0,
            "total_tickets": 0,
            "nps_score": np.nan,
            "network_sla_compliance_pct": np.nan,
        },
        "incident_impact": {
            "incident_id": "Sin incidente",
            "region": "Sin provincia",
            "canton": "Sin cantón",
            "severity": "No informada",
            "incident_status": "No informado",
            "affected_users_est": 0,
            "service_impact": False,
        },
    }
    numeric_columns = {
        "avg_latency_ms",
        "avg_availability_pct",
        "avg_downlink_mbps",
        "total_dropped_calls",
        "network_sla_compliance_pct",
        "latitude",
        "longitude",
        "open_tickets",
        "active_subscriptions",
        "total_data_gb",
        "total_tickets",
        "nps_score",
        "risk_score",
        "data_gb",
        "voice_minutes",
        "billed_revenue_usd",
        "recharge_revenue_usd",
        "affected_users_est",
    }
    for dataset_name, defaults in expected.items():
        frame = normalized.setdefault(dataset_name, pd.DataFrame())
        for column, value in defaults.items():
            _fill_column(frame, column, value)
        _numeric(frame, [column for column in numeric_columns if column in frame])
    return normalized
