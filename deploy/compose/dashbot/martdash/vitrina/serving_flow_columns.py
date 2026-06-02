"""Патчи схемы ClickHouse serving: метрики притока/оттока, tariff_title, client_type, client_type_title, kpi_arpu_daily active_clients."""

from __future__ import annotations

import logging
from typing import Any

from django.conf import settings

_LOGGER = logging.getLogger(__name__)

_TABLES_TARIFF_TITLE_NULLABLE: tuple[str, ...] = (
    "kpi_ab0_daily",
    "kpi_ab30_daily",
    "kpi_ab90_daily",
    "kpi_revenue_daily",
    "kpi_receipts_daily",
    "kpi_arpu_daily",
    "kpi_inflow_daily",
    "kpi_outflow_daily",
    "kpi_revenue_active_month",
    "kpi_revenue_active_week",
    "kpi_revenue_active_quarter",
    "kpi_revenue_active_year",
)

_TABLES_CLIENT_TYPE_NULLABLE: tuple[str, ...] = (
    "kpi_ab0_daily",
    "kpi_ab30_daily",
    "kpi_ab90_daily",
    "kpi_inflow_daily",
    "kpi_outflow_daily",
    "kpi_revenue_daily",
    "kpi_receipts_daily",
    "kpi_arpu_daily",
    "kpi_revenue_active_month",
    "kpi_revenue_active_week",
    "kpi_revenue_active_quarter",
    "kpi_revenue_active_year",
)

_METRICS_BY_TABLE: dict[str, tuple[tuple[str, str], ...]] = {
    "kpi_inflow_daily": (
        ("new_clients", "Int64"),
        ("new_agreements", "Int64"),
        ("new_orders", "Int64"),
    ),
    "kpi_outflow_daily": (
        ("churned_clients", "Int64"),
        ("completed_agreements", "Int64"),
        ("completed_orders", "Int64"),
    ),
}


def _qi(ident: str) -> str:
    return "`" + ident.replace("`", "``") + "`"


def _qualified_table(database: str, table: str) -> str:
    return f"{_qi(database)}.{_qi(table)}"


def _table_exists(client: Any, database: str, table: str) -> bool:
    r = client.query(
        "SELECT count() FROM system.tables WHERE database = {db:String} AND name = {tb:String}",
        parameters={"db": database, "tb": table},
    )
    if not r.result_rows:
        return False
    return int(r.result_rows[0][0]) > 0


def _existing_column_names(client: Any, database: str, table: str) -> set[str]:
    r = client.query(
        "SELECT name FROM system.columns WHERE database = {db:String} AND table = {tb:String}",
        parameters={"db": database, "tb": table},
    )
    return {str(row[0]).lower() for row in r.result_rows}


def ensure_tariff_title_column_for_table(client: Any, database: str, logical_table: str) -> None:
    """ADD COLUMN IF NOT EXISTS tariff_title для KPI (Nullable) и dim_tariff (String)."""
    if logical_table == "dim_tariff":
        type_sql = "String DEFAULT ''"
    elif logical_table in _TABLES_TARIFF_TITLE_NULLABLE:
        type_sql = "Nullable(String)"
    else:
        return
    if not _table_exists(client, database, logical_table):
        return
    existing = _existing_column_names(client, database, logical_table)
    if "tariff_title" in existing:
        return
    qt = _qualified_table(database, logical_table)
    sql = f"ALTER TABLE {qt} ADD COLUMN IF NOT EXISTS {_qi('tariff_title')} {type_sql}"
    try:
        client.command(sql)
        _LOGGER.info("ClickHouse: %s", sql)
    except Exception:  # noqa: BLE001
        _LOGGER.exception("ClickHouse ALTER failed: %s", sql)


def ensure_client_type_column_for_table(client: Any, database: str, logical_table: str) -> None:
    """ADD COLUMN IF NOT EXISTS client_type для AB/приток/отток (фильтр дашборда «Абонентская база»)."""
    if logical_table not in _TABLES_CLIENT_TYPE_NULLABLE:
        return
    if not _table_exists(client, database, logical_table):
        return
    existing = _existing_column_names(client, database, logical_table)
    if "client_type" in existing:
        return
    qt = _qualified_table(database, logical_table)
    sql = f"ALTER TABLE {qt} ADD COLUMN IF NOT EXISTS {_qi('client_type')} Nullable(String)"
    try:
        client.command(sql)
        _LOGGER.info("ClickHouse: %s", sql)
    except Exception:  # noqa: BLE001
        _LOGGER.exception("ClickHouse ALTER failed: %s", sql)


def ensure_client_type_title_column_for_table(client: Any, database: str, logical_table: str) -> None:
    """ADD COLUMN IF NOT EXISTS client_type_title (подпись из mart)."""
    if logical_table not in _TABLES_CLIENT_TYPE_NULLABLE:
        return
    if not _table_exists(client, database, logical_table):
        return
    existing = _existing_column_names(client, database, logical_table)
    if "client_type_title" in existing:
        return
    qt = _qualified_table(database, logical_table)
    sql = f"ALTER TABLE {qt} ADD COLUMN IF NOT EXISTS {_qi('client_type_title')} Nullable(String)"
    try:
        client.command(sql)
        _LOGGER.info("ClickHouse: %s", sql)
    except Exception:  # noqa: BLE001
        _LOGGER.exception("ClickHouse ALTER failed: %s", sql)


def ensure_kpi_arpu_monthly_column(client: Any, database: str) -> None:
    """ADD COLUMN arpu_monthly для kpi_arpu_daily (показатель = arpu_daily × дней в месяце)."""
    table = "kpi_arpu_daily"
    if not _table_exists(client, database, table):
        return
    existing = _existing_column_names(client, database, table)
    if "arpu_monthly" in existing:
        return
    qt = _qualified_table(database, table)
    sql = f"ALTER TABLE {qt} ADD COLUMN IF NOT EXISTS {_qi('arpu_monthly')} Nullable(Float64)"
    try:
        client.command(sql)
        _LOGGER.info("ClickHouse: %s", sql)
    except Exception:  # noqa: BLE001
        _LOGGER.exception("ClickHouse ALTER failed: %s", sql)


def ensure_kpi_arpu_active_clients_column(client: Any, database: str) -> None:
    """``paying_owners`` → ``active_clients`` для legacy kpi_arpu_daily в serving."""
    table = "kpi_arpu_daily"
    if not _table_exists(client, database, table):
        return
    try:
        r = client.query(
            "SELECT name FROM system.columns WHERE database = {db:String} AND table = {tb:String}",
            parameters={"db": database, "tb": table},
        )
        by_lower: dict[str, str] = {str(row[0]).lower(): str(row[0]) for row in r.result_rows}
    except Exception:  # noqa: BLE001
        _LOGGER.exception("ensure kpi_arpu active_clients: columns")
        return
    if "active_clients" in by_lower:
        return
    qt = _qualified_table(database, table)
    if "paying_owners" in by_lower:
        old = by_lower["paying_owners"]
        sql = f"ALTER TABLE {qt} RENAME COLUMN {_qi(old)} TO {_qi('active_clients')}"
    else:
        sql = f"ALTER TABLE {qt} ADD COLUMN IF NOT EXISTS {_qi('active_clients')} Int64 DEFAULT 0"
    try:
        client.command(sql)
        _LOGGER.info("ClickHouse: %s", sql)
    except Exception:  # noqa: BLE001
        _LOGGER.exception("ClickHouse ALTER failed: %s", sql)


def ensure_flow_metric_columns_for_table(client: Any, database: str, logical_table: str) -> None:
    """ADD COLUMN IF NOT EXISTS для метрик притока/оттока, если таблица есть, а колонок нет."""
    pairs = _METRICS_BY_TABLE.get(logical_table)
    if not pairs:
        return
    if not _table_exists(client, database, logical_table):
        return
    existing = _existing_column_names(client, database, logical_table)
    qt = _qualified_table(database, logical_table)
    for col_name, typ in pairs:
        if col_name.lower() in existing:
            continue
        sql = f"ALTER TABLE {qt} ADD COLUMN IF NOT EXISTS {_qi(col_name)} {typ} DEFAULT 0"
        try:
            client.command(sql)
            _LOGGER.info("ClickHouse: %s", sql)
        except Exception:  # noqa: BLE001
            _LOGGER.exception("ClickHouse ALTER failed: %s", sql)


def ensure_flow_metric_columns_at_startup() -> None:
    if not settings.CLICKHOUSE_ENSURE_FLOW_METRICS:
        return
    try:
        from vitrina.clickhouse_client import get_client

        ch = get_client()
        db = settings.CLICKHOUSE_DATABASE
        for t in _METRICS_BY_TABLE:
            ensure_flow_metric_columns_for_table(ch, db, t)
        for t in _TABLES_TARIFF_TITLE_NULLABLE:
            ensure_tariff_title_column_for_table(ch, db, t)
        for t in _TABLES_CLIENT_TYPE_NULLABLE:
            ensure_client_type_column_for_table(ch, db, t)
        for t in _TABLES_CLIENT_TYPE_NULLABLE:
            ensure_client_type_title_column_for_table(ch, db, t)
        ensure_tariff_title_column_for_table(ch, db, "dim_tariff")
        ensure_kpi_arpu_active_clients_column(ch, db)
        ensure_kpi_arpu_monthly_column(ch, db)
    except Exception:  # noqa: BLE001
        _LOGGER.exception("ensure_flow_metric_columns_at_startup")
