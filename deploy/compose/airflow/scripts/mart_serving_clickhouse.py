"""Копирование KPI-таблиц и ``dim_tariff`` из Iceberg MART (Trino) в ClickHouse (serving) **как есть**.

Режим **полной таблицы** (флаг ``--full-table`` в DAG): ``TRUNCATE`` целевой таблицы в ClickHouse
и потоковый ``INSERT`` всех строк из Iceberg **без** фильтра по дате — без привязки к дню
загрузки RAW/DDS.

Режим **за один день** (``--report-date``): подсчёт и синхронизация только строк с этим
``report_date`` / ``period_end``; в ClickHouse выполняется ``ALTER DELETE`` за день, затем вставка.

Параметры окружения (как в compose ``.env``): ``CLICKHOUSE_HOST``, ``CLICKHOUSE_DB``,
``CLICKHOUSE_USER``, ``CLICKHOUSE_PASSWORD``. Клиент **clickhouse-connect** ходит по **HTTP**
на порт **8123** (внутри docker-сети к сервису ``clickhouse``); переопределение:
``CLICKHOUSE_INTERNAL_HTTP_PORT``. Порт **9000** — только нативный ``clickhouse-client``, для
этого драйвера не подходит (сервер ответит 400).
"""
from __future__ import annotations

import argparse
import logging
import os
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Sequence

import clickhouse_connect
import trino.dbapi

from load_raw_day import (
    build_trino_target_config,
    connect_trino,
    load_config,
    quote_ident,
    quote_table,
    setup_logging,
    sql_date,
)
from mart_runner_common import KPI_TABLES_WITH_CLIENT_TYPE, mart_schema_from_config

LOGGER = logging.getLogger(__name__)

MART_TABLE_NAMES: tuple[str, ...] = (
    "kpi_ab0_daily",
    "kpi_ab30_daily",
    "kpi_ab90_daily",
    "kpi_arpu_daily",
    "kpi_revenue_daily",
    "kpi_receipts_daily",
    "kpi_revenue_active_month",
    "kpi_revenue_active_week",
    "kpi_revenue_active_quarter",
    "kpi_revenue_active_year",
    "kpi_inflow_daily",
    "kpi_outflow_daily",
    "dim_tariff",
    "dim_segment",
)

SERVING_TABLE_CHOICES: tuple[str, ...] = MART_TABLE_NAMES

DATE_COLUMN_BY_TABLE: dict[str, str] = {
    "kpi_ab0_daily": "report_date",
    "kpi_ab30_daily": "report_date",
    "kpi_ab90_daily": "report_date",
    "kpi_arpu_daily": "report_date",
    "kpi_revenue_daily": "report_date",
    "kpi_receipts_daily": "report_date",
    "kpi_inflow_daily": "report_date",
    "kpi_outflow_daily": "report_date",
    "kpi_revenue_active_month": "period_end",
    "kpi_revenue_active_week": "period_end",
    "kpi_revenue_active_quarter": "period_end",
    "kpi_revenue_active_year": "period_end",
    "dim_tariff": "refreshed_at",
    "dim_segment": "refreshed_at",
}

SELECT_COLUMNS_BY_TABLE: dict[str, tuple[str, ...]] = {
    "kpi_ab0_daily": (
        "report_date",
        "segment_id",
        "service_kind",
        "tariff_id",
        "tariff_title",
        "client_type",
        "client_type_title",
        "active_subscribers",
        "refreshed_at",
    ),
    "kpi_ab30_daily": (
        "report_date",
        "segment_id",
        "service_kind",
        "tariff_id",
        "tariff_title",
        "client_type",
        "client_type_title",
        "active_subscribers",
        "refreshed_at",
    ),
    "kpi_ab90_daily": (
        "report_date",
        "segment_id",
        "service_kind",
        "tariff_id",
        "tariff_title",
        "client_type",
        "client_type_title",
        "active_subscribers",
        "refreshed_at",
    ),
    "kpi_revenue_daily": (
        "report_date",
        "segment_id",
        "service_kind",
        "tariff_id",
        "tariff_title",
        "client_type",
        "client_type_title",
        "total_revenue",
        "paying_owners",
        "refreshed_at",
    ),
    "kpi_receipts_daily": (
        "report_date",
        "segment_id",
        "service_kind",
        "tariff_id",
        "tariff_title",
        "client_type",
        "client_type_title",
        "total_receipts",
        "paying_owners",
        "refreshed_at",
    ),
    "kpi_arpu_daily": (
        "report_date",
        "month_start",
        "segment_id",
        "service_kind",
        "tariff_id",
        "tariff_title",
        "client_type",
        "client_type_title",
        "total_revenue",
        "active_clients",
        "arpu_daily",
        "arpu_monthly",
        "refreshed_at",
    ),
    "kpi_inflow_daily": (
        "report_date",
        "segment_id",
        "service_kind",
        "tariff_id",
        "tariff_title",
        "client_type",
        "client_type_title",
        "new_clients",
        "new_agreements",
        "new_orders",
        "refreshed_at",
    ),
    "kpi_outflow_daily": (
        "report_date",
        "segment_id",
        "service_kind",
        "tariff_id",
        "tariff_title",
        "client_type",
        "client_type_title",
        "churned_clients",
        "completed_agreements",
        "completed_orders",
        "refreshed_at",
    ),
    "kpi_revenue_active_month": (
        "period_end",
        "segment_id",
        "service_kind",
        "tariff_id",
        "tariff_title",
        "total_revenue",
        "paying_owners",
        "refreshed_at",
    ),
    "kpi_revenue_active_week": (
        "period_end",
        "segment_id",
        "service_kind",
        "tariff_id",
        "tariff_title",
        "total_revenue",
        "paying_owners",
        "refreshed_at",
    ),
    "kpi_revenue_active_quarter": (
        "period_end",
        "segment_id",
        "service_kind",
        "tariff_id",
        "tariff_title",
        "total_revenue",
        "paying_owners",
        "refreshed_at",
    ),
    "kpi_revenue_active_year": (
        "period_end",
        "segment_id",
        "service_kind",
        "tariff_id",
        "tariff_title",
        "total_revenue",
        "paying_owners",
        "refreshed_at",
    ),
    "dim_tariff": (
        "tariff_id",
        "tariff_title",
        "refreshed_at",
    ),
    "dim_segment": (
        "segment_id",
        "parent_id",
        "segment_title",
        "refreshed_at",
    ),
}


def _ch_ident(name: str) -> str:
    """Экранирование идентификатора ClickHouse обратными кавычками."""
    return "`" + name.replace("`", "``") + "`"


def _ch_qualified_table(database: str, table: str) -> str:
    """Полное имя таблицы ClickHouse: ``db`.`table``."""
    return f"{_ch_ident(database)}.{_ch_ident(table)}"


_FLOW_METRIC_COLS: dict[str, tuple[tuple[str, str], ...]] = {
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

_TARIFF_TITLE_TABLES_NULLABLE: tuple[str, ...] = (
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

_CLIENT_TYPE_TABLES: frozenset[str] = KPI_TABLES_WITH_CLIENT_TYPE


def _ensure_tariff_title_column_ch(ch: Any, database: str, logical_table: str) -> None:
    """Добавляет колонку ``tariff_title`` в serving-таблицу ClickHouse, если отсутствует.

    Тип зависит от таблицы: ``dim_tariff`` → ``String DEFAULT ''`` (NOT NULL),
    KPI-таблицы → ``Nullable(String)``.

    Args:
        ch: Клиент clickhouse-connect.
        database: База данных ClickHouse.
        logical_table: Логическое имя таблицы.
    """
    if logical_table == "dim_tariff":
        type_sql = "String DEFAULT ''"
    elif logical_table in _TARIFF_TITLE_TABLES_NULLABLE:
        type_sql = "Nullable(String)"
    else:
        return
    try:
        r = ch.query(
            "SELECT count() FROM system.tables WHERE database = {db:String} AND name = {tb:String}",
            parameters={"db": database, "tb": logical_table},
        )
        if not r.result_rows or int(r.result_rows[0][0]) == 0:
            return
    except Exception:
        LOGGER.exception("ensure tariff_title: table check %s", logical_table)
        return
    try:
        r = ch.query(
            "SELECT name FROM system.columns WHERE database = {db:String} AND table = {tb:String}",
            parameters={"db": database, "tb": logical_table},
        )
        existing = {str(row[0]).lower() for row in r.result_rows}
    except Exception:
        LOGGER.exception("ensure tariff_title: columns %s", logical_table)
        return
    if "tariff_title" in existing:
        return
    qt = _ch_qualified_table(database, logical_table)
    sql = f"ALTER TABLE {qt} ADD COLUMN IF NOT EXISTS {_ch_ident('tariff_title')} {type_sql}"
    try:
        ch.command(sql)
        LOGGER.info("[INFO] serving schema: %s", sql)
    except Exception:
        LOGGER.exception("ensure tariff_title: %s", sql)


def _ensure_client_type_column_ch(ch: Any, database: str, logical_table: str) -> None:
    """Добавляет колонку ``client_type`` в serving-таблицу, если отсутствует.

    Только для таблиц из ``_CLIENT_TYPE_TABLES`` (дашборд «Абонентская база»).

    Args:
        ch: Клиент clickhouse-connect.
        database: База данных ClickHouse.
        logical_table: Логическое имя таблицы.
    """
    if logical_table not in _CLIENT_TYPE_TABLES:
        return
    try:
        r = ch.query(
            "SELECT count() FROM system.tables WHERE database = {db:String} AND name = {tb:String}",
            parameters={"db": database, "tb": logical_table},
        )
        if not r.result_rows or int(r.result_rows[0][0]) == 0:
            return
    except Exception:
        LOGGER.exception("ensure client_type: table check %s", logical_table)
        return
    try:
        r = ch.query(
            "SELECT name FROM system.columns WHERE database = {db:String} AND table = {tb:String}",
            parameters={"db": database, "tb": logical_table},
        )
        existing = {str(row[0]).lower() for row in r.result_rows}
    except Exception:
        LOGGER.exception("ensure client_type: columns %s", logical_table)
        return
    if "client_type" in existing:
        return
    qt = _ch_qualified_table(database, logical_table)
    sql = f"ALTER TABLE {qt} ADD COLUMN IF NOT EXISTS {_ch_ident('client_type')} Nullable(String)"
    try:
        ch.command(sql)
        LOGGER.info("[INFO] serving schema: %s", sql)
    except Exception:
        LOGGER.exception("ensure client_type: %s", sql)


def _ensure_client_type_title_column_ch(ch: Any, database: str, logical_table: str) -> None:
    """Добавляет колонку ``client_type_title`` (подпись типа клиента).

    Заполняется из ``client_property_dim`` при копировании из Iceberg.

    Args:
        ch: Клиент clickhouse-connect.
        database: База данных ClickHouse.
        logical_table: Логическое имя таблицы.
    """
    if logical_table not in _CLIENT_TYPE_TABLES:
        return
    try:
        r = ch.query(
            "SELECT count() FROM system.tables WHERE database = {db:String} AND name = {tb:String}",
            parameters={"db": database, "tb": logical_table},
        )
        if not r.result_rows or int(r.result_rows[0][0]) == 0:
            return
    except Exception:
        LOGGER.exception("ensure client_type_title: table check %s", logical_table)
        return
    try:
        r = ch.query(
            "SELECT name FROM system.columns WHERE database = {db:String} AND table = {tb:String}",
            parameters={"db": database, "tb": logical_table},
        )
        existing = {str(row[0]).lower() for row in r.result_rows}
    except Exception:
        LOGGER.exception("ensure client_type_title: columns %s", logical_table)
        return
    if "client_type_title" in existing:
        return
    qt = _ch_qualified_table(database, logical_table)
    sql = f"ALTER TABLE {qt} ADD COLUMN IF NOT EXISTS {_ch_ident('client_type_title')} Nullable(String)"
    try:
        ch.command(sql)
        LOGGER.info("[INFO] serving schema: %s", sql)
    except Exception:
        LOGGER.exception("ensure client_type_title: %s", sql)


def _ensure_kpi_arpu_active_clients_ch(ch: Any, database: str, logical_table: str) -> None:
    """Миграция схемы ``kpi_arpu_daily`` в CH: ``paying_owners`` → ``active_clients``.

    Старая схема использовала ``paying_owners``; актуальная MART — ``active_clients``.
    При наличии старой колонки выполняется ``RENAME COLUMN``; иначе ``ADD COLUMN``.

    Args:
        ch: Клиент clickhouse-connect.
        database: База данных ClickHouse.
        logical_table: Должно быть ``"kpi_arpu_daily"``.
    """
    if logical_table != "kpi_arpu_daily":
        return
    try:
        r = ch.query(
            "SELECT count() FROM system.tables WHERE database = {db:String} AND name = {tb:String}",
            parameters={"db": database, "tb": logical_table},
        )
        if not r.result_rows or int(r.result_rows[0][0]) == 0:
            return
    except Exception:
        LOGGER.exception("ensure kpi_arpu active_clients: table check")
        return
    try:
        r = ch.query(
            "SELECT name FROM system.columns WHERE database = {db:String} AND table = {tb:String}",
            parameters={"db": database, "tb": logical_table},
        )
        by_lower: dict[str, str] = {str(row[0]).lower(): str(row[0]) for row in r.result_rows}
    except Exception:
        LOGGER.exception("ensure kpi_arpu active_clients: columns")
        return
    if "active_clients" in by_lower:
        return
    qt = _ch_qualified_table(database, logical_table)
    if "paying_owners" in by_lower:
        old = by_lower["paying_owners"]
        sql = f"ALTER TABLE {qt} RENAME COLUMN {_ch_ident(old)} TO {_ch_ident('active_clients')}"
    else:
        sql = f"ALTER TABLE {qt} ADD COLUMN IF NOT EXISTS {_ch_ident('active_clients')} Int64 DEFAULT 0"
    try:
        ch.command(sql)
        LOGGER.info("[INFO] serving schema: %s", sql)
    except Exception:
        LOGGER.exception("ensure kpi_arpu active_clients: %s", sql)


def _ensure_kpi_arpu_monthly_column_ch(ch: Any, database: str, logical_table: str) -> None:
    """Добавляет колонку ``arpu_monthly Nullable(Float64)`` в ``kpi_arpu_daily`` CH.

    Args:
        ch: Клиент clickhouse-connect.
        database: База данных ClickHouse.
        logical_table: Должно быть ``"kpi_arpu_daily"``.
    """
    if logical_table != "kpi_arpu_daily":
        return
    try:
        r = ch.query(
            "SELECT count() FROM system.tables WHERE database = {db:String} AND name = {tb:String}",
            parameters={"db": database, "tb": logical_table},
        )
        if not r.result_rows or int(r.result_rows[0][0]) == 0:
            return
    except Exception:
        LOGGER.exception("ensure kpi_arpu arpu_monthly: table check")
        return
    try:
        r = ch.query(
            "SELECT name FROM system.columns WHERE database = {db:String} AND table = {tb:String}",
            parameters={"db": database, "tb": logical_table},
        )
        existing = {str(row[0]).lower() for row in r.result_rows}
    except Exception:
        LOGGER.exception("ensure kpi_arpu arpu_monthly: columns")
        return
    if "arpu_monthly" in existing:
        return
    qt = _ch_qualified_table(database, logical_table)
    sql = f"ALTER TABLE {qt} ADD COLUMN IF NOT EXISTS {_ch_ident('arpu_monthly')} Nullable(Float64)"
    try:
        ch.command(sql)
        LOGGER.info("[INFO] serving schema: %s", sql)
    except Exception:
        LOGGER.exception("ensure kpi_arpu arpu_monthly: %s", sql)


def _ensure_flow_metric_columns_ch(ch: Any, database: str, logical_table: str) -> None:
    """Добавляет недостающие метрические колонки в serving-таблицы притока/оттока.

    ``kpi_inflow_daily``: ``new_clients``, ``new_agreements``, ``new_orders``.
    ``kpi_outflow_daily``: ``churned_clients``, ``completed_agreements``, ``completed_orders``.
    Тип всех колонок — ``Int64 DEFAULT 0``.

    Args:
        ch: Клиент clickhouse-connect.
        database: База данных ClickHouse.
        logical_table: ``kpi_inflow_daily`` или ``kpi_outflow_daily``.
    """
    pairs = _FLOW_METRIC_COLS.get(logical_table)
    if not pairs:
        return
    try:
        r = ch.query(
            "SELECT count() FROM system.tables WHERE database = {db:String} AND name = {tb:String}",
            parameters={"db": database, "tb": logical_table},
        )
        if not r.result_rows or int(r.result_rows[0][0]) == 0:
            return
    except Exception:
        LOGGER.exception("ensure flow metrics: table check %s", logical_table)
        return
    try:
        r = ch.query(
            "SELECT name FROM system.columns WHERE database = {db:String} AND table = {tb:String}",
            parameters={"db": database, "tb": logical_table},
        )
        existing = {str(row[0]).lower() for row in r.result_rows}
    except Exception:
        LOGGER.exception("ensure flow metrics: columns %s", logical_table)
        return
    qt = _ch_qualified_table(database, logical_table)
    for col_name, typ in pairs:
        if col_name.lower() in existing:
            continue
        sql = f"ALTER TABLE {qt} ADD COLUMN IF NOT EXISTS {_ch_ident(col_name)} {typ} DEFAULT 0"
        try:
            ch.command(sql)
            LOGGER.info("[INFO] serving schema: %s", sql)
        except Exception:
            LOGGER.exception("ensure flow metrics: %s", sql)


def _clickhouse_http_port() -> int:
    """Порт ClickHouse HTTP (из env ``CLICKHOUSE_INTERNAL_HTTP_PORT`` или 8123).

    Returns:
        Номер порта.
    """
    raw = os.getenv("CLICKHOUSE_INTERNAL_HTTP_PORT", "").strip()
    if raw:
        return int(raw)
    return 8123


def _clickhouse_client():
    """Создаёт и возвращает клиент clickhouse-connect.

    Параметры подключения из переменных окружения:
    ``CLICKHOUSE_HOST``, ``CLICKHOUSE_DB``, ``CLICKHOUSE_USER``, ``CLICKHOUSE_PASSWORD``.
    Порт — результат ``_clickhouse_http_port()``.

    Returns:
        Экземпляр ``clickhouse_connect.Client``.
    """
    host = os.getenv("CLICKHOUSE_HOST", "clickhouse").strip()
    port = _clickhouse_http_port()
    database = os.getenv("CLICKHOUSE_DB", "serving").strip()
    user = os.getenv("CLICKHOUSE_USER", "default").strip()
    password = os.getenv("CLICKHOUSE_PASSWORD", "").strip()
    return clickhouse_connect.get_client(
        host=host,
        port=port,
        username=user,
        password=password,
        database=database,
    )


def _cell_for_ch(val: Any) -> Any:
    """Приведение значения ячейки из Trino в формат, совместимый с ClickHouse.

    Тип-маппинг Iceberg → ClickHouse:
    - ``None`` → ``None``.
    - ``Decimal`` → ``float`` (ClickHouse Float64).
    - ``datetime`` с tz → UTC без tz (DateTime64 ClickHouse не принимает tz).
    - Остальные типы передаются как есть.

    Args:
        val: Значение из Trino (через ``trino.dbapi``).

    Returns:
        Значение для вставки в ClickHouse.
    """
    if val is None:
        return None
    if isinstance(val, Decimal):
        return float(val)
    if isinstance(val, datetime):
        if val.tzinfo is not None:
            return val.astimezone(timezone.utc).replace(tzinfo=None)
        return val
    return val


def _ddl_clickhouse(database: str, logical_name: str) -> str:
    """DDL ``CREATE TABLE IF NOT EXISTS`` для ClickHouse serving.

    Типы и имена колонок согласованы с Iceberg MART. Для ``dim_*`` таблиц —
    ``ORDER BY (id)``, для ``kpi_*`` — ``PARTITION BY toYYYYMM(date_col)``
    + ``ORDER BY (date, coalesce(dim, default), …)``.

    Args:
        database: База данных ClickHouse (например ``serving``).
        logical_name: Логическое имя таблицы (например ``kpi_ab0_daily``).

    Returns:
        Строка ``CREATE TABLE IF NOT EXISTS``.

    Raises:
        ValueError: Если ``logical_name`` не соответствует ни одной serving-таблице.
    """
    t = _ch_qualified_table(database, logical_name)
    if logical_name == "kpi_ab0_daily":
        return f"""
        CREATE TABLE IF NOT EXISTS {t} (
            report_date Date NOT NULL,
            segment_id Nullable(Int64),
            service_kind Nullable(String),
            tariff_id Nullable(Int64),
            tariff_title Nullable(String),
            client_type Nullable(String),
            client_type_title Nullable(String),
            active_subscribers Int64 NOT NULL,
            refreshed_at DateTime64(6, 'UTC') NOT NULL
        ) ENGINE = MergeTree
        PARTITION BY toYYYYMM(report_date)
        ORDER BY (report_date, coalesce(segment_id, toInt64(-1)), coalesce(tariff_id, toInt64(-1)), coalesce(service_kind, ''))
        """
    if logical_name in {"kpi_ab30_daily", "kpi_ab90_daily"}:
        return f"""
        CREATE TABLE IF NOT EXISTS {t} (
            report_date Date NOT NULL,
            segment_id Nullable(Int64),
            service_kind Nullable(String),
            tariff_id Nullable(Int64),
            tariff_title Nullable(String),
            client_type Nullable(String),
            client_type_title Nullable(String),
            active_subscribers Int64 NOT NULL,
            refreshed_at DateTime64(6, 'UTC') NOT NULL
        ) ENGINE = MergeTree
        PARTITION BY toYYYYMM(report_date)
        ORDER BY (report_date, coalesce(segment_id, toInt64(-1)), coalesce(tariff_id, toInt64(-1)), coalesce(service_kind, ''))
        """
    if logical_name == "kpi_revenue_daily":
        return f"""
        CREATE TABLE IF NOT EXISTS {t} (
            report_date Date NOT NULL,
            segment_id Nullable(Int64),
            service_kind Nullable(String),
            tariff_id Nullable(Int64),
            tariff_title Nullable(String),
            client_type Nullable(String),
            client_type_title Nullable(String),
            total_revenue Float64 NOT NULL,
            paying_owners Int64 NOT NULL,
            refreshed_at DateTime64(6, 'UTC') NOT NULL
        ) ENGINE = MergeTree
        PARTITION BY toYYYYMM(report_date)
        ORDER BY (report_date, coalesce(segment_id, toInt64(-1)), coalesce(tariff_id, toInt64(-1)), coalesce(service_kind, ''))
        """
    if logical_name == "kpi_receipts_daily":
        return f"""
        CREATE TABLE IF NOT EXISTS {t} (
            report_date Date NOT NULL,
            segment_id Nullable(Int64),
            service_kind Nullable(String),
            tariff_id Nullable(Int64),
            tariff_title Nullable(String),
            client_type Nullable(String),
            client_type_title Nullable(String),
            total_receipts Float64 NOT NULL,
            paying_owners Int64 NOT NULL,
            refreshed_at DateTime64(6, 'UTC') NOT NULL
        ) ENGINE = MergeTree
        PARTITION BY toYYYYMM(report_date)
        ORDER BY (report_date, coalesce(segment_id, toInt64(-1)), coalesce(tariff_id, toInt64(-1)), coalesce(service_kind, ''))
        """
    if logical_name == "kpi_arpu_daily":
        return f"""
        CREATE TABLE IF NOT EXISTS {t} (
            report_date Date NOT NULL,
            month_start Date NOT NULL,
            segment_id Nullable(Int64),
            service_kind Nullable(String),
            tariff_id Nullable(Int64),
            tariff_title Nullable(String),
            client_type Nullable(String),
            client_type_title Nullable(String),
            total_revenue Float64 NOT NULL,
            active_clients Int64 NOT NULL,
            arpu_daily Nullable(Float64),
            arpu_monthly Nullable(Float64),
            refreshed_at DateTime64(6, 'UTC') NOT NULL
        ) ENGINE = MergeTree
        PARTITION BY toYYYYMM(report_date)
        ORDER BY (report_date, month_start, coalesce(segment_id, toInt64(-1)), coalesce(tariff_id, toInt64(-1)), coalesce(service_kind, ''))
        """
    if logical_name == "kpi_inflow_daily":
        return f"""
        CREATE TABLE IF NOT EXISTS {t} (
            report_date Date NOT NULL,
            segment_id Nullable(Int64),
            service_kind Nullable(String),
            tariff_id Nullable(Int64),
            tariff_title Nullable(String),
            client_type Nullable(String),
            client_type_title Nullable(String),
            new_clients Int64 NOT NULL,
            new_agreements Int64 NOT NULL,
            new_orders Int64 NOT NULL,
            refreshed_at DateTime64(6, 'UTC') NOT NULL
        ) ENGINE = MergeTree
        PARTITION BY toYYYYMM(report_date)
        ORDER BY (report_date, coalesce(segment_id, toInt64(-1)), coalesce(tariff_id, toInt64(-1)), coalesce(service_kind, ''))
        """
    if logical_name == "kpi_outflow_daily":
        return f"""
        CREATE TABLE IF NOT EXISTS {t} (
            report_date Date NOT NULL,
            segment_id Nullable(Int64),
            service_kind Nullable(String),
            tariff_id Nullable(Int64),
            tariff_title Nullable(String),
            client_type Nullable(String),
            client_type_title Nullable(String),
            churned_clients Int64 NOT NULL,
            completed_agreements Int64 NOT NULL,
            completed_orders Int64 NOT NULL,
            refreshed_at DateTime64(6, 'UTC') NOT NULL
        ) ENGINE = MergeTree
        PARTITION BY toYYYYMM(report_date)
        ORDER BY (report_date, coalesce(segment_id, toInt64(-1)), coalesce(tariff_id, toInt64(-1)), coalesce(service_kind, ''))
        """
    if logical_name == "dim_tariff":
        return f"""
        CREATE TABLE IF NOT EXISTS {t} (
            tariff_id Int64 NOT NULL,
            tariff_title String NOT NULL,
            refreshed_at DateTime64(6, 'UTC') NOT NULL
        ) ENGINE = MergeTree
        ORDER BY (tariff_id)
        """
    if logical_name == "dim_segment":
        return f"""
        CREATE TABLE IF NOT EXISTS {t} (
            segment_id Int64 NOT NULL,
            parent_id Nullable(Int64),
            segment_title String NOT NULL,
            refreshed_at DateTime64(6, 'UTC') NOT NULL
        ) ENGINE = MergeTree
        ORDER BY (segment_id)
        """
    if logical_name in {
        "kpi_revenue_active_month",
        "kpi_revenue_active_week",
        "kpi_revenue_active_quarter",
        "kpi_revenue_active_year",
    }:
        return f"""
        CREATE TABLE IF NOT EXISTS {t} (
            period_end Date NOT NULL,
            segment_id Nullable(Int64),
            service_kind Nullable(String),
            tariff_id Nullable(Int64),
            tariff_title Nullable(String),
            total_revenue Float64 NOT NULL,
            paying_owners Int64 NOT NULL,
            refreshed_at DateTime64(6, 'UTC') NOT NULL
        ) ENGINE = MergeTree
        PARTITION BY toYYYYMM(period_end)
        ORDER BY (period_end, coalesce(segment_id, toInt64(-1)), coalesce(tariff_id, toInt64(-1)), coalesce(service_kind, ''))
        """
    raise ValueError(f"Неизвестная таблица для DDL ClickHouse: {logical_name}")


def _source_count_sql(
    *,
    src_table_sql: str,
    date_col: str,
    report_day: date,
) -> str:
    """SQL для подсчёта строк в Trino за один календарный день.

    Args:
        src_table_sql: Полное имя исходной таблицы (``catalog.schema.table``).
        date_col: Имя колонки даты (``report_date`` или ``period_end``).
        report_day: Календарный день.

    Returns:
        Строка ``SELECT count(*) … WHERE date_col = 'YYYY-MM-DD'``.
    """
    rd = sql_date(report_day)
    return f"SELECT count(*) FROM {src_table_sql} WHERE {quote_ident(date_col)} = {rd}"


def _source_select_sql(
    *,
    src_table_sql: str,
    date_col: str,
    report_day: date,
    columns: Sequence[str],
) -> str:
    """SQL для чтения строк из Trino за один календарный день.

    Args:
        src_table_sql: Полное имя исходной таблицы.
        date_col: Имя колонки даты.
        report_day: Календарный день.
        columns: Список колонок для SELECT.

    Returns:
        Строка ``SELECT col1, col2, … WHERE date_col = 'YYYY-MM-DD'``.
    """
    rd = sql_date(report_day)
    cols = ", ".join(quote_ident(c) for c in columns)
    return f"SELECT {cols} FROM {src_table_sql} WHERE {quote_ident(date_col)} = {rd}"


def _source_count_full_sql(*, src_table_sql: str) -> str:
    """SQL для подсчёта всех строк в таблице Trino (режим full-table).

    Args:
        src_table_sql: Полное имя исходной таблицы.

    Returns:
        ``SELECT count(*) FROM table``.
    """
    return f"SELECT count(*) FROM {src_table_sql}"


def _source_select_full_sql(*, src_table_sql: str, columns: Sequence[str]) -> str:
    """SQL для чтения всех строк из таблицы Trino (режим full-table).

    Args:
        src_table_sql: Полное имя исходной таблицы.
        columns: Список колонок для SELECT.

    Returns:
        ``SELECT col1, col2, … FROM table``.
    """
    cols = ", ".join(quote_ident(c) for c in columns)
    return f"SELECT {cols} FROM {src_table_sql}"


def sync_mart_table_to_clickhouse(
    *,
    config_path: str,
    logical_table: str,
    allow_empty_source: bool,
    batch_size: int,
    dry_run: bool,
    full_table: bool,
    report_day: date | None = None,
) -> int:
    """Основная функция синхронизации одной таблицы MART → ClickHouse.

    Режимы:
    - ``full_table=True``: ``TRUNCATE`` целевой таблицы CH + потоковый ``INSERT`` всех строк из Iceberg.
    - ``report_day`` задан: ``ALTER DELETE`` за день → ``INSERT`` строк за этот день.

    Шаги:
    1. Подсчёт строк в Trino (count).
    2. Создание DDL целевой таблицы CH и миграции колонок.
    3. Очистка: ``TRUNCATE`` (full) или ``ALTER DELETE WHERE date = '…'`` (daily).
       В daily-режиме включается ``mutations_sync = 1`` для синхронного выполнения мутации.
    4. Потоковое чтение Trino → вставка в CH батчами по ``batch_size``.

    Args:
        config_path: Путь к raw_load_config.json.
        logical_table: Имя таблицы в MART (например ``kpi_ab0_daily``).
        allow_empty_source: Если True — не пропускать при 0 строк в источнике.
        batch_size: Размер батча чтения/вставки.
        dry_run: Только подсчёт и логирование, без записи в CH.
        full_table: Режим полной таблицы.
        report_day: Календарный день для daily-режима.

    Returns:
        Число строк источника (до вставки).

    Raises:
        ValueError: Для dim_* таблиц без full_table.
    """
    if logical_table in {"dim_tariff", "dim_segment"} and not full_table:
        raise ValueError(
            f"{logical_table}: только полная выгрузка --full-table (копия из mart Iceberg в serving как есть)"
        )

    if logical_table not in SELECT_COLUMNS_BY_TABLE:
        raise ValueError(f"Неизвестная таблица: {logical_table}. Допустимо: {SERVING_TABLE_CHOICES}")

    config = load_config(config_path)
    target_cfg = config.get("target", {})
    mart_schema = mart_schema_from_config(config)
    trino_target = build_trino_target_config(
        target_cfg, fallback_schema=str(target_cfg.get("default_schema", "raw"))
    )
    catalog = trino_target.catalog
    src_table = quote_table(catalog, mart_schema, logical_table)
    date_col = DATE_COLUMN_BY_TABLE[logical_table]
    columns = SELECT_COLUMNS_BY_TABLE[logical_table]

    ch_db = os.getenv("CLICKHOUSE_DB", "serving").strip()

    if full_table:
        report_day = None
    elif report_day is None:
        raise ValueError("Задайте report_day или включите full_table")

    # Шаг 1: подсчёт строк в источнике
    with connect_trino(trino_target) as conn:
        with conn.cursor() as cur:
            cur.execute(trino_target.verify_connection_sql)
            _ = cur.fetchone()
            if full_table:
                cur.execute(_source_count_full_sql(src_table_sql=src_table))
            else:
                cur.execute(
                    _source_count_sql(
                        src_table_sql=src_table,
                        date_col=date_col,
                        report_day=report_day,
                    )
                )
            row = cur.fetchone()
            n_src = int(row[0]) if row and row[0] is not None else 0

    scope = "full_table" if full_table else f"day={report_day.isoformat()}"
    LOGGER.info(
        "[INFO] mart→ch table=%s scope=%s src_rows=%s allow_empty=%s dry_run=%s",
        logical_table,
        scope,
        n_src,
        allow_empty_source,
        dry_run,
    )

    if n_src == 0 and not allow_empty_source:
        LOGGER.warning(
            "[WARN] источник Trino пуст для %s (%s) — пропуск (ClickHouse не меняем)",
            logical_table,
            scope,
        )
        return 0

    if dry_run:
        return n_src

    # Шаг 2: создание/миграция схемы в ClickHouse
    ch = _clickhouse_client()
    ch.command("CREATE DATABASE IF NOT EXISTS " + _ch_ident(ch_db))
    ch.command(_ddl_clickhouse(ch_db, logical_table).strip())
    _ensure_flow_metric_columns_ch(ch, ch_db, logical_table)
    _ensure_tariff_title_column_ch(ch, ch_db, logical_table)
    _ensure_client_type_column_ch(ch, ch_db, logical_table)
    _ensure_client_type_title_column_ch(ch, ch_db, logical_table)
    _ensure_kpi_arpu_active_clients_ch(ch, ch_db, logical_table)
    _ensure_kpi_arpu_monthly_column_ch(ch, ch_db, logical_table)

    # Шаг 3: очистка целевого среза
    if full_table:
        ch.command(f"TRUNCATE TABLE IF EXISTS {_ch_qualified_table(ch_db, logical_table)}")
    else:
        # mutations_sync = 1 — ждём завершения мутации ALTER DELETE (иначе данные
        # могут дублироваться при немедленном INSERT)
        ch.command("SET mutations_sync = 1")
        assert report_day is not None
        literal_date = report_day.isoformat()
        delete_sql = (
            f"ALTER TABLE {_ch_qualified_table(ch_db, logical_table)} "
            f"DELETE WHERE {_ch_ident(date_col)} = '{literal_date}'"
        )
        ch.command(delete_sql)

    # Шаг 4: потоковое копирование Trino → ClickHouse
    select_sql = (
        _source_select_full_sql(src_table_sql=src_table, columns=columns)
        if full_table
        else _source_select_sql(
            src_table_sql=src_table,
            date_col=date_col,
            report_day=report_day,
            columns=columns,
        )
    )
    inserted = 0
    with connect_trino(trino_target) as conn:
        with conn.cursor() as cur:
            cur.execute(select_sql)
            while True:
                rows = cur.fetchmany(batch_size)
                if not rows:
                    break
                norm = [tuple(_cell_for_ch(v) for v in r) for r in rows]
                ch.insert(logical_table, norm, column_names=list(columns))
                inserted += len(norm)

    LOGGER.info("[INFO] mart→ch table=%s inserted=%s", logical_table, inserted)
    return inserted


def parse_args() -> argparse.Namespace:
    """Разбор аргументов CLI для ``mart_serving_clickhouse.py``.

    Два режима:
    - ``--full-table``: полная выгрузка (TRUNCATE + INSERT всех строк).
    - ``--report-date YYYY-MM-DD``: инкремент за один день (ALTER DELETE + INSERT).

    Для ``dim_tariff`` / ``dim_segment`` доступен только ``--full-table``.

    Returns:
        Namespace с полями: config, full_table, report_date, table, allow_empty_source,
        batch_size, dry_run, log_level.
    """
    p = argparse.ArgumentParser(description="Синхронизация KPI MART (Trino/Iceberg) → ClickHouse")
    p.add_argument(
        "--config",
        default="/opt/airflow/scripts/raw_load_config.json",
        help="JSON с секциями target / pipeline",
    )
    p.add_argument(
        "--full-table",
        action="store_true",
        help="Полная выгрузка: TRUNCATE целевой таблицы в ClickHouse и INSERT всех строк из Iceberg (без фильтра по дате)",
    )
    p.add_argument(
        "--report-date",
        default=None,
        help="Режим по одному календарному дню: YYYY-MM-DD (альтернатива --full-table)",
    )
    p.add_argument(
        "--table",
        required=True,
        choices=SERVING_TABLE_CHOICES,
        help="Имя таблицы в mart (Iceberg), например kpi_ab0_daily или dim_tariff",
    )
    p.add_argument(
        "--allow-empty-source",
        action="store_true",
        help="Если в Iceberg 0 строк — всё равно очистить срез в ClickHouse (DELETE за день или TRUNCATE при --full-table)",
    )
    p.add_argument("--batch-size", type=int, default=20_000, help="Размер батча чтения Trino / вставки CH")
    p.add_argument("--dry-run", action="store_true", help="Только логирование и подсчёт в Trino")
    p.add_argument(
        "--log-level",
        default=os.getenv("RAW_LOADER_LOG_LEVEL", "INFO"),
    )
    ns = p.parse_args()
    if ns.full_table and ns.report_date:
        p.error("Нельзя одновременно указывать --full-table и --report-date")
    if not ns.full_table and not ns.report_date:
        p.error("Укажите --full-table или --report-date YYYY-MM-DD")
    if ns.table in {"dim_tariff", "dim_segment"} and not ns.full_table:
        p.error(f"{ns.table}: только --full-table (полная копия из mart в serving)")
    return ns


def main() -> None:
    args = parse_args()
    setup_logging(args.log_level)
    sync_mart_table_to_clickhouse(
        config_path=args.config,
        logical_table=args.table,
        allow_empty_source=args.allow_empty_source,
        batch_size=int(args.batch_size),
        dry_run=args.dry_run,
        full_table=args.full_table,
        report_day=date.fromisoformat(args.report_date) if args.report_date else None,
    )


if __name__ == "__main__":
    main()
