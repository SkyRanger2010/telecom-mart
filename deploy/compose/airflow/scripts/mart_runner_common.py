"""
SQL и метаданные для KPI-витрин в слое MART.

Сюда собрано всё, что не нужно CLI: сборка INSERT/DELETE, DDL при первом запуске,
разбор ключей из конфига. Исполнение в Trino — в `mart_vitrina_runner.run_generate`.
Источник фактов: Iceberg DDS (dds_orders, dds_agreements, dds_clients, dds_ledgers);
kpi_revenue_daily — credits ÷ дней [date_start, date_end] проводки, если report_date в интервале и заказ ENABLED;
kpi_arpu_daily — та же дневная выручка / активные AB0 на дату;
kpi_revenue_active_* — сумма kpi_revenue_daily за календарный период, paying_owners с ARPU на конец периода.
Поля ``client_type`` / ``client_type_title`` в AB/приток/отток: код из ``dds_clients.type``,
человекочитаемое имя — из ``dds_client_property_snapshot.title`` (public.client_property), иначе запасной текст по коду.

АБ0: прямой COUNT(DISTINCT subscriber_id) на дату среза — все клиенты с хотя бы одним
активным заказом (ENABLED, activated, не истёк).
АБ30/АБ90: прямой COUNT(DISTINCT subscriber_id) за окно 30/90 дней — абоненты,
у которых был хотя бы один день с активным заказом в окне [D−N+1, D].
Все три витрины АБ — один проход по sub_ord (вызов gen_kpi_ab0 считает AB0+AB30+AB90).
АБ30 — с 30-го дня витрины, АБ90 — с 90-го; раньше только снимок окна 30/90 дней.
Задачи Airflow: inflow и outflow до ab0/ab30/ab90.
"""
from __future__ import annotations

import calendar
import logging
from datetime import date, timedelta
from typing import Any

from load_raw_day import quote_schema, quote_table, sql_date

LOGGER = logging.getLogger(__name__)

# Full snapshot DDS для справочника свойств клиента (raw_load_config: public.client_property, mode full).
DDS_CLIENT_PROPERTY_SNAPSHOT_TABLE = "dds_client_property_snapshot"

# Таблицы mart с разрезом tariff_id: витрина хранит tariff_title из dim_tariff (обновляется задачей dim_tariff).
KPI_TABLES_WITH_TARIFF_TITLE: frozenset[str] = frozenset(
    {
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
    }
)


# Таблицы mart, в которые пишется client_type (тип клиента из DDS clients).
KPI_TABLES_WITH_CLIENT_TYPE: frozenset[str] = frozenset(
    {
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
    }
)


def add_tariff_title_column_sql(*, catalog: str, mart_schema: str, logical_table: str) -> str:
    """DDL ``ALTER TABLE … ADD COLUMN tariff_title varchar`` для существующих Iceberg-таблиц.

    Вызывается после проверки ``information_schema.columns`` (только если колонки ещё нет).

    Args:
        catalog: Имя каталога Trino (например ``iceberg``).
        mart_schema: Схема MART (например ``mart``).
        logical_table: Логическое имя таблицы (например ``kpi_ab0_daily``).

    Returns:
        Строка DDL ``ALTER TABLE``.
    """
    t = quote_table(catalog, mart_schema, logical_table)
    return f"ALTER TABLE {t} ADD COLUMN tariff_title varchar"


def add_client_type_column_sql(*, catalog: str, mart_schema: str, logical_table: str) -> str:
    """DDL ``ALTER TABLE … ADD COLUMN client_type varchar``.

    Колонка содержит код типа клиента (person / ip / org / unknown) для дашборда
    «Абонентская база» (разрез по типу клиента).

    Args:
        catalog: Каталог Trino.
        mart_schema: Схема MART.
        logical_table: Целевая таблица.

    Returns:
        Строка DDL.
    """
    t = quote_table(catalog, mart_schema, logical_table)
    return f"ALTER TABLE {t} ADD COLUMN client_type varchar"


def add_client_type_title_column_sql(*, catalog: str, mart_schema: str, logical_table: str) -> str:
    """DDL ``ALTER TABLE … ADD COLUMN client_type_title varchar``.

    Человекочитаемая подпись типа клиента: из ``dds_client_property_snapshot.title``
    (public.client_property) или запасной текст по коду (Физлицо / ИП / Юрлицо / unknown).

    Args:
        catalog: Каталог Trino.
        mart_schema: Схема MART.
        logical_table: Целевая таблица.

    Returns:
        Строка DDL.
    """
    t = quote_table(catalog, mart_schema, logical_table)
    return f"ALTER TABLE {t} ADD COLUMN client_type_title varchar"


def add_arpu_monthly_column_sql(*, catalog: str, mart_schema: str) -> str:
    """DDL ``ALTER TABLE kpi_arpu_daily ADD COLUMN arpu_monthly double``.

    ARPU за календарный месяц = ``arpu_daily`` × число дней в месяце ``report_date``.
    Для существующих Iceberg-таблиц ``kpi_arpu_daily`` без этой колонки.

    Args:
        catalog: Каталог Trino.
        mart_schema: Схема MART.

    Returns:
        Строка DDL.
    """
    t = quote_table(catalog, mart_schema, "kpi_arpu_daily")
    return f"ALTER TABLE {t} ADD COLUMN arpu_monthly double"



# kpi_revenue_daily / kpi_arpu_daily: приведённая дневная выручка — для ENABLED-заказов на report_date
# суммируем credits / число дней интервала проводки [date_start, date_end], если report_date попадает в интервал.
# Без date_start/date_end — интервал из period + period_type (месяц/год/день).
# kpi_receipts_daily: поступления (сумма credits без нормализации) по дате проводки ledger.period.
# arpu_daily = total_revenue / active_clients; arpu_monthly = arpu_daily × число дней в месяце report_date.
# kpi_revenue_active_*: сумма kpi_revenue_daily за календарный период; paying_owners — active_clients из ARPU на конец.


def _ledger_interval_start_sql(ledger_alias: str = "l") -> str:
    """Начало интервала проводки (inclusive)."""
    la = ledger_alias
    return f"""CAST(COALESCE(
        CAST({la}.date_start AS DATE),
        CASE
            WHEN {la}.period IS NOT NULL THEN
                CASE CAST({la}.period_type AS VARCHAR)
                    WHEN 'MONTHLY' THEN date_trunc('month', CAST({la}.period AS DATE))
                    WHEN 'YEARLY' THEN date_trunc('year', CAST({la}.period AS DATE))
                    ELSE CAST({la}.period AS DATE)
                END
        END
    ) AS DATE)"""


def _ledger_interval_end_sql(ledger_alias: str = "l") -> str:
    """Конец интервала проводки (inclusive)."""
    la = ledger_alias
    return f"""CAST(COALESCE(
        CAST({la}.date_end AS DATE),
        CAST({la}.date_start AS DATE),
        CASE
            WHEN {la}.period IS NOT NULL THEN
                CASE CAST({la}.period_type AS VARCHAR)
                    WHEN 'MONTHLY' THEN date_add(
                        'day', -1,
                        date_add('month', 1, date_trunc('month', CAST({la}.period AS DATE)))
                    )
                    WHEN 'YEARLY' THEN date_add(
                        'day', -1,
                        date_add('year', 1, date_trunc('year', CAST({la}.period AS DATE)))
                    )
                    ELSE CAST({la}.period AS DATE)
                END
        END
    ) AS DATE)"""


def _ledger_inclusive_days_sql(ledger_alias: str = "l") -> str:
    """Число календарных дней в интервале проводки [date_start, date_end] включительно.

    Минимум 1 день (GREATEST). Используется для нормализации выручки:
    ``credits / inclusive_days`` → дневная доля проводки.

    Args:
        ledger_alias: Алиас таблицы ``dds_ledgers`` в запросе.

    Returns:
        SQL-выражение ``GREATEST(date_diff('day', start, end) + 1, 1)``.
    """
    start = _ledger_interval_start_sql(ledger_alias)
    end = _ledger_interval_end_sql(ledger_alias)
    return f"GREATEST(date_diff('day', {start}, {end}) + 1, 1)"


def _ledger_daily_revenue_sql(ledger_alias: str = "l") -> str:
    """Дневная доля проводки: credits / число календарных дней интервала."""
    la = ledger_alias
    return f"(CAST({la}.credits AS DOUBLE) / CAST({_ledger_inclusive_days_sql(la)} AS DOUBLE))"


def _ledger_covers_report_date_predicate(ledger_alias: str, report_date_sql: str) -> str:
    """Предикат: ``report_date`` попадает в [date_start, date_end] проводки.

    С запасным расчётом интервала по ``period`` + ``period_type``,
    если ``date_start`` / ``date_end`` не заданы.

    Args:
        ledger_alias: Алиас таблицы ``dds_ledgers``.
        report_date_sql: SQL-выражение для даты отчёта (например ``c.process_date``).

    Returns:
        SQL-предикат для ``WHERE``.
    """
    start = _ledger_interval_start_sql(ledger_alias)
    end = _ledger_interval_end_sql(ledger_alias)
    d = f"CAST({report_date_sql} AS DATE)"
    return f"({start} IS NOT NULL AND {end} IS NOT NULL AND {d} >= {start} AND {d} <= {end})"


def _ledger_revenue_base_where_sql(ledger_alias: str = "l") -> str:
    """Базовый фильтр для проводок с выручкой: credits > 0 и задан интервал.

    Args:
        ledger_alias: Алиас таблицы ``dds_ledgers``.

    Returns:
        SQL-предикат для ``WHERE``.
    """
    la = ledger_alias
    return f"""{la}.credits IS NOT NULL
          AND {la}.credits > 0
          AND ({la}.date_start IS NOT NULL OR {la}.period IS NOT NULL)"""


# --- Календарь: границы периодов для revenue_active_* (см. docstring statements_for_vitrina).


def _calendar_month_end(d: date) -> date:
    """Последний календарный день месяца для даты ``d`` (28–31).

    Args:
        d: Произвольная дата.

    Returns:
        Дата последнего дня месяца.
    """
    return date(d.year, d.month, calendar.monthrange(d.year, d.month)[1])


def _iso_week_monday_sunday(d: date) -> tuple[date, date]:
    """Границы ISO-недели (понедельник–воскресенье) для даты ``d``.

    Используется в ``revenue_active_week``: ``period_end`` = воскресенье.

    Args:
        d: Произвольная дата.

    Returns:
        Кортеж ``(monday, sunday)``.
    """
    monday = d - timedelta(days=d.weekday())
    sunday = monday + timedelta(days=6)
    return monday, sunday


def _calendar_quarter_start(d: date) -> date:
    """Первый день календарного квартала для даты ``d``.

    Args:
        d: Произвольная дата.

    Returns:
        Дата начала квартала (1 января / 1 апреля / 1 июля / 1 октября).
    """
    q0 = (d.month - 1) // 3
    first_m = q0 * 3 + 1
    return date(d.year, first_m, 1)


def _calendar_quarter_end(d: date) -> date:
    """Последний календарный день квартала для даты ``d``.

    Args:
        d: Произвольная дата.

    Returns:
        Дата конца квартала.
    """
    q0 = (d.month - 1) // 3
    last_m = q0 * 3 + 3
    return date(d.year, last_m, calendar.monthrange(d.year, last_m)[1])


def _calendar_year_end(d: date) -> date:
    """31 декабря года даты ``d``.

    Args:
        d: Произвольная дата.

    Returns:
        Дата 31 декабря.
    """
    return date(d.year, 12, 31)


def _receipts_cte(dds_ledgers: str) -> str:
    """CTE ``rcpt``: поступления (сумма credits без нормализации) по дате проводки.

    В отличие от ``kpi_revenue_daily``, здесь credits не делятся на число дней интервала —
    это фактические поступления, а не приведённая дневная выручка.

    Args:
        dds_ledgers: Полное имя таблицы ``dds_ledgers`` (catalog.schema.table).

    Returns:
        SQL-фрагмент CTE ``rcpt AS (SELECT …)``.
    """
    return f"""rcpt AS (
        SELECT CAST(l.period AS DATE) AS revenue_day,
               l.order_id,
               CAST(SUM(CAST(l.credits AS DOUBLE)) AS DOUBLE) AS receipt_amount
        FROM {dds_ledgers} l
        WHERE l.period IS NOT NULL
          AND l.credits IS NOT NULL
          AND l.credits > 0
        GROUP BY 1, 2
    )"""


# Заказы + договор: owner_id в HALK — agreements.id; метрики по абоненту — agreements.client_id.


def _client_property_dim_cte(dds_client_property_snapshot: str) -> str:
    """Справочник client_property из full-snapshot DDS (колонки id/title в JSON payload, см. dim_tariff)."""
    return f"""client_property_dim AS (
        SELECT
            CAST(COALESCE(
                TRY_CAST(JSON_EXTRACT_SCALAR(j, '$.id') AS BIGINT),
                TRY_CAST(NULLIF(TRIM(CAST(source_pkey AS VARCHAR)), '') AS BIGINT)
            ) AS BIGINT) AS property_id,
            CAST(COALESCE(
                NULLIF(TRIM(COALESCE(JSON_EXTRACT_SCALAR(j, '$.title'), '')), ''),
                ''
            ) AS VARCHAR) AS title
        FROM (
            SELECT
                source_pkey,
                TRY(JSON_PARSE(COALESCE(NULLIF(TRIM(CAST(payload AS VARCHAR)), ''), '{{}}'))) AS j
            FROM {dds_client_property_snapshot}
        ) parsed
        WHERE CAST(COALESCE(
            TRY_CAST(JSON_EXTRACT_SCALAR(j, '$.id') AS BIGINT),
            TRY_CAST(NULLIF(TRIM(CAST(source_pkey AS VARCHAR)), '') AS BIGINT)
        ) AS BIGINT) IS NOT NULL
    )"""


def _sub_ord_cte(dds_orders: str, dds_agreements: str, dds_clients: str) -> str:
    """HALK: orders.owner_id → agreements.id; subscriber_id = agreements.client_id; тип и подпись — clients + client_property_dim."""
    return f"""sub_ord AS (
        SELECT
            o.*,
            CAST(a.client_id AS BIGINT) AS subscriber_id,
            COALESCE(LOWER(CAST(cl.type AS VARCHAR)), 'unknown') AS client_type,
            COALESCE(
                NULLIF(TRIM(CAST(cp.title AS VARCHAR)), ''),
                CASE LOWER(CAST(cl.type AS VARCHAR))
                    WHEN 'person' THEN 'Физлицо'
                    WHEN 'ip' THEN 'ИП'
                    WHEN 'org' THEN 'Юрлицо'
                    ELSE COALESCE(LOWER(CAST(cl.type AS VARCHAR)), 'unknown')
                END
            ) AS client_type_title
        FROM {dds_orders} o
        INNER JOIN {dds_agreements} a
            ON CAST(a.id AS BIGINT) = o.owner_id
        LEFT JOIN {dds_clients} cl
            ON CAST(cl.id AS BIGINT) = CAST(a.client_id AS BIGINT)
        LEFT JOIN client_property_dim cp
            ON cp.property_id = CAST(cl.property_id AS BIGINT)
        WHERE COALESCE(o.expenditure, false) = false
          AND COALESCE(o.is_draft, false) = false
    )"""


def _sub_ord_stack_cte(
    dds_orders: str,
    dds_agreements: str,
    dds_clients: str,
    dds_client_property_snapshot: str,
) -> str:
    """CTE client_property_dim + sub_ord (порядок важен для Trino WITH)."""
    return f"{_client_property_dim_cte(dds_client_property_snapshot)},\n{_sub_ord_cte(dds_orders, dds_agreements, dds_clients)}"


def _subscription_active_on_date(snapshot_date_sql: str) -> str:
    """Активный продуктовый заказ на дату среза (совпадает с логикой АБ0)."""
    return f"""(
        o.status = 'ENABLED'
          AND o.activated IS NOT NULL
          AND (o.expire_time IS NULL OR CAST(o.expire_time AS DATE) >= CAST({snapshot_date_sql} AS DATE))
    )"""


def _arpu_days_in_month_sql(anchor_date_sql: str) -> str:
    """Число календарных дней в месяце даты ``anchor`` (28–31).

    Используется в расчёте скользящего окна ARPU и ``arpu_monthly``.

    Args:
        anchor_date_sql: SQL-выражение для якорной даты.

    Returns:
        SQL-выражение ``day(last_day_of_month(anchor))``.
    """
    anchor = f"CAST({anchor_date_sql} AS DATE)"
    month_last_day = (
        f"date_add('day', -1, date_add('month', 1, date_trunc('month', {anchor})))"
    )
    return f"day({month_last_day})"


def _arpu_rolling_window_start_sql(anchor_date_sql: str) -> str:
    """Начало скользящего окна ARPU: ``anchor − (N_days_in_month − 1)`` календарных дней.

    На каждый день окно захватывает ровно N календарных дней назад (включая anchor),
    где N = число дней в месяце anchor. Это даёт сравнимое окно для любого дня месяца.

    Args:
        anchor_date_sql: SQL-выражение для якорной даты.

    Returns:
        SQL-выражение начала окна.
    """
    anchor = f"CAST({anchor_date_sql} AS DATE)"
    dim = _arpu_days_in_month_sql(anchor_date_sql)
    return f"date_add('day', -({dim} - 1), {anchor})"


def _arpu_window_start_sql(anchor_date_sql: str) -> str:
    """Начало окна выручки для ARPU.

    **Правило**:
    - Если ``anchor`` — последний день месяца → окно = календарный месяц (с 1-го числа).
    - Иначе → скользящие N дней, где N = число дней в месяце anchor.

    Это обеспечивает: на конец месяца видим полный месяц; в середине — скользящую оценку
    за сравнимый промежуток.

    Args:
        anchor_date_sql: SQL-выражение для якорной даты.

    Returns:
        SQL-выражение ``CASE WHEN … THEN month_start ELSE rolling_start END``.
    """
    anchor = f"CAST({anchor_date_sql} AS DATE)"
    month_start = f"CAST(date_trunc('month', {anchor}) AS DATE)"
    month_last_day = (
        f"date_add('day', -1, date_add('month', 1, date_trunc('month', {anchor})))"
    )
    rolling_start = _arpu_rolling_window_start_sql(anchor_date_sql)
    return f"""CASE
        WHEN {anchor} = {month_last_day} THEN {month_start}
        ELSE {rolling_start}
    END"""


def _subscriber_service_calendar_window_predicate(*, anchor_date_sql: str, window_days_inclusive: int) -> str:
    """Подписка пересекается с [anchor - (N-1) дней, anchor] включительно; без SCD2 по статусу — см. документацию."""
    offset = window_days_inclusive - 1
    return f"""(
        o.activated IS NOT NULL
          AND CAST(o.activated AS DATE) <= CAST({anchor_date_sql} AS DATE)
          AND (o.expire_time IS NULL OR CAST(o.expire_time AS DATE) >= date_add('day', -{offset}, CAST({anchor_date_sql} AS DATE)))
          AND o.status IN ('ENABLED', 'EXPIRED')
    )"""


# Имена витрин в CLI и в mart_jobs (см. normalize_vitrina_key для синонимов).
VITRINA_KEYS = frozenset(
    {
        "ab0",
        "ab30",
        "ab90",
        "revenue",
        "receipts",
        "arpu",
        "inflow",
        "outflow",
        "revenue_active_month",
        "revenue_active_week",
        "revenue_active_quarter",
        "revenue_active_year",
        "dim_tariff",
        "dim_segment",
    }
)


def mart_schema_from_config(config: dict[str, Any]) -> str:
    """Имя схемы MART из конфигурации.

    Args:
        config: Словарь raw_load_config.json.

    Returns:
        Имя схемы (по умолчанию ``"mart"``).
    """
    return str(config.get("pipeline", {}).get("mart_schema", "mart"))


def dds_schema_from_config(config: dict[str, Any]) -> str:
    """Имя схемы DDS из конфигурации.

    Сначала проверяет ``pipeline.dds_schema``, затем ``dds.schema``.

    Args:
        config: Словарь raw_load_config.json.

    Returns:
        Имя схемы (по умолчанию ``"dds"``).
    """
    p = config.get("pipeline", {})
    if isinstance(p.get("dds_schema"), str):
        return str(p["dds_schema"])
    return str(config.get("dds", {}).get("schema", "dds"))


def normalize_vitrina_key(arg: str) -> str:
    """Приводит строковый идентификатор витрины к каноническому ключу.

    Поддерживает синонимы: ``kpi_ab0_daily`` → ``ab0``, ``revenue`` → ``revenue``,
    ``поступления`` → ``receipts``, ``kpi_segment`` → ``dim_segment`` и т.д.

    Args:
        arg: Строковый идентификатор (из CLI или конфигурации).

    Returns:
        Канонический ключ витрины (один из ``VITRINA_KEYS``).

    Raises:
        ValueError: Если ключ не удалось сопоставить ни с одной витриной.
    """
    key = arg.strip().lower().replace("-", "_")
    aliases = {
        "kpi_ab0_daily": "ab0",
        "kpi_ab0": "ab0",
        "ab0": "ab0",
        "kpi_ab30_daily": "ab30",
        "kpi_ab30": "ab30",
        "ab30": "ab30",
        "kpi_ab90_daily": "ab90",
        "kpi_ab90": "ab90",
        "ab90": "ab90",
        "kpi_revenue_daily": "revenue",
        "revenue": "revenue",
        "kpi_receipts_daily": "receipts",
        "receipts": "receipts",
        "поступления": "receipts",
        "kpi_arpu_daily": "arpu",
        "arpu": "arpu",
        "kpi_inflow_daily": "inflow",
        "inflow": "inflow",
        "kpi_outflow_daily": "outflow",
        "outflow": "outflow",
        "revenue_active_month": "revenue_active_month",
        "kpi_revenue_active_month": "revenue_active_month",
        "revenue_active_week": "revenue_active_week",
        "kpi_revenue_active_week": "revenue_active_week",
        "revenue_active_quarter": "revenue_active_quarter",
        "kpi_revenue_active_quarter": "revenue_active_quarter",
        "revenue_active_year": "revenue_active_year",
        "kpi_revenue_active_year": "revenue_active_year",
        "dim_tariff": "dim_tariff",
        "dim_segment": "dim_segment",
        "kpi_segment": "dim_segment",
    }
    resolved = aliases.get(key, key)
    if resolved not in VITRINA_KEYS:
        raise ValueError(f"Неизвестная витрина '{arg}'. Допустимо: {sorted(VITRINA_KEYS)}")
    return resolved


# Целевая Iceberg-таблица в mart для vitrina_key (одна задача Airflow на витрину).
MART_TARGET_TABLE_BY_VITRINA_KEY: dict[str, str] = {
    "ab0": "kpi_ab0_daily",
    "ab30": "kpi_ab30_daily",
    "ab90": "kpi_ab90_daily",
    "revenue": "kpi_revenue_daily",
    "receipts": "kpi_receipts_daily",
    "arpu": "kpi_arpu_daily",
    "inflow": "kpi_inflow_daily",
    "outflow": "kpi_outflow_daily",
    "revenue_active_month": "kpi_revenue_active_month",
    "revenue_active_week": "kpi_revenue_active_week",
    "revenue_active_quarter": "kpi_revenue_active_quarter",
    "revenue_active_year": "kpi_revenue_active_year",
    "dim_tariff": "dim_tariff",
    "dim_segment": "dim_segment",
}

# Витрины, читающие ``kpi_arpu_daily`` в Trino (нужна колонка ``active_clients``).
VITRINAS_READING_KPI_ARPU_DAILY: frozenset[str] = frozenset(
    (
        "arpu",
        "revenue_active_month",
        "revenue_active_week",
        "revenue_active_quarter",
        "revenue_active_year",
    )
)


def kpi_arpu_paying_owners_rename_migration_sql(
    catalog: str, mart_schema: str, *, existing_columns: set[str]
) -> str | None:
    """Если Iceberg-таблица создана старой версией (``paying_owners``) — ``RENAME`` в ``active_clients``."""
    cols = {c.lower() for c in existing_columns}
    if "paying_owners" not in cols:
        return None
    if "active_clients" in cols:
        return None
    t = quote_table(catalog, mart_schema, "kpi_arpu_daily")
    return f"ALTER TABLE {t} RENAME COLUMN paying_owners TO active_clients"


def kpi_inflow_legacy_connected_owners_migration_sqls(
    catalog: str, mart_schema: str, *, existing_columns: set[str]
) -> list[str]:
    """Схема до 2 колонок метрик (``new_orders`` + ``new_connected_owners``) → ``new_*`` как сейчас."""
    cols_lower = {c.lower() for c in existing_columns}
    if "new_connected_owners" not in cols_lower:
        return []
    t = quote_table(catalog, mart_schema, "kpi_inflow_daily")
    stmts: list[str] = []
    if "new_clients" not in cols_lower:
        stmts.append(f"ALTER TABLE {t} ADD COLUMN new_clients bigint")
    if "new_agreements" not in cols_lower:
        stmts.append(f"ALTER TABLE {t} ADD COLUMN new_agreements bigint")
    stmts.append(f"ALTER TABLE {t} DROP COLUMN new_connected_owners")
    return stmts


def kpi_outflow_legacy_terminal_snapshots_migration_sqls(
    catalog: str, mart_schema: str, *, existing_columns: set[str]
) -> list[str]:
    """Схема с ``terminal_*_snapshot`` → ``churned_clients`` / ``completed_*`` как сейчас."""
    cols_lower = {c.lower() for c in existing_columns}
    has_terminal = "terminal_orders_snapshot" in cols_lower or "terminal_owners_snapshot" in cols_lower
    if not has_terminal:
        return []
    t = quote_table(catalog, mart_schema, "kpi_outflow_daily")
    stmts: list[str] = []
    if "churned_clients" not in cols_lower:
        stmts.append(f"ALTER TABLE {t} ADD COLUMN churned_clients bigint")
    if "completed_agreements" not in cols_lower:
        stmts.append(f"ALTER TABLE {t} ADD COLUMN completed_agreements bigint")
    if "completed_orders" not in cols_lower:
        stmts.append(f"ALTER TABLE {t} ADD COLUMN completed_orders bigint")
    if "terminal_orders_snapshot" in cols_lower:
        stmts.append(f"ALTER TABLE {t} DROP COLUMN terminal_orders_snapshot")
    if "terminal_owners_snapshot" in cols_lower:
        stmts.append(f"ALTER TABLE {t} DROP COLUMN terminal_owners_snapshot")
    return stmts


_KPI_TABLE_BUILD_ORDER: tuple[str, ...] = tuple(MART_TARGET_TABLE_BY_VITRINA_KEY.values())


def _kpi_target_create_table_sql(catalog: str, mart_schema: str, logical_table_name: str) -> str:
    """DDL ``CREATE TABLE IF NOT EXISTS`` для одной KPI-витрины.

    Генерирует полную схему Iceberg-таблицы с партиционированием. Для ``dim_*`` таблиц
    — без партиций; для ``kpi_*`` — партиционирование по ``day(report_date)`` или
    ``day(period_end)``. Резервное копирование: ``paying_owners`` для исторической
    совместимости (мигрируется в ``active_clients`` через ``_maybe_migrate_kpi_arpu``).

    Args:
        catalog: Каталог Trino.
        mart_schema: Схема MART.
        logical_table_name: Логическое имя (например ``kpi_ab0_daily``).

    Returns:
        Строка ``CREATE TABLE IF NOT EXISTS``.

    Raises:
        ValueError: Если ``logical_table_name`` не соответствует ни одной KPI-таблице.
    """
    qt = lambda base: quote_table(catalog, mart_schema, base)
    if logical_table_name == "kpi_ab0_daily":
        return f"""
        CREATE TABLE IF NOT EXISTS {qt("kpi_ab0_daily")} (
            report_date DATE NOT NULL,
            segment_id BIGINT,
            service_kind VARCHAR,
            tariff_id BIGINT,
            tariff_title VARCHAR,
            client_type VARCHAR,
            client_type_title VARCHAR,
            active_subscribers BIGINT NOT NULL,
            refreshed_at TIMESTAMP(6) WITH TIME ZONE NOT NULL
        )
        WITH (partitioning = ARRAY['day(report_date)'])
        """
    if logical_table_name == "kpi_ab30_daily":
        return f"""
        CREATE TABLE IF NOT EXISTS {qt("kpi_ab30_daily")} (
            report_date DATE NOT NULL,
            segment_id BIGINT,
            service_kind VARCHAR,
            tariff_id BIGINT,
            tariff_title VARCHAR,
            client_type VARCHAR,
            client_type_title VARCHAR,
            active_subscribers BIGINT NOT NULL,
            refreshed_at TIMESTAMP(6) WITH TIME ZONE NOT NULL
        )
        WITH (partitioning = ARRAY['day(report_date)'])
        """
    if logical_table_name == "kpi_ab90_daily":
        return f"""
        CREATE TABLE IF NOT EXISTS {qt("kpi_ab90_daily")} (
            report_date DATE NOT NULL,
            segment_id BIGINT,
            service_kind VARCHAR,
            tariff_id BIGINT,
            tariff_title VARCHAR,
            client_type VARCHAR,
            client_type_title VARCHAR,
            active_subscribers BIGINT NOT NULL,
            refreshed_at TIMESTAMP(6) WITH TIME ZONE NOT NULL
        )
        WITH (partitioning = ARRAY['day(report_date)'])
        """
    if logical_table_name == "kpi_revenue_daily":
        return f"""
        CREATE TABLE IF NOT EXISTS {qt("kpi_revenue_daily")} (
            report_date DATE NOT NULL,
            segment_id BIGINT,
            service_kind VARCHAR,
            tariff_id BIGINT,
            tariff_title VARCHAR,
            client_type VARCHAR,
            client_type_title VARCHAR,
            total_revenue DOUBLE NOT NULL,
            paying_owners BIGINT NOT NULL,
            refreshed_at TIMESTAMP(6) WITH TIME ZONE NOT NULL
        )
        WITH (partitioning = ARRAY['day(report_date)'])
        """
    if logical_table_name == "kpi_receipts_daily":
        return f"""
        CREATE TABLE IF NOT EXISTS {qt("kpi_receipts_daily")} (
            report_date DATE NOT NULL,
            segment_id BIGINT,
            service_kind VARCHAR,
            tariff_id BIGINT,
            tariff_title VARCHAR,
            client_type VARCHAR,
            client_type_title VARCHAR,
            total_receipts DOUBLE NOT NULL,
            paying_owners BIGINT NOT NULL,
            refreshed_at TIMESTAMP(6) WITH TIME ZONE NOT NULL
        )
        WITH (partitioning = ARRAY['day(report_date)'])
        """
    if logical_table_name == "kpi_arpu_daily":
        return f"""
        CREATE TABLE IF NOT EXISTS {qt("kpi_arpu_daily")} (
            report_date DATE NOT NULL,
            month_start DATE NOT NULL,
            segment_id BIGINT,
            service_kind VARCHAR,
            tariff_id BIGINT,
            tariff_title VARCHAR,
            client_type VARCHAR,
            client_type_title VARCHAR,
            total_revenue DOUBLE NOT NULL,
            active_clients BIGINT NOT NULL,
            arpu_daily DOUBLE,
            arpu_monthly DOUBLE,
            refreshed_at TIMESTAMP(6) WITH TIME ZONE NOT NULL
        )
        WITH (partitioning = ARRAY['day(report_date)'])
        """
    if logical_table_name == "kpi_inflow_daily":
        return f"""
        CREATE TABLE IF NOT EXISTS {qt("kpi_inflow_daily")} (
            report_date DATE NOT NULL,
            segment_id BIGINT,
            service_kind VARCHAR,
            tariff_id BIGINT,
            tariff_title VARCHAR,
            client_type VARCHAR,
            client_type_title VARCHAR,
            new_clients BIGINT NOT NULL,
            new_agreements BIGINT NOT NULL,
            new_orders BIGINT NOT NULL,
            refreshed_at TIMESTAMP(6) WITH TIME ZONE NOT NULL
        )
        WITH (partitioning = ARRAY['day(report_date)'])
        """
    if logical_table_name == "kpi_outflow_daily":
        return f"""
        CREATE TABLE IF NOT EXISTS {qt("kpi_outflow_daily")} (
            report_date DATE NOT NULL,
            segment_id BIGINT,
            service_kind VARCHAR,
            tariff_id BIGINT,
            tariff_title VARCHAR,
            client_type VARCHAR,
            client_type_title VARCHAR,
            churned_clients BIGINT NOT NULL,
            completed_agreements BIGINT NOT NULL,
            completed_orders BIGINT NOT NULL,
            refreshed_at TIMESTAMP(6) WITH TIME ZONE NOT NULL
        )
        WITH (partitioning = ARRAY['day(report_date)'])
        """
    if logical_table_name == "kpi_revenue_active_month":
        return f"""
        CREATE TABLE IF NOT EXISTS {qt("kpi_revenue_active_month")} (
            period_end DATE NOT NULL,
            segment_id BIGINT,
            service_kind VARCHAR,
            tariff_id BIGINT,
            tariff_title VARCHAR,
            client_type VARCHAR,
            client_type_title VARCHAR,
            total_revenue DOUBLE NOT NULL,
            paying_owners BIGINT NOT NULL,
            refreshed_at TIMESTAMP(6) WITH TIME ZONE NOT NULL
        )
        WITH (partitioning = ARRAY['day(period_end)'])
        """
    if logical_table_name == "kpi_revenue_active_week":
        return f"""
        CREATE TABLE IF NOT EXISTS {qt("kpi_revenue_active_week")} (
            period_end DATE NOT NULL,
            segment_id BIGINT,
            service_kind VARCHAR,
            tariff_id BIGINT,
            tariff_title VARCHAR,
            client_type VARCHAR,
            client_type_title VARCHAR,
            total_revenue DOUBLE NOT NULL,
            paying_owners BIGINT NOT NULL,
            refreshed_at TIMESTAMP(6) WITH TIME ZONE NOT NULL
        )
        WITH (partitioning = ARRAY['day(period_end)'])
        """
    if logical_table_name == "kpi_revenue_active_quarter":
        return f"""
        CREATE TABLE IF NOT EXISTS {qt("kpi_revenue_active_quarter")} (
            period_end DATE NOT NULL,
            segment_id BIGINT,
            service_kind VARCHAR,
            tariff_id BIGINT,
            tariff_title VARCHAR,
            client_type VARCHAR,
            client_type_title VARCHAR,
            total_revenue DOUBLE NOT NULL,
            paying_owners BIGINT NOT NULL,
            refreshed_at TIMESTAMP(6) WITH TIME ZONE NOT NULL
        )
        WITH (partitioning = ARRAY['day(period_end)'])
        """
    if logical_table_name == "kpi_revenue_active_year":
        return f"""
        CREATE TABLE IF NOT EXISTS {qt("kpi_revenue_active_year")} (
            period_end DATE NOT NULL,
            segment_id BIGINT,
            service_kind VARCHAR,
            tariff_id BIGINT,
            tariff_title VARCHAR,
            client_type VARCHAR,
            client_type_title VARCHAR,
            total_revenue DOUBLE NOT NULL,
            paying_owners BIGINT NOT NULL,
            refreshed_at TIMESTAMP(6) WITH TIME ZONE NOT NULL
        )
        WITH (partitioning = ARRAY['day(period_end)'])
        """
    if logical_table_name == "dim_tariff":
        return f"""
        CREATE TABLE IF NOT EXISTS {qt("dim_tariff")} (
            tariff_id BIGINT NOT NULL,
            tariff_title VARCHAR NOT NULL,
            refreshed_at TIMESTAMP(6) WITH TIME ZONE NOT NULL
        )
        """
    if logical_table_name == "dim_segment":
        return f"""
        CREATE TABLE IF NOT EXISTS {qt("dim_segment")} (
            segment_id BIGINT NOT NULL,
            parent_id BIGINT,
            segment_title VARCHAR NOT NULL,
            refreshed_at TIMESTAMP(6) WITH TIME ZONE NOT NULL
        )
        """
    raise ValueError(f"Неизвестная KPI-таблица для DDL: {logical_table_name}")


def ensure_vitrina_target_ddl_sql(catalog: str, mart_schema: str, vitrina_key: str) -> list[str]:
    """CREATE SCHEMA IF NOT EXISTS + CREATE TABLE для одной витрины (обычный запуск без --ensure-all-ddl)."""
    logical = MART_TARGET_TABLE_BY_VITRINA_KEY[vitrina_key]
    sch = quote_schema(catalog, mart_schema)
    return [
        f"CREATE SCHEMA IF NOT EXISTS {sch}",
        _kpi_target_create_table_sql(catalog, mart_schema, logical),
    ]


def ensure_tables_sql(catalog: str, mart_schema: str) -> list[str]:
    """CREATE SCHEMA + CREATE TABLE IF NOT EXISTS для всех KPI (флаг --ensure-all-ddl)."""
    sch = quote_schema(catalog, mart_schema)
    return [
        f"CREATE SCHEMA IF NOT EXISTS {sch}",
        *[
            _kpi_target_create_table_sql(catalog, mart_schema, logical_name)
            for logical_name in _KPI_TABLE_BUILD_ORDER
        ],
    ]


def dds_segments_mirror_table_name(config: dict[str, Any]) -> str:
    """Имя зеркала DDS для ``public.segments``.

    ``mode`` full → ``dds_segments_snapshot``, иначе → ``dds_segments``.

    Args:
        config: Словарь raw_load_config.json.

    Returns:
        Имя таблицы в схеме DDS.
    """
    for row in config.get("tables", []):
        if str(row.get("name", "")).strip().lower() != "public.segments":
            continue
        mode = str(row.get("mode", "incremental")).strip().lower()
        return "dds_segments_snapshot" if mode == "full" else "dds_segments"
    return "dds_segments_snapshot"


def dds_tariffs_mirror_table_name(config: dict[str, Any]) -> str:
    """Имя зеркала DDS для ``public.tariffs``.

    ``dds_table_runner.load_one_table``: ``mode`` full → ``dds_tariffs_snapshot``,
    иначе → ``dds_tariffs``.

    Args:
        config: Словарь raw_load_config.json.

    Returns:
        Имя таблицы в схеме DDS.
    """
    for row in config.get("tables", []):
        if str(row.get("name", "")).strip().lower() != "public.tariffs":
            continue
        mode = str(row.get("mode", "incremental")).strip().lower()
        return "dds_tariffs_snapshot" if mode == "full" else "dds_tariffs"
    return "dds_tariffs_snapshot"


def _dim_tariff_insert_sql(*, catalog: str, mart_schema: str, dds_schema: str, config: dict[str, Any]) -> str:
    """Плоский справочник id/title из payload DDS (снимок/state RAW)."""
    dds_t = quote_table(catalog, dds_schema, dds_tariffs_mirror_table_name(config))
    mart_t = quote_table(catalog, mart_schema, "dim_tariff")
    return f"""
    INSERT INTO {mart_t} (tariff_id, tariff_title, refreshed_at)
    WITH parsed AS (
        SELECT
            source_pkey,
            TRY(JSON_PARSE(COALESCE(NULLIF(TRIM(CAST(payload AS VARCHAR)), ''), '{{}}'))) AS j
        FROM {dds_t}
    )
    SELECT DISTINCT
        CAST(COALESCE(
            TRY_CAST(JSON_EXTRACT_SCALAR(j, '$.id') AS BIGINT),
            TRY_CAST(NULLIF(TRIM(CAST(source_pkey AS VARCHAR)), '') AS BIGINT)
        ) AS BIGINT),
        CAST(COALESCE(
            NULLIF(TRIM(COALESCE(JSON_EXTRACT_SCALAR(j, '$.title'), JSON_EXTRACT_SCALAR(j, '$.name'), '')), ''),
            ''
        ) AS VARCHAR),
        current_timestamp
    FROM parsed
    WHERE CAST(COALESCE(
        TRY_CAST(JSON_EXTRACT_SCALAR(j, '$.id') AS BIGINT),
        TRY_CAST(NULLIF(TRIM(CAST(source_pkey AS VARCHAR)), '') AS BIGINT)
    ) AS BIGINT) IS NOT NULL
    """.strip()


def _dim_segment_insert_sql(*, catalog: str, mart_schema: str, dds_schema: str, config: dict[str, Any]) -> str:
    """Справочник сегментов: segment_title = цепочка title от корня (parent_id) до узла."""
    dds_t = quote_table(catalog, dds_schema, dds_segments_mirror_table_name(config))
    mart_t = quote_table(catalog, mart_schema, "dim_segment")
    # Trino: только ``WITH RECURSIVE <name> AS (...)`` — без отдельного ``parsed`` перед RECURSIVE.
    parsed_sub = f"""
        SELECT
            CAST(COALESCE(
                TRY_CAST(JSON_EXTRACT_SCALAR(j, '$.id') AS BIGINT),
                TRY_CAST(NULLIF(TRIM(CAST(source_pkey AS VARCHAR)), '') AS BIGINT)
            ) AS BIGINT) AS segment_id,
            TRY_CAST(JSON_EXTRACT_SCALAR(j, '$.parent_id') AS BIGINT) AS parent_id,
            CAST(COALESCE(
                NULLIF(TRIM(COALESCE(JSON_EXTRACT_SCALAR(j, '$.title'), '')), ''),
                ''
            ) AS VARCHAR) AS title
        FROM (
            SELECT
                source_pkey,
                TRY(JSON_PARSE(COALESCE(NULLIF(TRIM(CAST(payload AS VARCHAR)), ''), '{{}}'))) AS j
            FROM {dds_t}
        ) raw
        WHERE CAST(COALESCE(
            TRY_CAST(JSON_EXTRACT_SCALAR(j, '$.id') AS BIGINT),
            TRY_CAST(NULLIF(TRIM(CAST(source_pkey AS VARCHAR)), '') AS BIGINT)
        ) AS BIGINT) IS NOT NULL
    """
    return f"""
    INSERT INTO {mart_t} (segment_id, parent_id, segment_title, refreshed_at)
    WITH RECURSIVE tree(segment_id, parent_id, title, segment_title) AS (
        SELECT
            p.segment_id,
            p.parent_id,
            p.title,
            p.title AS segment_title
        FROM ({parsed_sub}) p
        WHERE p.parent_id IS NULL
           OR NOT EXISTS (
                SELECT 1 FROM ({parsed_sub}) parent WHERE parent.segment_id = p.parent_id
           )
        UNION ALL
        SELECT
            c.segment_id,
            c.parent_id,
            c.title,
            CAST(concat(t.segment_title, ' / ', c.title) AS VARCHAR) AS segment_title
        FROM ({parsed_sub}) c
        INNER JOIN tree t ON c.parent_id = t.segment_id
    )
    SELECT DISTINCT
        segment_id,
        parent_id,
        segment_title,
        current_timestamp
    FROM tree
    """.strip()


def statements_for_vitrina(
    *,
    catalog: str,
    mart_schema: str,
    dds_schema: str,
    vitrina_key: str,
    report_day: date,
    config: dict[str, Any],
) -> tuple[str, list[str]]:
    """Возвращает (логическое_имя_таблицы, [DELETE за день, INSERT]).

    Для revenue_active_* строка с ключом ``period_end`` = **конец календарного периода** (конец месяца /
    воскресенье недели / конец квартала / 31.12); метрики — **на последний день отчёта в периоде**
    ``LEAST(report_anchor, period_end)`` (незавершённый период — частичная сумма дневной выручки).

    kpi_arpu_daily — окно выручки (N = дней в месяце; на конец месяца — календарный месяц) / AB0 на дату;
    kpi_revenue_active_* — сумма ``kpi_revenue_daily`` за период, ``paying_owners`` из ARPU на конец.

    Уникальный абонент в COUNT(DISTINCT …): agreements.client_id; связка orders.owner_id = agreements.id.
    """
    rd = sql_date(report_day)

    if vitrina_key == "dim_tariff":
        mart_t = quote_table(catalog, mart_schema, "dim_tariff")
        ins = _dim_tariff_insert_sql(
            catalog=catalog,
            mart_schema=mart_schema,
            dds_schema=dds_schema,
            config=config,
        )
        return ("dim_tariff", [f"DELETE FROM {mart_t} WHERE 1 = 1", ins])

    if vitrina_key == "dim_segment":
        mart_t = quote_table(catalog, mart_schema, "dim_segment")
        ins = _dim_segment_insert_sql(
            catalog=catalog,
            mart_schema=mart_schema,
            dds_schema=dds_schema,
            config=config,
        )
        return ("dim_segment", [f"DELETE FROM {mart_t} WHERE 1 = 1", ins])

    # Общие CTE почти для всех витрин (дата среза, заказы с subscriber_id, нормализованная выручка).
    dds_orders = quote_table(catalog, dds_schema, "dds_orders")
    dds_agreements = quote_table(catalog, dds_schema, "dds_agreements")
    dds_clients = quote_table(catalog, dds_schema, "dds_clients")
    dds_prop = quote_table(catalog, dds_schema, DDS_CLIENT_PROPERTY_SNAPSHOT_TABLE)
    dds_ledgers = quote_table(catalog, dds_schema, "dds_ledgers")

    cfg_cte = f"cfg AS (SELECT {rd} AS process_date)"
    sub_ord_cte = _sub_ord_stack_cte(dds_orders, dds_agreements, dds_clients, dds_prop)
    revenue_active_daily = _subscription_active_on_date("CAST(c.process_date AS DATE)")
    ledger_daily = _ledger_daily_revenue_sql("l")
    ledger_covers_day = _ledger_covers_report_date_predicate("l", "b.process_date")
    ledger_where = _ledger_revenue_base_where_sql("l")
    mart_arpu_t = quote_table(catalog, mart_schema, "kpi_arpu_daily")
    mart_revenue_daily_t = quote_table(catalog, mart_schema, "kpi_revenue_daily")
    mart_dim_tariff_t = quote_table(catalog, mart_schema, "dim_tariff")

    if vitrina_key == "ab0":
        # АБ0 + АБ30 + АБ90 — все три витрины за один проход по sub_ord.
        # sub_ord CTE строится один раз, затем три INSERT'а с разными предикатами.
        # Это даёт ~3x ускорение по сравнению с тремя отдельными вызовами.
        mart_t0 = quote_table(catalog, mart_schema, "kpi_ab0_daily")
        mart_t30 = quote_table(catalog, mart_schema, "kpi_ab30_daily")
        mart_t90 = quote_table(catalog, mart_schema, "kpi_ab90_daily")
        active_day_slice = _subscription_active_on_date("CAST(c.process_date AS DATE)")
        window_30 = _subscriber_service_calendar_window_predicate(
            anchor_date_sql="c.process_date", window_days_inclusive=30
        )
        window_90 = _subscriber_service_calendar_window_predicate(
            anchor_date_sql="c.process_date", window_days_inclusive=90
        )
        # Общий шаблон INSERT для всех трёх витрин
        _ab_insert = lambda mart_t, pred: f"""
        INSERT INTO {mart_t} (
            report_date, segment_id, service_kind, tariff_id, tariff_title,
            client_type, client_type_title, active_subscribers, refreshed_at
        )
        WITH {cfg_cte},
             {sub_ord_cte}
        SELECT c.process_date,
               o.segment_id,
               o.base_type AS service_kind,
               o.tariff_id,
               COALESCE(MAX(CAST(dt.tariff_title AS VARCHAR)), CAST('' AS VARCHAR)),
               o.client_type,
               o.client_type_title,
               COUNT(DISTINCT o.subscriber_id),
               current_timestamp
        FROM cfg c
        INNER JOIN sub_ord o ON true
        LEFT JOIN {mart_dim_tariff_t} dt ON dt.tariff_id IS NOT DISTINCT FROM o.tariff_id
        WHERE {pred}
        GROUP BY c.process_date, o.segment_id, o.base_type, o.tariff_id, o.client_type, o.client_type_title
        """
        return ("kpi_ab0_daily", [
            f"DELETE FROM {mart_t0}  WHERE report_date = {rd}",
            f"DELETE FROM {mart_t30} WHERE report_date = {rd}",
            f"DELETE FROM {mart_t90} WHERE report_date = {rd}",
            _ab_insert(mart_t0, active_day_slice),
            _ab_insert(mart_t30, window_30),
            _ab_insert(mart_t90, window_90),
        ])

    if vitrina_key in ("ab30", "ab90"):
        # AB30/AB90 считаются вместе с AB0 в едином проходе — здесь пустой no-op.
        # Оставлено для совместимости с DAG (задачи gen_kpi_ab30/ab90 не падают).
        mart_t = quote_table(catalog, mart_schema, MART_TARGET_TABLE_BY_VITRINA_KEY[vitrina_key])
        return (MART_TARGET_TABLE_BY_VITRINA_KEY[vitrina_key], [f"SELECT 1 WHERE false"])

    if vitrina_key == "revenue":
        mart_t = quote_table(catalog, mart_schema, "kpi_revenue_daily")
        active_day_slice = _subscription_active_on_date("CAST(b.process_date AS DATE)")
        insert = f"""
        INSERT INTO {mart_t} (
            report_date, segment_id, service_kind, tariff_id, tariff_title,
            client_type, client_type_title,
            total_revenue, paying_owners, refreshed_at
        )
        WITH
            {cfg_cte},
            -- Привязка даты к началу месяца (для JOIN с окнами)
            mb AS (
                SELECT c.process_date,
                       CAST(date_trunc('month', c.process_date) AS DATE) AS month_start
                FROM cfg c
            ),
            {sub_ord_cte},
            -- Активные заказы (AB0) на дату среза с уникальными subscriber_id
            actives AS (
                SELECT b.process_date AS report_date,
                       b.month_start,
                       o.segment_id,
                       o.base_type AS service_kind,
                       o.tariff_id,
                       o.client_type,
                       o.client_type_title,
                       COUNT(DISTINCT o.subscriber_id) AS active_clients
                FROM mb b
                INNER JOIN sub_ord o ON true
                WHERE {active_day_slice}
                GROUP BY b.process_date, b.month_start, o.segment_id, o.base_type, o.tariff_id,
                         o.client_type, o.client_type_title
            ),
            -- Дневная выручка: credits / число дней интервала проводки (нормализация)
            day_rev AS (
                SELECT b.process_date AS report_date,
                       o.segment_id,
                       o.base_type AS service_kind,
                       o.tariff_id,
                       o.client_type,
                       o.client_type_title,
                       CAST(SUM({ledger_daily}) AS DOUBLE) AS total_revenue
                FROM mb b
                INNER JOIN sub_ord o ON true
                INNER JOIN {dds_ledgers} l ON l.order_id = o.id
                WHERE {active_day_slice}
                  AND {ledger_where}
                  AND {ledger_covers_day}
                GROUP BY b.process_date, o.segment_id, o.base_type, o.tariff_id,
                         o.client_type, o.client_type_title
            )
        SELECT
            a.report_date,
            a.segment_id,
            a.service_kind,
            a.tariff_id,
            COALESCE(CAST(dt.tariff_title AS VARCHAR), CAST('' AS VARCHAR)),
            a.client_type,
            a.client_type_title,
            ROUND(COALESCE(d.total_revenue, CAST(0 AS DOUBLE)), 2),
            a.active_clients,
            current_timestamp
        FROM actives a
        LEFT JOIN day_rev d
            ON a.report_date = d.report_date
           AND a.segment_id IS NOT DISTINCT FROM d.segment_id
           AND a.service_kind IS NOT DISTINCT FROM d.service_kind
           AND a.tariff_id IS NOT DISTINCT FROM d.tariff_id
           AND a.client_type IS NOT DISTINCT FROM d.client_type
           AND a.client_type_title IS NOT DISTINCT FROM d.client_type_title
        LEFT JOIN {mart_dim_tariff_t} dt ON dt.tariff_id IS NOT DISTINCT FROM a.tariff_id
        """
        return ("kpi_revenue_daily", [f"DELETE FROM {mart_t} WHERE report_date = {rd}", insert])

    if vitrina_key == "receipts":
        mart_t = quote_table(catalog, mart_schema, "kpi_receipts_daily")
        active_day_slice = _subscription_active_on_date("CAST(b.process_date AS DATE)")
        insert = f"""
        INSERT INTO {mart_t} (
            report_date, segment_id, service_kind, tariff_id, tariff_title,
            client_type, client_type_title,
            total_receipts, paying_owners, refreshed_at
        )
        WITH
            {cfg_cte},
            mb AS (
                SELECT c.process_date,
                       CAST(date_trunc('month', c.process_date) AS DATE) AS month_start
                FROM cfg c
            ),
            {sub_ord_cte},
            {_receipts_cte(dds_ledgers)},
            actives AS (
                SELECT b.process_date AS report_date,
                       b.month_start,
                       o.segment_id,
                       o.base_type AS service_kind,
                       o.tariff_id,
                       o.client_type,
                       o.client_type_title,
                       COUNT(DISTINCT o.subscriber_id) AS active_clients
                FROM mb b
                INNER JOIN sub_ord o ON true
                WHERE {active_day_slice}
                GROUP BY b.process_date, b.month_start, o.segment_id, o.base_type, o.tariff_id,
                         o.client_type, o.client_type_title
            ),
            day_rcpt AS (
                SELECT b.process_date AS report_date,
                       o.segment_id,
                       o.base_type AS service_kind,
                       o.tariff_id,
                       o.client_type,
                       o.client_type_title,
                       CAST(SUM(r.receipt_amount) AS DOUBLE) AS total_receipts
                FROM mb b
                INNER JOIN rcpt r
                    ON r.revenue_day = CAST(b.process_date AS DATE)
                INNER JOIN sub_ord o ON o.id = r.order_id
                WHERE {active_day_slice}
                GROUP BY b.process_date, o.segment_id, o.base_type, o.tariff_id,
                         o.client_type, o.client_type_title
            )
        SELECT
            a.report_date,
            a.segment_id,
            a.service_kind,
            a.tariff_id,
            COALESCE(CAST(dt.tariff_title AS VARCHAR), CAST('' AS VARCHAR)),
            a.client_type,
            a.client_type_title,
            ROUND(COALESCE(d.total_receipts, CAST(0 AS DOUBLE)), 2),
            a.active_clients,
            current_timestamp
        FROM actives a
        LEFT JOIN day_rcpt d
            ON a.report_date = d.report_date
           AND a.segment_id IS NOT DISTINCT FROM d.segment_id
           AND a.service_kind IS NOT DISTINCT FROM d.service_kind
           AND a.tariff_id IS NOT DISTINCT FROM d.tariff_id
           AND a.client_type IS NOT DISTINCT FROM d.client_type
           AND a.client_type_title IS NOT DISTINCT FROM d.client_type_title
        LEFT JOIN {mart_dim_tariff_t} dt ON dt.tariff_id IS NOT DISTINCT FROM a.tariff_id
        """
        return ("kpi_receipts_daily", [f"DELETE FROM {mart_t} WHERE report_date = {rd}", insert])

    if vitrina_key == "arpu":
        mart_t = quote_table(catalog, mart_schema, "kpi_arpu_daily")
        active_day_slice = _subscription_active_on_date("CAST(b.process_date AS DATE)")
        window_start_expr = _arpu_window_start_sql("c.process_date")
        days_in_month_expr = _arpu_days_in_month_sql("a.report_date")
        insert = f"""
        INSERT INTO {mart_t} (
            report_date, month_start, segment_id, service_kind, tariff_id, tariff_title,
            client_type, client_type_title,
            total_revenue, active_clients, arpu_daily, arpu_monthly, refreshed_at
        )
        WITH
            {cfg_cte},
            -- ARPU окно: конец месяца → календарный месяц; иначе → скользящие N дней
            mb AS (
                SELECT c.process_date,
                       {window_start_expr} AS month_start
                FROM cfg c
            ),
            {sub_ord_cte},
            -- Активные клиенты (AB0) на дату среза — знаменатель ARPU
            actives AS (
                SELECT b.process_date AS report_date,
                       b.month_start,
                       o.segment_id,
                       o.base_type AS service_kind,
                       o.tariff_id,
                       o.client_type,
                       o.client_type_title,
                       COUNT(DISTINCT o.subscriber_id) AS active_clients
                FROM mb b
                INNER JOIN sub_ord o ON true
                WHERE {active_day_slice}
                GROUP BY b.process_date, b.month_start, o.segment_id, o.base_type, o.tariff_id,
                         o.client_type, o.client_type_title
            ),
            -- Дневная нормализованная выручка (та же методика, что kpi_revenue_daily)
            day_rev AS (
                SELECT b.process_date AS report_date,
                       b.month_start,
                       o.segment_id,
                       o.base_type AS service_kind,
                       o.tariff_id,
                       o.client_type,
                       o.client_type_title,
                       CAST(SUM({ledger_daily}) AS DOUBLE) AS total_revenue
                FROM mb b
                INNER JOIN sub_ord o ON true
                INNER JOIN {dds_ledgers} l ON l.order_id = o.id
                WHERE {active_day_slice}
                  AND {ledger_where}
                  AND {ledger_covers_day}
                GROUP BY b.process_date, b.month_start, o.segment_id, o.base_type, o.tariff_id,
                         o.client_type, o.client_type_title
            )
        SELECT
            a.report_date,
            a.month_start,
            a.segment_id,
            a.service_kind,
            a.tariff_id,
            COALESCE(CAST(dt.tariff_title AS VARCHAR), CAST('' AS VARCHAR)),
            a.client_type,
            a.client_type_title,
            ROUND(COALESCE(d.total_revenue, CAST(0 AS DOUBLE)), 2),
            a.active_clients,
            -- ARPU за день: суммарная дневная выручка / число активных клиентов
            CASE
                WHEN a.active_clients > 0
                THEN ROUND(
                    COALESCE(d.total_revenue, CAST(0 AS DOUBLE)) / CAST(a.active_clients AS DOUBLE),
                    2
                )
                ELSE CAST(NULL AS DOUBLE)
            END,
            -- ARPU за месяц: arpu_daily × число календарных дней в месяце anchor
            CASE
                WHEN a.active_clients > 0
                THEN ROUND(
                    COALESCE(d.total_revenue, CAST(0 AS DOUBLE))
                    / CAST(a.active_clients AS DOUBLE)
                    * CAST({days_in_month_expr} AS DOUBLE),
                    2
                )
                ELSE CAST(NULL AS DOUBLE)
            END,
            current_timestamp
        FROM actives a
        LEFT JOIN day_rev d
            ON a.report_date = d.report_date
           AND a.month_start IS NOT DISTINCT FROM d.month_start
           AND a.segment_id IS NOT DISTINCT FROM d.segment_id
           AND a.service_kind IS NOT DISTINCT FROM d.service_kind
           AND a.tariff_id IS NOT DISTINCT FROM d.tariff_id
           AND a.client_type IS NOT DISTINCT FROM d.client_type
           AND a.client_type_title IS NOT DISTINCT FROM d.client_type_title
        LEFT JOIN {mart_dim_tariff_t} dt ON dt.tariff_id IS NOT DISTINCT FROM a.tariff_id
        """
        return ("kpi_arpu_daily", [f"DELETE FROM {mart_t} WHERE report_date = {rd}", insert])

    if vitrina_key == "revenue_active_month":
        mart_t = quote_table(catalog, mart_schema, "kpi_revenue_active_month")
        period_end_d = _calendar_month_end(report_day)
        month_start_d = date(report_day.year, report_day.month, 1)
        delete_pe = sql_date(period_end_d)
        rd_ms = sql_date(month_start_d)
        insert = f"""
        INSERT INTO {mart_t} (
            period_end, segment_id, service_kind, tariff_id, tariff_title,
            client_type, client_type_title,
            total_revenue, paying_owners, refreshed_at
        )
        WITH
            cfg AS (
                SELECT CAST({rd} AS DATE) AS report_anchor,
                       CAST({rd_ms} AS DATE) AS month_start,
                       CAST({delete_pe} AS DATE) AS period_end
            ),
            rev_sum AS (
                SELECT
                    c.period_end,
                    r.segment_id,
                    r.service_kind,
                    r.tariff_id,
                    r.client_type,
                    r.client_type_title,
                    ROUND(SUM(r.total_revenue), 2) AS total_revenue
                FROM cfg c
                INNER JOIN {mart_revenue_daily_t} r
                    ON r.report_date >= c.month_start
                   AND r.report_date <= LEAST(c.report_anchor, c.period_end)
                GROUP BY c.period_end, r.segment_id, r.service_kind, r.tariff_id,
                         r.client_type, r.client_type_title
            ),
            last_ab AS (
                SELECT
                    c.period_end,
                    a.segment_id,
                    a.service_kind,
                    a.tariff_id,
                    a.client_type,
                    a.client_type_title,
                    a.active_clients
                FROM cfg c
                INNER JOIN {mart_arpu_t} a
                    ON a.report_date = LEAST(c.report_anchor, c.period_end)
            )
        SELECT
            rs.period_end,
            rs.segment_id,
            rs.service_kind,
            rs.tariff_id,
            COALESCE(CAST(dt.tariff_title AS VARCHAR), CAST('' AS VARCHAR)),
            rs.client_type,
            rs.client_type_title,
            rs.total_revenue,
            COALESCE(la.active_clients, CAST(0 AS BIGINT)),
            current_timestamp
        FROM rev_sum rs
        LEFT JOIN last_ab la
            ON rs.period_end IS NOT DISTINCT FROM la.period_end
           AND rs.segment_id IS NOT DISTINCT FROM la.segment_id
           AND rs.service_kind IS NOT DISTINCT FROM la.service_kind
           AND rs.tariff_id IS NOT DISTINCT FROM la.tariff_id
           AND rs.client_type IS NOT DISTINCT FROM la.client_type
           AND rs.client_type_title IS NOT DISTINCT FROM la.client_type_title
        LEFT JOIN {mart_dim_tariff_t} dt ON dt.tariff_id IS NOT DISTINCT FROM rs.tariff_id
        """
        return ("kpi_revenue_active_month", [f"DELETE FROM {mart_t} WHERE period_end = {delete_pe}", insert])

    if vitrina_key == "revenue_active_quarter":
        mart_t = quote_table(catalog, mart_schema, "kpi_revenue_active_quarter")
        p_start_d = _calendar_quarter_start(report_day)
        period_end_d = _calendar_quarter_end(report_day)
        delete_pe = sql_date(period_end_d)
        rd_ps = sql_date(p_start_d)
        insert = f"""
        INSERT INTO {mart_t} (
            period_end, segment_id, service_kind, tariff_id, tariff_title,
            client_type, client_type_title,
            total_revenue, paying_owners, refreshed_at
        )
        WITH
            cfg AS (
                SELECT CAST({rd} AS DATE) AS report_anchor,
                       CAST({rd_ps} AS DATE) AS p_start,
                       CAST({delete_pe} AS DATE) AS period_end
            ),
            bounds AS (
                SELECT report_anchor, p_start, period_end FROM cfg
            ),
            rev_sum AS (
                SELECT
                    b.period_end,
                    r.segment_id,
                    r.service_kind,
                    r.tariff_id,
                    r.client_type,
                    r.client_type_title,
                    ROUND(SUM(r.total_revenue), 2) AS total_revenue
                FROM bounds b
                INNER JOIN {mart_revenue_daily_t} r
                    ON r.report_date >= b.p_start
                   AND r.report_date <= LEAST(b.report_anchor, b.period_end)
                GROUP BY b.period_end, r.segment_id, r.service_kind, r.tariff_id,
                         r.client_type, r.client_type_title
            ),
            last_ab AS (
                SELECT
                    b.period_end,
                    a.segment_id,
                    a.service_kind,
                    a.tariff_id,
                    a.client_type,
                    a.client_type_title,
                    a.active_clients
                FROM bounds b
                INNER JOIN {mart_arpu_t} a
                    ON a.report_date = LEAST(b.report_anchor, b.period_end)
            )
        SELECT
            rs.period_end,
            rs.segment_id,
            rs.service_kind,
            rs.tariff_id,
            COALESCE(CAST(dt.tariff_title AS VARCHAR), CAST('' AS VARCHAR)),
            rs.client_type,
            rs.client_type_title,
            rs.total_revenue,
            COALESCE(la.active_clients, CAST(0 AS BIGINT)),
            current_timestamp
        FROM rev_sum rs
        LEFT JOIN last_ab la
            ON rs.period_end IS NOT DISTINCT FROM la.period_end
           AND rs.segment_id IS NOT DISTINCT FROM la.segment_id
           AND rs.service_kind IS NOT DISTINCT FROM la.service_kind
           AND rs.tariff_id IS NOT DISTINCT FROM la.tariff_id
           AND rs.client_type IS NOT DISTINCT FROM la.client_type
           AND rs.client_type_title IS NOT DISTINCT FROM la.client_type_title
        LEFT JOIN {mart_dim_tariff_t} dt ON dt.tariff_id IS NOT DISTINCT FROM rs.tariff_id
        """
        return ("kpi_revenue_active_quarter", [f"DELETE FROM {mart_t} WHERE period_end = {delete_pe}", insert])

    if vitrina_key == "revenue_active_year":
        mart_t = quote_table(catalog, mart_schema, "kpi_revenue_active_year")
        p_start_d = date(report_day.year, 1, 1)
        period_end_d = _calendar_year_end(report_day)
        delete_pe = sql_date(period_end_d)
        rd_ps = sql_date(p_start_d)
        insert = f"""
        INSERT INTO {mart_t} (
            period_end, segment_id, service_kind, tariff_id, tariff_title,
            client_type, client_type_title,
            total_revenue, paying_owners, refreshed_at
        )
        WITH
            cfg AS (
                SELECT CAST({rd} AS DATE) AS report_anchor,
                       CAST({rd_ps} AS DATE) AS p_start,
                       CAST({delete_pe} AS DATE) AS period_end
            ),
            bounds AS (
                SELECT report_anchor, p_start, period_end FROM cfg
            ),
            rev_sum AS (
                SELECT
                    b.period_end,
                    r.segment_id,
                    r.service_kind,
                    r.tariff_id,
                    r.client_type,
                    r.client_type_title,
                    ROUND(SUM(r.total_revenue), 2) AS total_revenue
                FROM bounds b
                INNER JOIN {mart_revenue_daily_t} r
                    ON r.report_date >= b.p_start
                   AND r.report_date <= LEAST(b.report_anchor, b.period_end)
                GROUP BY b.period_end, r.segment_id, r.service_kind, r.tariff_id,
                         r.client_type, r.client_type_title
            ),
            last_ab AS (
                SELECT
                    b.period_end,
                    a.segment_id,
                    a.service_kind,
                    a.tariff_id,
                    a.client_type,
                    a.client_type_title,
                    a.active_clients
                FROM bounds b
                INNER JOIN {mart_arpu_t} a
                    ON a.report_date = LEAST(b.report_anchor, b.period_end)
            )
        SELECT
            rs.period_end,
            rs.segment_id,
            rs.service_kind,
            rs.tariff_id,
            COALESCE(CAST(dt.tariff_title AS VARCHAR), CAST('' AS VARCHAR)),
            rs.client_type,
            rs.client_type_title,
            rs.total_revenue,
            COALESCE(la.active_clients, CAST(0 AS BIGINT)),
            current_timestamp
        FROM rev_sum rs
        LEFT JOIN last_ab la
            ON rs.period_end IS NOT DISTINCT FROM la.period_end
           AND rs.segment_id IS NOT DISTINCT FROM la.segment_id
           AND rs.service_kind IS NOT DISTINCT FROM la.service_kind
           AND rs.tariff_id IS NOT DISTINCT FROM la.tariff_id
           AND rs.client_type IS NOT DISTINCT FROM la.client_type
           AND rs.client_type_title IS NOT DISTINCT FROM la.client_type_title
        LEFT JOIN {mart_dim_tariff_t} dt ON dt.tariff_id IS NOT DISTINCT FROM rs.tariff_id
        """
        return ("kpi_revenue_active_year", [f"DELETE FROM {mart_t} WHERE period_end = {delete_pe}", insert])

    if vitrina_key == "revenue_active_week":
        mart_t = quote_table(catalog, mart_schema, "kpi_revenue_active_week")
        w_mon, w_sun = _iso_week_monday_sunday(report_day)
        delete_pe = sql_date(w_sun)
        rd_wm = sql_date(w_mon)
        insert = f"""
        INSERT INTO {mart_t} (
            period_end, segment_id, service_kind, tariff_id, tariff_title,
            client_type, client_type_title,
            total_revenue, paying_owners, refreshed_at
        )
        WITH
            cfg AS (
                SELECT CAST({rd} AS DATE) AS report_anchor,
                       CAST({rd_wm} AS DATE) AS p_start,
                       CAST({delete_pe} AS DATE) AS period_end
            ),
            bounds AS (
                SELECT report_anchor, p_start, period_end FROM cfg
            ),
            rev_sum AS (
                SELECT
                    b.period_end,
                    r.segment_id,
                    r.service_kind,
                    r.tariff_id,
                    r.client_type,
                    r.client_type_title,
                    ROUND(SUM(r.total_revenue), 2) AS total_revenue
                FROM bounds b
                INNER JOIN {mart_revenue_daily_t} r
                    ON r.report_date >= b.p_start
                   AND r.report_date <= LEAST(b.report_anchor, b.period_end)
                GROUP BY b.period_end, r.segment_id, r.service_kind, r.tariff_id,
                         r.client_type, r.client_type_title
            ),
            last_ab AS (
                SELECT
                    b.period_end,
                    a.segment_id,
                    a.service_kind,
                    a.tariff_id,
                    a.client_type,
                    a.client_type_title,
                    a.active_clients
                FROM bounds b
                INNER JOIN {mart_arpu_t} a
                    ON a.report_date = LEAST(b.report_anchor, b.period_end)
            )
        SELECT
            rs.period_end,
            rs.segment_id,
            rs.service_kind,
            rs.tariff_id,
            COALESCE(CAST(dt.tariff_title AS VARCHAR), CAST('' AS VARCHAR)),
            rs.client_type,
            rs.client_type_title,
            rs.total_revenue,
            COALESCE(la.active_clients, CAST(0 AS BIGINT)),
            current_timestamp
        FROM rev_sum rs
        LEFT JOIN last_ab la
            ON rs.period_end IS NOT DISTINCT FROM la.period_end
           AND rs.segment_id IS NOT DISTINCT FROM la.segment_id
           AND rs.service_kind IS NOT DISTINCT FROM la.service_kind
           AND rs.tariff_id IS NOT DISTINCT FROM la.tariff_id
           AND rs.client_type IS NOT DISTINCT FROM la.client_type
           AND rs.client_type_title IS NOT DISTINCT FROM la.client_type_title
        LEFT JOIN {mart_dim_tariff_t} dt ON dt.tariff_id IS NOT DISTINCT FROM rs.tariff_id
        """
        return ("kpi_revenue_active_week", [f"DELETE FROM {mart_t} WHERE period_end = {delete_pe}", insert])

    if vitrina_key == "inflow":
        mart_t = quote_table(catalog, mart_schema, "kpi_inflow_daily")
        insert = f"""
        INSERT INTO {mart_t} (
            report_date, segment_id, service_kind, tariff_id, tariff_title,
            client_type, client_type_title,
            new_clients, new_agreements, new_orders, refreshed_at
        )
        WITH {cfg_cte},
             {sub_ord_cte},
             -- Первый день активации для каждого клиента (глобальный минимум по subscriber_id)
             first_client_activation AS (
                 SELECT subscriber_id,
                        MIN(CAST(activated AS DATE)) AS first_activation_day
                 FROM sub_ord
                 WHERE activated IS NOT NULL
                 GROUP BY subscriber_id
             ),
             -- Первый день активации для каждого договора (глобальный минимум по agreement_id)
             first_agreement_activation AS (
                 SELECT owner_id AS agreement_id,
                        MIN(CAST(activated AS DATE)) AS first_activation_day
                 FROM sub_ord
                 WHERE activated IS NOT NULL
                 GROUP BY owner_id
             )
        SELECT c.process_date,
               o.segment_id,
               o.base_type,
               o.tariff_id,
               COALESCE(MAX(CAST(dt.tariff_title AS VARCHAR)), CAST('' AS VARCHAR)),
               o.client_type,
               o.client_type_title,
               -- Новые клиенты: те, у кого глобальный первый день активации совпадает с report_date
               COUNT(DISTINCT CASE
                   WHEN fca.first_activation_day = c.process_date
                   THEN o.subscriber_id
               END),
               -- Новые договоры: те, у которых глобальный первый день активации = report_date
               COUNT(DISTINCT CASE
                   WHEN faa.first_activation_day = c.process_date
                   THEN o.owner_id
               END),
               -- Новые заказы: все заказы с activated = report_date
               COUNT(*),
               current_timestamp
        FROM cfg c
        INNER JOIN sub_ord o ON CAST(o.activated AS DATE) = c.process_date
        INNER JOIN first_client_activation fca ON fca.subscriber_id = o.subscriber_id
        INNER JOIN first_agreement_activation faa ON faa.agreement_id = o.owner_id
        LEFT JOIN {mart_dim_tariff_t} dt ON dt.tariff_id IS NOT DISTINCT FROM o.tariff_id
        GROUP BY c.process_date, o.segment_id, o.base_type, o.tariff_id, o.client_type, o.client_type_title
        """
        return ("kpi_inflow_daily", [f"DELETE FROM {mart_t} WHERE report_date = {rd}", insert])

    # outflow: завершившиеся заказы в день; договоры/клиенты — если этот день = MAX(expire) по сущности.
    mart_t = quote_table(catalog, mart_schema, "kpi_outflow_daily")
    insert = f"""
    INSERT INTO {mart_t} (
        report_date, segment_id, service_kind, tariff_id, tariff_title,
        client_type, client_type_title,
        churned_clients, completed_agreements, completed_orders, refreshed_at
    )
    WITH {cfg_cte},
         {sub_ord_cte},
         -- Последний день окончания услуги для каждого договора (MAX expire_time)
         agreement_last_end AS (
             SELECT owner_id AS agreement_id,
                    MAX(CAST(expire_time AS DATE)) AS last_service_end_day
             FROM sub_ord
             WHERE expire_time IS NOT NULL
             GROUP BY owner_id
         ),
         -- Последний день окончания услуги для каждого клиента
         client_last_end AS (
             SELECT subscriber_id,
                    MAX(CAST(expire_time AS DATE)) AS last_service_end_day
             FROM sub_ord
             WHERE expire_time IS NOT NULL
             GROUP BY subscriber_id
         )
    SELECT c.process_date,
           o.segment_id,
           o.base_type,
           o.tariff_id,
           COALESCE(MAX(CAST(dt.tariff_title AS VARCHAR)), CAST('' AS VARCHAR)),
           o.client_type,
           o.client_type_title,
           -- Ушедшие клиенты: те, у кого последний expire = report_date
           COUNT(DISTINCT CASE
               WHEN cle.last_service_end_day = c.process_date
               THEN o.subscriber_id
           END),
           -- Завершённые договоры: договор, у которого MAX(expire) = report_date
           COUNT(DISTINCT CASE
               WHEN ale.last_service_end_day = c.process_date
               THEN o.owner_id
           END),
           -- Завершённые заказы: все заказы с expire_time = report_date
           COUNT(*),
           current_timestamp
    FROM cfg c
    INNER JOIN sub_ord o
        ON o.expire_time IS NOT NULL
       AND CAST(o.expire_time AS DATE) = c.process_date
    LEFT JOIN agreement_last_end ale ON ale.agreement_id = o.owner_id
    LEFT JOIN client_last_end cle ON cle.subscriber_id = o.subscriber_id
    LEFT JOIN {mart_dim_tariff_t} dt ON dt.tariff_id IS NOT DISTINCT FROM o.tariff_id
    GROUP BY c.process_date, o.segment_id, o.base_type, o.tariff_id, o.client_type, o.client_type_title
    """
    return ("kpi_outflow_daily", [f"DELETE FROM {mart_t} WHERE report_date = {rd}", insert])
