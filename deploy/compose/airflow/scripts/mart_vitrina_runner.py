"""Точка входа CLI для одной KPI-витрины: DDL целевой таблицы при необходимости + DELETE строк за день + INSERT из DDS (Trino).

Тексты запросов и DDL — модуль mart_runner_common. От Airflow каждая витрина дергается
отдельным скриптом в mart_jobs/ с фиксированным implicit_default_vitrina (без общего shim).
По умолчанию перед генерацией создаются схема mart и одна целевая Iceberg-таблица для этой витрины;
флаг ``--ensure-all-ddl`` создаёт все KPI-таблицы разом (инициализация контура).
"""
from __future__ import annotations

import argparse
import logging
import os
from datetime import date

from load_raw_day import (
    build_trino_target_config,
    connect_trino,
    load_config,
    quote_ident,
    setup_logging,
)

from mart_runner_common import (
    KPI_TABLES_WITH_CLIENT_TYPE,
    KPI_TABLES_WITH_TARIFF_TITLE,
    VITRINA_KEYS,
    VITRINAS_READING_KPI_ARPU_DAILY,
    add_arpu_monthly_column_sql,
    add_client_type_column_sql,
    add_client_type_title_column_sql,
    add_tariff_title_column_sql,
    dds_schema_from_config,
    ensure_tables_sql,
    ensure_vitrina_target_ddl_sql,
    kpi_arpu_paying_owners_rename_migration_sql,
    kpi_inflow_legacy_connected_owners_migration_sqls,
    kpi_outflow_legacy_terminal_snapshots_migration_sqls,
    mart_schema_from_config,
    normalize_vitrina_key,
    statements_for_vitrina,
)

LOGGER = logging.getLogger(__name__)


def _information_schema_column_names(cur, *, catalog: str, schema: str, table: str) -> set[str]:
    """Читает имена колонок целевой Iceberg-таблицы из ``information_schema.columns``.

    Используется для проверки необходимости DDL-миграций (ALTER ADD COLUMN, RENAME и т.п.)
    перед выполнением INSERT. Без нормализации регистра — возвращает имена как есть.

    Args:
        cur: Курсор Trino.
        catalog: Каталог (например ``iceberg``).
        schema: Схема (например ``mart``).
        table: Имя таблицы (например ``kpi_ab0_daily``).

    Returns:
        Множество строк — имена колонок.
    """
    c = catalog.replace("'", "''")
    s = schema.replace("'", "''")
    t = table.replace("'", "''")
    sql = f"""
    SELECT column_name
    FROM {quote_ident(catalog)}.information_schema.columns
    WHERE table_catalog = '{c}'
      AND table_schema = '{s}'
      AND table_name = '{t}'
    """
    cur.execute(sql.strip())
    rows = cur.fetchall() or []
    return {str(row[0]) for row in rows if row and row[0] is not None}


def _maybe_add_tariff_title_column(
    cur,
    *,
    catalog: str,
    mart_schema: str,
    logical_name: str,
    dry_run: bool,
) -> None:
    """Добавляет колонку ``tariff_title`` в Iceberg-таблицу, если её ещё нет.

    Колонка заполняется из справочника ``dim_tariff`` при INSERT. Проверка по
    ``information_schema.columns``, выполнение через ``ALTER TABLE … ADD COLUMN``.
    Применимо только к таблицам из ``KPI_TABLES_WITH_TARIFF_TITLE``.

    Args:
        cur: Курсор Trino.
        catalog: Каталог.
        mart_schema: Схема MART.
        logical_name: Логическое имя таблицы.
        dry_run: Если True — только логирование, без выполнения DDL.
    """
    if logical_name not in KPI_TABLES_WITH_TARIFF_TITLE:
        return
    try:
        cols = _information_schema_column_names(cur, catalog=catalog, schema=mart_schema, table=logical_name)
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning(
            "[WARN] mart: не удалось прочитать information_schema для %s.%s.%s: %s",
            catalog,
            mart_schema,
            logical_name,
            exc,
        )
        return
    if "tariff_title" in {c.lower() for c in cols}:
        return
    sql = add_tariff_title_column_sql(catalog=catalog, mart_schema=mart_schema, logical_table=logical_name)
    if dry_run:
        LOGGER.info("[INFO] mart dry_run: пропуск ADD COLUMN tariff_title: %s", sql.strip())
        return
    LOGGER.info("[INFO] mart: ADD COLUMN tariff_title в %s: %s", logical_name, sql.strip())
    cur.execute(sql.strip())


def _maybe_add_client_type_column(
    cur,
    *,
    catalog: str,
    mart_schema: str,
    logical_name: str,
    dry_run: bool,
) -> None:
    """Добавляет колонку ``client_type`` (person/ip/org/unknown) при отсутствии.

    Только для таблиц из ``KPI_TABLES_WITH_CLIENT_TYPE``.

    Args:
        cur: Курсор Trino.
        catalog: Каталог.
        mart_schema: Схема MART.
        logical_name: Логическое имя таблицы.
        dry_run: Флаг dry-run.
    """
    if logical_name not in KPI_TABLES_WITH_CLIENT_TYPE:
        return
    try:
        cols = _information_schema_column_names(cur, catalog=catalog, schema=mart_schema, table=logical_name)
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning(
            "[WARN] mart: не удалось прочитать information_schema для %s.%s.%s: %s",
            catalog,
            mart_schema,
            logical_name,
            exc,
        )
        return
    if "client_type" in {c.lower() for c in cols}:
        return
    sql = add_client_type_column_sql(catalog=catalog, mart_schema=mart_schema, logical_table=logical_name)
    if dry_run:
        LOGGER.info("[INFO] mart dry_run: пропуск ADD COLUMN client_type: %s", sql.strip())
        return
    LOGGER.info("[INFO] mart: ADD COLUMN client_type в %s: %s", logical_name, sql.strip())
    cur.execute(sql.strip())


def _maybe_add_client_type_title_column(
    cur,
    *,
    catalog: str,
    mart_schema: str,
    logical_name: str,
    dry_run: bool,
) -> None:
    """Добавляет колонку ``client_type_title`` при её отсутствии.

    Человекочитаемая подпись типа клиента (например «Физлицо»). Заполняется
    из ``client_property_dim`` CTE при INSERT.

    Args:
        cur: Курсор Trino.
        catalog: Каталог.
        mart_schema: Схема MART.
        logical_name: Логическое имя таблицы.
        dry_run: Флаг dry-run.
    """
    if logical_name not in KPI_TABLES_WITH_CLIENT_TYPE:
        return
    try:
        cols = _information_schema_column_names(cur, catalog=catalog, schema=mart_schema, table=logical_name)
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning(
            "[WARN] mart: не удалось прочитать information_schema для %s.%s.%s: %s",
            catalog,
            mart_schema,
            logical_name,
            exc,
        )
        return
    if "client_type_title" in {c.lower() for c in cols}:
        return
    sql = add_client_type_title_column_sql(catalog=catalog, mart_schema=mart_schema, logical_table=logical_name)
    if dry_run:
        LOGGER.info("[INFO] mart dry_run: пропуск ADD COLUMN client_type_title: %s", sql.strip())
        return
    LOGGER.info("[INFO] mart: ADD COLUMN client_type_title в %s: %s", logical_name, sql.strip())
    cur.execute(sql.strip())


def _maybe_add_arpu_monthly_column(
    cur,
    *,
    catalog: str,
    mart_schema: str,
    logical_name: str,
    dry_run: bool,
) -> None:
    """Добавляет колонку ``arpu_monthly`` в ``kpi_arpu_daily``.

    ``arpu_monthly = arpu_daily × дней_в_месяце``; вычисляется на лету в INSERT,
    но физическая колонка нужна для хранения и репликации в ClickHouse.

    Args:
        cur: Курсор Trino.
        catalog: Каталог.
        mart_schema: Схема MART.
        logical_name: Должно быть ``"kpi_arpu_daily"``.
        dry_run: Флаг dry-run.
    """
    if logical_name != "kpi_arpu_daily":
        return
    try:
        cols = _information_schema_column_names(cur, catalog=catalog, schema=mart_schema, table="kpi_arpu_daily")
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning(
            "[WARN] mart: не удалось прочитать information_schema для %s.%s.kpi_arpu_daily: %s",
            catalog,
            mart_schema,
            exc,
        )
        return
    if "arpu_monthly" in {c.lower() for c in cols}:
        return
    sql = add_arpu_monthly_column_sql(catalog=catalog, mart_schema=mart_schema)
    if dry_run:
        LOGGER.info("[INFO] mart dry_run: пропуск ADD COLUMN arpu_monthly: %s", sql.strip())
        return
    LOGGER.info("[INFO] mart: ADD COLUMN arpu_monthly в kpi_arpu_daily: %s", sql.strip())
    cur.execute(sql.strip())


def _maybe_migrate_kpi_arpu_paying_owners_column(
    cur,
    *,
    catalog: str,
    mart_schema: str,
    vitrina_key: str,
    dry_run: bool,
) -> None:
    """Миграция схемы ``kpi_arpu_daily``: ``paying_owners`` → ``active_clients``.

    Переименование колонки при переходе со старой схемы. Только для витрин,
    читающих ``kpi_arpu_daily`` (``VITRINAS_READING_KPI_ARPU_DAILY``).

    Args:
        cur: Курсор Trino.
        catalog: Каталог.
        mart_schema: Схема MART.
        vitrina_key: Ключ витрины (для проверки необходимости ARPU).
        dry_run: Флаг dry-run.
    """
    if vitrina_key not in VITRINAS_READING_KPI_ARPU_DAILY:
        return
    try:
        cols = _information_schema_column_names(cur, catalog=catalog, schema=mart_schema, table="kpi_arpu_daily")
    except Exception as exc:  # noqa: BLE001 — не блокируем INSERT, если metadata недоступна
        LOGGER.warning(
            "[WARN] mart: не удалось прочитать information_schema для %s.%s.kpi_arpu_daily: %s",
            catalog,
            mart_schema,
            exc,
        )
        return
    mig = kpi_arpu_paying_owners_rename_migration_sql(catalog, mart_schema, existing_columns=cols)
    if not mig:
        return
    if dry_run:
        LOGGER.info("[INFO] mart dry_run: пропуск миграции схемы: %s", mig.strip())
        return
    LOGGER.info("[INFO] mart: миграция схемы kpi_arpu_daily: %s", mig.strip())
    cur.execute(mig.strip())


def _maybe_migrate_kpi_inflow_outflow_legacy_columns(
    cur,
    *,
    catalog: str,
    mart_schema: str,
    vitrina_key: str,
    dry_run: bool,
) -> None:
    """Миграция устаревшей схемы ``kpi_inflow_daily`` / ``kpi_outflow_daily``.

    - ``inflow``: ``new_connected_owners`` → ``new_clients`` + ``new_agreements``.
    - ``outflow``: ``terminal_*_snapshot`` → ``churned_clients`` + ``completed_*``.

    Args:
        cur: Курсор Trino.
        catalog: Каталог.
        mart_schema: Схема MART.
        vitrina_key: ``"inflow"`` или ``"outflow"``.
        dry_run: Флаг dry-run.
    """
    if vitrina_key not in {"inflow", "outflow"}:
        return
    table = "kpi_inflow_daily" if vitrina_key == "inflow" else "kpi_outflow_daily"
    try:
        cols = _information_schema_column_names(cur, catalog=catalog, schema=mart_schema, table=table)
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning(
            "[WARN] mart: не удалось прочитать information_schema для %s.%s.%s: %s",
            catalog,
            mart_schema,
            table,
            exc,
        )
        return
    if vitrina_key == "inflow":
        stmts = kpi_inflow_legacy_connected_owners_migration_sqls(
            catalog, mart_schema, existing_columns=cols
        )
    else:
        stmts = kpi_outflow_legacy_terminal_snapshots_migration_sqls(
            catalog, mart_schema, existing_columns=cols
        )
    if not stmts:
        return
    if dry_run:
        LOGGER.info("[INFO] mart dry_run: пропуск миграций %s: %s", table, stmts)
        return
    for sql in stmts:
        LOGGER.info("[INFO] mart: миграция схемы %s: %s", table, sql.strip())
        cur.execute(sql.strip())


def parse_args(implicit_default_vitrina: str | None = None) -> argparse.Namespace:
    """Разбор аргументов командной строки для ``mart_vitrina_runner``.

    Args:
        implicit_default_vitrina: Имя витрины по умолчанию (для mart_jobs-обёрток).
            Если задано, ``--vitrina`` становится опциональным.

    Returns:
        Namespace с полями: vitrina, config, report_date, log_level, dry_run, ensure_all_ddl.
    """
    p = argparse.ArgumentParser(description="Сгенерировать одну KPI-витрину в iceberg.mart")
    p.add_argument(
        "--vitrina",
        default=implicit_default_vitrina,
        required=implicit_default_vitrina is None,
        help=f"Идентификатор витрины: {sorted(VITRINA_KEYS)}",
    )
    p.add_argument(
        "--config",
        default="/opt/airflow/scripts/raw_load_config.json",
        help="JSON с секциями target / pipeline / dds",
    )
    p.add_argument(
        "--report-date",
        required=True,
        help="Календарный день витрины YYYY-MM-DD (как ds)",
    )
    p.add_argument(
        "--log-level",
        default=os.getenv("RAW_LOADER_LOG_LEVEL", "INFO"),
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Не выполнять DELETE/INSERT",
    )
    p.add_argument(
        "--ensure-all-ddl",
        action="store_true",
        help="Создать все KPI-таблицы в mart (иначе только схема + таблица текущей витрины)",
    )
    return p.parse_args()


def run_generate(
    *,
    args: argparse.Namespace | None = None,
    implicit_default_vitrina: str | None = None,
) -> None:
    """Основной рабочий процесс генерации одной KPI-витрины.

    Шаги:
    1. Загрузка конфигурации и разрешение имён схем.
    2. Генерация DELETE + INSERT через ``statements_for_vitrina``.
    3. Создание DDL (схема + таблица) и миграции колонок.
    4. Выполнение SQL в Trino (или dry-run).

    Args:
        args: Namespace с параметрами (если None — вызывается parse_args).
        implicit_default_vitrina: Витрина по умолчанию для mart_jobs-обёрток.
    """
    if args is None:
        args = parse_args(implicit_default_vitrina=implicit_default_vitrina)

    setup_logging(args.log_level)
    report_day = date.fromisoformat(args.report_date)
    vitrina_key = normalize_vitrina_key(str(args.vitrina))

    config = load_config(args.config)
    target_cfg = config.get("target", {})
    mart_schema = mart_schema_from_config(config)
    dds_schema = dds_schema_from_config(config)
    trino_target = build_trino_target_config(
        target_cfg, fallback_schema=str(target_cfg.get("default_schema", "raw"))
    )
    catalog = trino_target.catalog

    LOGGER.info(
        "[INFO] mart vitrina=%s report_date=%s catalog=%s mart=%s dds=%s dry_run=%s",
        vitrina_key,
        report_day.isoformat(),
        catalog,
        mart_schema,
        dds_schema,
        args.dry_run,
    )

    logical_name, stmts = statements_for_vitrina(
        catalog=catalog,
        mart_schema=mart_schema,
        dds_schema=dds_schema,
        vitrina_key=vitrina_key,
        report_day=report_day,
        config=config,
    )

    LOGGER.info("[INFO] mart target_table=%s sql_statements=%s", logical_name, len(stmts))

    with connect_trino(trino_target) as conn:
        with conn.cursor() as cur:
            cur.execute(trino_target.verify_connection_sql)
            _ = cur.fetchone()
            ddl_list = (
                ensure_tables_sql(catalog, mart_schema)
                if args.ensure_all_ddl
                else ensure_vitrina_target_ddl_sql(catalog, mart_schema, vitrina_key)
            )
            for ddl in ddl_list:
                cur.execute(ddl.strip())
            _maybe_add_tariff_title_column(
                cur,
                catalog=catalog,
                mart_schema=mart_schema,
                logical_name=logical_name,
                dry_run=bool(args.dry_run),
            )
            _maybe_add_client_type_column(
                cur,
                catalog=catalog,
                mart_schema=mart_schema,
                logical_name=logical_name,
                dry_run=bool(args.dry_run),
            )
            _maybe_add_client_type_title_column(
                cur,
                catalog=catalog,
                mart_schema=mart_schema,
                logical_name=logical_name,
                dry_run=bool(args.dry_run),
            )
            _maybe_add_arpu_monthly_column(
                cur,
                catalog=catalog,
                mart_schema=mart_schema,
                logical_name=logical_name,
                dry_run=bool(args.dry_run),
            )
            _maybe_migrate_kpi_arpu_paying_owners_column(
                cur,
                catalog=catalog,
                mart_schema=mart_schema,
                vitrina_key=vitrina_key,
                dry_run=bool(args.dry_run),
            )
            _maybe_migrate_kpi_inflow_outflow_legacy_columns(
                cur,
                catalog=catalog,
                mart_schema=mart_schema,
                vitrina_key=vitrina_key,
                dry_run=bool(args.dry_run),
            )
            if not args.dry_run:
                for sql in stmts:
                    cur.execute(sql.strip())
            commit_fn = getattr(conn, "commit", None)
            if callable(commit_fn):
                commit_fn()


def main(implicit_default_vitrina: str | None = None) -> None:
    args = parse_args(implicit_default_vitrina=implicit_default_vitrina)
    run_generate(args=args)


if __name__ == "__main__":
    main()
