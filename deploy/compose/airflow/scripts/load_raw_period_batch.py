"""Загрузка RAW-слоя для диапазона дат из PostgreSQL (HALK) в Iceberg через Trino.

Используется для массовой загрузки исторических данных за произвольный период.
В отличие от ``load_raw_day.py`` (одна дата), этот скрипт загружает сразу
несколько дней, управляя состоянием через служебную таблицу
``raw_load_service_state`` — так загрузку можно остановить и продолжить.

Режимы загрузки (из ``raw_load_config.json``):
  incremental — события из журнала global_log за весь период, нормализация
    в *__events и актуальное состояние в *__state.
  full — снимок всех full-таблиц на последний день периода в *__snapshot.

Параметры:
  --day YYYY-MM-DD — начальная дата (если не указана, вычисляется из
    служебной таблицы: last_loaded_day + 1).
  --days-count N — сколько дней грузить (по умолчанию 7).

Служебная таблица ``raw_load_service_state``:
  loader_name VARCHAR — идентификатор загрузчика (из конфига).
  tz_name VARCHAR — часовой пояс.
  last_fully_loaded_day DATE — последний успешно загруженный день.
  updated_at TIMESTAMP(6) WITH TIME ZONE — время обновления записи.
"""
from __future__ import annotations

import argparse
import logging
import os
from datetime import date, datetime, time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

LOGGER = logging.getLogger(__name__)

from load_raw_day import (
    TableStats,
    TrinoTargetConfig,
    build_pg_dsn,
    build_trino_target_config,
    commit_if_supported,
    connect_source,
    connect_trino,
    ensure_manifest_table,
    ensure_raw_structures,
    load_config,
    load_full_snapshot,
    load_incremental_table_streaming,
    parse_table_configs,
    quote_ident,
    quote_schema,
    quote_table,
    raw_load_source_id_from_config,
    setup_logging,
    split_table_name,
    sql_date,
    sql_string,
    sql_timestamptz,
    write_manifest,
)


def parse_args() -> argparse.Namespace:
    """Разбирает аргументы командной строки для пакетной загрузки периода.

    Returns:
        argparse.Namespace с атрибутами config, day, days_count, tz, log_level, dry_run.
    """
    parser = argparse.ArgumentParser(
        description="Load batch of calendar days to Iceberg schema via Trino",
    )
    parser.add_argument(
        "--config",
        default="/opt/airflow/scripts/raw_load_config.json",
        help="Path to JSON config",
    )
    parser.add_argument(
        "--day",
        required=False,
        help="Start day in YYYY-MM-DD format (UTC window [day, day+days_count))",
    )
    parser.add_argument(
        "--days-count",
        type=int,
        default=7,
        help="How many days to load from --day (default: 7)",
    )
    parser.add_argument(
        "--tz",
        default="UTC",
        help="Timezone passed to loader (example: Europe/Moscow)",
    )
    parser.add_argument(
        "--log-level",
        default=os.getenv("RAW_LOADER_LOG_LEVEL", "INFO"),
        help="Logging level (DEBUG, INFO, WARNING, ERROR)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Read source only, do not write to target",
    )
    return parser.parse_args()


def service_table_name(load_cfg: dict[str, Any]) -> str:
    """Извлекает имя служебной таблицы состояния из конфига.

    Args:
        load_cfg: Секция ``load`` из JSON-конфига.

    Returns:
        Имя таблицы (по умолчанию ``raw_load_service_state``).
    """
    service_cfg = load_cfg.get("batch_service", {})
    return str(service_cfg.get("state_table", "raw_load_service_state"))


def loader_name(load_cfg: dict[str, Any]) -> str:
    """Извлекает идентификатор загрузчика из конфига.

    Args:
        load_cfg: Секция ``load`` из JSON-конфига.

    Returns:
        Имя загрузчика (по умолчанию ``raw_period_batch_loader``).
    """
    service_cfg = load_cfg.get("batch_service", {})
    return str(service_cfg.get("loader_name", "raw_period_batch_loader"))


def bootstrap_last_loaded_day(load_cfg: dict[str, Any]) -> date | None:
    """Читает начальную дату из конфига для холодного старта.

    Используется когда служебная таблица пуста — загрузка начинается
    с bootstrap_last_loaded_day + 1.

    Args:
        load_cfg: Секция ``load`` из JSON-конфига.

    Returns:
        Дата в формате YYYY-MM-DD или None, если не задана.

    Raises:
        ValueError: Если значение не в формате YYYY-MM-DD.
    """
    service_cfg = load_cfg.get("batch_service", {})
    raw_value = service_cfg.get("bootstrap_last_loaded_day")
    if raw_value is None:
        return None
    bootstrap_value = str(raw_value).strip()
    if not bootstrap_value:
        return None
    try:
        return date.fromisoformat(bootstrap_value)
    except ValueError as exc:
        raise ValueError(
            "load.batch_service.bootstrap_last_loaded_day must be in YYYY-MM-DD format"
        ) from exc


def ensure_service_table(dst_cur, catalog: str, raw_schema: str, state_table: str) -> str:
    """Создаёт служебную таблицу состояния (если не существует).

    Таблица хранит позицию последней успешной загрузки для каждого загрузчика и tz.

    Args:
        dst_cur: Курсор Trino.
        catalog: Каталог Iceberg.
        raw_schema: Схема RAW.
        state_table: Имя таблицы.

    Returns:
        Полная ссылка на таблицу (catalog.schema.table).
    """
    table_ref = quote_table(catalog, raw_schema, state_table)
    dst_cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_ref} (
            loader_name VARCHAR,
            tz_name VARCHAR,
            last_fully_loaded_day DATE,
            updated_at TIMESTAMP(6) WITH TIME ZONE
        )
        """
    )
    return table_ref


def read_last_loaded_day(dst_cur, table_ref: str, configured_loader_name: str, tz_name: str) -> date | None:
    """Читает последний успешно загруженный день из служебной таблицы.

    Args:
        dst_cur: Курсор Trino.
        table_ref: Полная ссылка на таблицу состояния.
        configured_loader_name: Идентификатор загрузчика.
        tz_name: Часовой пояс.

    Returns:
        Дата последней загрузки или None, если записи нет.
    """
    dst_cur.execute(
        f"""
        SELECT last_fully_loaded_day
        FROM {table_ref}
        WHERE loader_name = {sql_string(configured_loader_name)}
          AND tz_name = {sql_string(tz_name)}
        LIMIT 1
        """
    )
    row = dst_cur.fetchone()
    if row is None:
        return None
    return row[0]


def write_last_loaded_day(
    dst_cur,
    table_ref: str,
    configured_loader_name: str,
    tz_name: str,
    day_value: date,
) -> None:
    """Записывает (MERGE) последний загруженный день в служебную таблицу.

    Использует MERGE для идемпотентности: обновляет существующую запись
    или создаёт новую.

    Args:
        dst_cur: Курсор Trino.
        table_ref: Полная ссылка на таблицу состояния.
        configured_loader_name: Идентификатор загрузчика.
        tz_name: Часовой пояс.
        day_value: Дата для записи.
    """
    dst_cur.execute(
        f"""
        MERGE INTO {table_ref} t
        USING (
            SELECT
                {sql_string(configured_loader_name)} AS loader_name,
                {sql_string(tz_name)} AS tz_name,
                {sql_date(day_value)} AS last_fully_loaded_day,
                {sql_timestamptz(datetime.now(timezone.utc))} AS updated_at
        ) s
        ON t.loader_name = s.loader_name
           AND t.tz_name = s.tz_name
        WHEN MATCHED THEN
            UPDATE SET
                last_fully_loaded_day = s.last_fully_loaded_day,
                updated_at = s.updated_at
        WHEN NOT MATCHED THEN
            INSERT (loader_name, tz_name, last_fully_loaded_day, updated_at)
            VALUES (s.loader_name, s.tz_name, s.last_fully_loaded_day, s.updated_at)
        """
    )


def build_period_window(
    start_day_value: date,
    tz_name: str,
    days_count: int,
) -> tuple[date, date, datetime, datetime]:
    """Вычисляет границы периода в UTC на основе начальной даты и tz.

    Args:
        start_day_value: Начальная дата (включительно).
        tz_name: Часовой пояс.
        days_count: Число дней в периоде.

    Returns:
        Кортеж (start_day, end_day, period_start_utc, period_end_utc).
        period_end_utc — эксклюзивная граница.
    """
    end_day_value = start_day_value + timedelta(days=days_count - 1)
    tz = ZoneInfo(tz_name)
    local_start = datetime.combine(start_day_value, time.min, tzinfo=tz)
    local_end = local_start + timedelta(days=days_count)
    return (
        start_day_value,
        end_day_value,
        local_start.astimezone(timezone.utc),
        local_end.astimezone(timezone.utc),
    )


def resolve_start_day(
    args_day: str | None,
    load_cfg: dict[str, Any],
    tz_name: str,
    trino_target: TrinoTargetConfig | None,
    raw_schema: str,
) -> date:
    """Определяет начальную дату загрузки из аргументов или служебной таблицы.

    Приоритет:
    1. Явный --day.
    2. last_fully_loaded_day + 1 из служебной таблицы.
    3. bootstrap_last_loaded_day + 1 из конфига.

    Args:
        args_day: Значение --day или None.
        load_cfg: Секция ``load`` конфига.
        tz_name: Часовой пояс.
        trino_target: Конфигурация Trino (нужна для чтения служебной таблицы).
        raw_schema: Схема RAW.

    Returns:
        Начальная дата.

    Raises:
        ValueError: Если дата не может быть определена.
        RuntimeError: Если trino_target не задан при авто-определении.
    """
    if args_day is not None:
        return date.fromisoformat(args_day)
    if trino_target is None:
        raise RuntimeError("Target Trino config is required to resolve --day from service table")
    with connect_trino(trino_target) as state_conn:
        with state_conn.cursor() as state_cur:
            state_cur.execute(trino_target.verify_connection_sql)
            _ = state_cur.fetchone()
            state_cur.execute(
                f"CREATE SCHEMA IF NOT EXISTS {quote_schema(trino_target.catalog, raw_schema)}"
            )
            state_table_ref = ensure_service_table(
                dst_cur=state_cur,
                catalog=trino_target.catalog,
                raw_schema=raw_schema,
                state_table=service_table_name(load_cfg),
            )
            commit_if_supported(state_conn, context="ensure_service_table_for_start_day")
            last_loaded_day = read_last_loaded_day(
                dst_cur=state_cur,
                table_ref=state_table_ref,
                configured_loader_name=loader_name(load_cfg),
                tz_name=tz_name,
            )
            if last_loaded_day is None:
                bootstrap_day = bootstrap_last_loaded_day(load_cfg)
                if bootstrap_day is None:
                    raise ValueError(
                        "--day is not specified and service table has no last loaded day for this loader/tz; "
                        "set load.batch_service.bootstrap_last_loaded_day in config for initial start"
                    )
                start_day_value = bootstrap_day + timedelta(days=1)
                LOGGER.info(
                    "[INFO] start day resolved from bootstrap config: bootstrap_last_loaded_day=%s start_day=%s",
                    bootstrap_day.isoformat(),
                    start_day_value.isoformat(),
                )
                return start_day_value
            start_day_value = last_loaded_day + timedelta(days=1)
            LOGGER.info(
                "[INFO] start day resolved from service table: last_loaded_day=%s start_day=%s",
                last_loaded_day.isoformat(),
                start_day_value.isoformat(),
            )
            return start_day_value


def main() -> None:
    """Точка входа: загрузка периода дней в RAW-слой.

    Алгоритм:
    1. Парсинг аргументов и конфига.
    2. Определение начальной даты (--day или служебная таблица).
    3. Построение UTC-окна периода.
    4. Инкрементальная загрузка всех таблиц через load_incremental_table_streaming
       (сразу за весь период, а не подневно).
    5. Full snapshot для full-таблиц на последний день периода.
    6. Запись манифеста и обновление служебной таблицы.
    """
    args = parse_args()
    setup_logging(args.log_level)
    if args.days_count <= 0:
        raise ValueError("--days-count must be greater than 0")
    config = load_config(args.config)
    source_cfg = config["source"]
    raw_lineage_id = raw_load_source_id_from_config(source_cfg)
    source_default_schema = str(source_cfg.get("default_schema", "public"))
    source_log_schema = str(source_cfg.get("log_schema", source_default_schema))
    source_log_table = str(source_cfg.get("log_table", "global_log"))
    table_configs = parse_table_configs(config["tables"], default_source_schema=source_default_schema)
    load_cfg = config.get("load", {})
    fallback_batch_size = int(load_cfg.get("batch_size", 1000))
    source_fetch_batch_size = int(load_cfg.get("source_fetch_batch_size", fallback_batch_size))
    target_insert_batch_size = int(load_cfg.get("target_insert_batch_size", fallback_batch_size))
    if source_fetch_batch_size <= 0:
        raise ValueError("load.source_fetch_batch_size must be greater than 0")
    if target_insert_batch_size <= 0:
        raise ValueError("load.target_insert_batch_size must be greater than 0")
    target_cfg = config.get("target", {})
    raw_schema = str(target_cfg.get("default_schema", "raw"))
    started_at = datetime.now(timezone.utc)

    src_dsn = build_pg_dsn(source_cfg)
    trino_target: TrinoTargetConfig | None = None
    if not args.dry_run or args.day is None:
        trino_target = build_trino_target_config(target_cfg, fallback_schema=raw_schema)
        raw_schema = trino_target.schema

    start_day_value = resolve_start_day(
        args_day=args.day,
        load_cfg=load_cfg,
        tz_name=args.tz,
        trino_target=trino_target,
        raw_schema=raw_schema,
    )
    start_day_value, end_day_value, period_start_utc, period_end_utc = build_period_window(
        start_day_value=start_day_value,
        tz_name=args.tz,
        days_count=args.days_count,
    )

    LOGGER.info(
        "[INFO] period batch: day_range=[%s..%s] days_count=%s tz=%s "
        "window_utc=[%s..%s) source_fetch_batch_size=%s target_insert_batch_size=%s "
        "source_log=%s.%s dry_run=%s",
        start_day_value.isoformat(),
        end_day_value.isoformat(),
        args.days_count,
        args.tz,
        period_start_utc.isoformat(),
        period_end_utc.isoformat(),
        source_fetch_batch_size,
        target_insert_batch_size,
        source_log_schema,
        source_log_table,
        args.dry_run,
    )
    LOGGER.info("[INFO] source driver=psycopg")
    if args.dry_run:
        LOGGER.info("[INFO] source DSN resolved (target is not required in dry-run)")
    else:
        LOGGER.info("[INFO] target driver=trino (Iceberg catalog)")

    with connect_source(src_dsn) as src_conn:
        with src_conn.cursor() as src_cur:
            LOGGER.info("[INFO] source connection established")
            if args.dry_run:
                incremental_tables = [table for table in table_configs if table.mode == "incremental"]
                incremental_filter_tables = sorted({table.name for table in incremental_tables})
                if incremental_filter_tables:
                    count_sql = f"""
                        SELECT count(*)
                        FROM {quote_ident(source_log_schema)}.{quote_ident(source_log_table)}
                        WHERE stamp >= %s
                          AND stamp < %s
                          AND "table" = ANY(%s::text[])
                    """
                    src_cur.execute(count_sql, (period_start_utc, period_end_utc, incremental_filter_tables))
                    inc_count = src_cur.fetchone()[0]
                else:
                    inc_count = 0
                LOGGER.info("[DRY-RUN] incremental global_log rows=%s", inc_count)

                for table in table_configs:
                    if table.mode == "full":
                        source_schema, source_table = split_table_name(table.name)
                        src_cur.execute(
                            f"SELECT count(*) FROM {quote_ident(source_schema)}.{quote_ident(source_table)}"
                        )
                        full_count = src_cur.fetchone()[0]
                        LOGGER.info("[DRY-RUN] full table=%s rows=%s", table.name, full_count)
                return

            if trino_target is None:
                raise RuntimeError("Target Trino config is not prepared for non-dry-run mode")

            with connect_trino(trino_target) as dst_conn:
                with dst_conn.cursor() as dst_cur:
                    dst_cur.execute(trino_target.verify_connection_sql)
                    _ = dst_cur.fetchone()
                    LOGGER.info(
                        "[INFO] target connection established: host=%s port=%s catalog=%s schema=%s",
                        trino_target.host,
                        trino_target.port,
                        trino_target.catalog,
                        trino_target.schema,
                    )

                    structures: dict[str, dict[str, str]] = {}
                    stats: dict[str, TableStats] = {table.name: TableStats() for table in table_configs}

                    dst_cur.execute(
                        f"CREATE SCHEMA IF NOT EXISTS {quote_schema(trino_target.catalog, raw_schema)}"
                    )
                    ensure_manifest_table(dst_cur, trino_target.catalog, raw_schema)
                    for table in table_configs:
                        if table.mode == "full":
                            structures[table.name] = ensure_raw_structures(
                                dst_cur=dst_cur,
                                catalog=trino_target.catalog,
                                raw_schema=raw_schema,
                                table=table,
                            )
                        else:
                            structures[table.name] = {}

                    incremental_list = [table for table in table_configs if table.mode == "incremental"]
                    incremental_names = [table.name for table in incremental_list]

                    for incremental_table in incremental_list:
                        LOGGER.info("[INFO] incremental bulk start: table=%s", incremental_table.name)
                        table_stats = load_incremental_table_streaming(
                            src_cur=src_cur,
                            dst_cur=dst_cur,
                            catalog=trino_target.catalog,
                            raw_schema=raw_schema,
                            table=incremental_table,
                            day_start=period_start_utc,
                            day_end=period_end_utc,
                            source_log_schema=source_log_schema,
                            source_log_table=source_log_table,
                            source_fetch_batch_size=source_fetch_batch_size,
                            target_insert_batch_size=target_insert_batch_size,
                            lineage_source_id=raw_lineage_id,
                        )
                        stats[incremental_table.name] = table_stats
                        LOGGER.info(
                            "[INFO] incremental bulk done: table=%s rows_read=%s events_inserted=%s "
                            "state_upserted=%s backfilled=%s replayed=%s invalid=%s cast_to_null=%s",
                            incremental_table.name,
                            table_stats.rows_read,
                            table_stats.rows_events_inserted,
                            table_stats.rows_state_upserted,
                            table_stats.rows_backfilled,
                            table_stats.rows_replayed,
                            table_stats.rows_invalid,
                            table_stats.rows_cast_to_null,
                        )

                    for table in table_configs:
                        if table.mode != "full":
                            continue
                        snapshot_table = structures[table.name]["snapshot"]
                        count = load_full_snapshot(
                            src_cur=src_cur,
                            dst_cur=dst_cur,
                            catalog=trino_target.catalog,
                            raw_schema=raw_schema,
                            table=table,
                            snapshot_day=end_day_value,
                            snapshot_table=snapshot_table,
                            source_fetch_batch_size=source_fetch_batch_size,
                            target_insert_batch_size=target_insert_batch_size,
                            lineage_source_id=raw_lineage_id,
                        )
                        stats[table.name].rows_read = count
                        stats[table.name].rows_snapshot_loaded = count
                        LOGGER.info(
                            "[INFO] full snapshot loaded: table=%s rows=%s snapshot_day=%s",
                            table.name,
                            count,
                            end_day_value.isoformat(),
                        )

                    finished_at = datetime.now(timezone.utc)
                    write_manifest(
                        dst_cur=dst_cur,
                        catalog=trino_target.catalog,
                        raw_schema=raw_schema,
                        day_value=end_day_value,
                        tz_name=args.tz,
                        day_start_utc=period_start_utc,
                        day_end_utc=period_end_utc,
                        table_configs=table_configs,
                        stats=stats,
                        started_at=started_at,
                        finished_at=finished_at,
                    )
                    state_table_ref = ensure_service_table(
                        dst_cur=dst_cur,
                        catalog=trino_target.catalog,
                        raw_schema=raw_schema,
                        state_table=service_table_name(load_cfg),
                    )
                    write_last_loaded_day(
                        dst_cur=dst_cur,
                        table_ref=state_table_ref,
                        configured_loader_name=loader_name(load_cfg),
                        tz_name=args.tz,
                        day_value=end_day_value,
                    )
                    commit_if_supported(dst_conn, context=f"batch_end_day={end_day_value.isoformat()}")

                    incremental_rows_processed = sum(
                        item.rows_read for name, item in stats.items() if name in incremental_names
                    )
                    full_rows_loaded = sum(item.rows_snapshot_loaded for item in stats.values())
                    backfilled_rows = sum(item.rows_backfilled for item in stats.values())
                    replayed_rows = sum(item.rows_replayed for item in stats.values())
                    invalid_rows = sum(item.rows_invalid for item in stats.values())
                    cast_to_null_rows = sum(item.rows_cast_to_null for item in stats.values())

                    LOGGER.info("[INFO] incremental_rows_processed=%s", incremental_rows_processed)
                    LOGGER.info("[INFO] full_rows_loaded=%s", full_rows_loaded)
                    LOGGER.info("[INFO] backfilled_rows=%s", backfilled_rows)
                    LOGGER.info("[INFO] replayed_rows=%s", replayed_rows)
                    LOGGER.info("[INFO] invalid_rows=%s", invalid_rows)
                    LOGGER.info("[INFO] cast_to_null_rows=%s", cast_to_null_rows)
                    LOGGER.info("[INFO] service_last_fully_loaded_day=%s", end_day_value.isoformat())
                    LOGGER.info("[INFO] target_catalog=%s", trino_target.catalog)
                    LOGGER.info("[INFO] target_schema=%s", raw_schema)


if __name__ == "__main__":
    main()
