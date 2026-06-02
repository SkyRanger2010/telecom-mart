"""
Одна строка из raw_load_config → одна таблица в DDS (Iceberg).

Типичный порядок внутри задачи Airflow для одной таблицы:
  1) (опционально) count источника в RAW для лога.
  2) dds_clean/*.sql — поправить зеркало в RAW перед чтением.
  3) dds_validate/*.sql — проверки, не ломающие пайплайн сами по себе.
  4) Загрузка в DDS из RAW-слоя: incremental — при наличии целевой таблицы дельта за суточное окно
     по ``*__events`` (MERGE + DELETE), иначе полный CREATE AS; full — последний snapshot в ``*_snapshot``.
  5) Запись в dds_table_load_log (dq_status/dq_note, rows_loaded).

Тонкие обёртки dds_jobs/*.py вызывают cli_for_table("public.orders") и пробрасывают cutoff-day.

При ``dds.scd2: true`` данные пишутся в Iceberg ``*_scd2``; привычное имя ``dds_*`` — VIEW с
``is_current = TRUE`` (витрины и MART могут не меняться). Откат: ``--dds-legacy-flat``.

Incremental SCD2: дельта из ``*__events`` + сравнение ``row_attr_hash`` зеркала с текущей версией.
Full snapshot SCD2: снимок за ``snapshot_day = cutoff_day`` и сравнение хеша ``payload``
(``from_big_endian_64(xxhash64(to_utf8(...)))``).
"""
from __future__ import annotations

import argparse
import logging
import os
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from load_raw_day import (
    ICEBERG_HOTSPOT_WRITE_RETRY_ATTEMPTS,
    RAW_LOAD_SOURCE_COL,
    RAW_LOADED_AT_COL,
    build_day_window,
    build_pg_dsn,
    build_trino_target_config,
    connect_source,
    connect_trino,
    execute_trino_write_with_iceberg_retry,
    fetch_source_columns,
    fetch_source_primary_key,
    load_config,
    parse_table_configs,
    quote_ident,
    quote_schema,
    quote_table,
    setup_logging,
    split_table_name,
    sql_bigint,
    sql_date,
    sql_string,
    sql_timestamptz,
    target_table_exists,
    target_table_name,
)

import dds_scd2

LOGGER = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Вспомогательный датакласс: статистика сборки одной таблицы
# ---------------------------------------------------------------------------


@dataclass
class BuiltTableStats:
    """Итог сборки одной таблицы DDS — имя, количество строк, режим источника.

    Attributes:
        name: Логическое имя таблицы DDS (dds_orders,
            dds_orders_snapshot и т.п.).
        rows: Число строк, загруженных/изменённых за прогон.
        source_mode: Режим RAW-источника — "incremental" или "full".
    """
    name: str
    rows: int
    source_mode: str


# ---------------------------------------------------------------------------
# Пути к sidecar-SQL: dds_validate/<table>.sql и dds_clean/<table>.sql
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).resolve().parent
# SQL с тем же именем, что и источник: public.orders → public__orders.sql
DDS_VALIDATE_DIR = SCRIPT_DIR / "dds_validate"
DDS_CLEAN_DIR = SCRIPT_DIR / "dds_clean"


# ===========================================================================
# Помощники чтения конфига DDS-секции
# ===========================================================================


def dds_cfg(config: dict[str, Any]) -> dict[str, Any]:
    """Извлечь dds-секцию конфигурации (raw_load_config.json).

    Args:
        config: Полный словарь конфигурации (ключи source, target, tables, dds).

    Returns:
        Словарь секции ``dds`` или пустой ``{}`` при её отсутствии.
    """
    return dict(config.get("dds", {}))


def dds_schema_name(config: dict[str, Any]) -> str:
    """Имя схемы DDS из конфига, по умолчанию ``dds``.

    Args:
        config: Полный словарь конфигурации.

    Returns:
        Имя схемы (строковое).
    """
    return str(dds_cfg(config).get("schema", "dds"))


def dds_service_table_name(config: dict[str, Any]) -> str:
    """Имя служебной таблицы состояния загрузки DDS.

    Args:
        config: Полный словарь конфигурации.

    Returns:
        Имя таблицы, по умолчанию ``dds_load_service_state``.
    """
    return str(dds_cfg(config).get("state_table", "dds_load_service_state"))


def dds_loader_name(config: dict[str, Any]) -> str:
    """Имя загрузчика для подписи в служебной таблице.

    Args:
        config: Полный словарь конфигурации.

    Returns:
        Имя загрузчика, по умолчанию ``dds_initial_loader``.
    """
    return str(dds_cfg(config).get("loader_name", "dds_initial_loader"))


# ===========================================================================
# Аргументы CLI / поиск конфига таблицы
# ===========================================================================


def normalize_table_arg(arg: str, default_schema: str) -> str:
    """Привести аргумент --table к полному qualified-имени.

    Если передан только ``orders``, достраивает ``public.orders``
    (схема из ``source.default_schema`` конфига).

    Args:
        arg: Строковое значение ``--table``.
        default_schema: Схема по умолчанию (обычно ``public``).

    Returns:
        Полное имя ``schema.table``.

    Raises:
        ValueError: Если аргумент пуст.
    """
    arg = arg.strip()
    if not arg:
        raise ValueError("--table must not be empty")
    if "." not in arg:
        return f"{default_schema}.{arg}"
    return arg


def find_table_cfg(table_configs: list[Any], want: str):
    """Найти конфиг таблицы по полному имени (без учёта регистра).

    Args:
        table_configs: Список объектов таблиц из конфига.
        want: Искомое имя, например ``public.orders``.

    Returns:
        Объект таблицы из конфига.

    Raises:
        ValueError: Если таблица не найдена в ``tables[]``.
    """
    want_l = want.lower().strip()
    for t in table_configs:
        if t.name.lower() == want_l:
            return t
    raise ValueError(f"Table '{want}' is not listed in config JSON tables[].name")


# ===========================================================================
# Служебные таблицы DDS (состояние загрузки, лог, манифест)
# ===========================================================================


def ensure_dds_service_table(dst_cur, catalog: str, dds_schema: str, state_table: str) -> str:
    """Создать (если нет) служебную таблицу состояния загрузки DDS.

    Аргументы:
        dst_cur: Trino-курсор.
        catalog: Каталог Iceberg.
        dds_schema: Схема DDS.
        state_table: Имя таблицы состояния.

    Returns:
        Полное qualified-имя таблицы ``catalog.schema.table``.
    """
    table_ref = quote_table(catalog, dds_schema, state_table)
    dst_cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_ref} (
            loader_name VARCHAR,
            snapshot_day DATE,
            updated_at TIMESTAMP(6) WITH TIME ZONE
        )
        """
    )
    return table_ref


def ensure_dds_table_load_log(dst_cur, catalog: str, dds_schema: str) -> str:
    """Создать (если нет) таблицу журнала загрузки DDS с партиционированием.

    Партиционирование по ``day(snapshot_day)`` и ``source_table_fqn``
    для быстрой выборки логов конкретной таблицы за день.

    Args:
        dst_cur: Trino-курсор.
        catalog: Каталог Iceberg.
        dds_schema: Схема DDS.

    Returns:
        Полное имя таблицы ``catalog.dds_schema.dds_table_load_log``.
    """
    table_ref = quote_table(catalog, dds_schema, "dds_table_load_log")
    dst_cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_ref} (
            source_table_fqn VARCHAR,
            snapshot_day DATE,
            dds_table VARCHAR,
            source_mode VARCHAR,
            rows_loaded BIGINT,
            dq_status VARCHAR,
            dq_note VARCHAR,
            loaded_at TIMESTAMP(6) WITH TIME ZONE NOT NULL
        )
        WITH (
            partitioning = ARRAY['day(snapshot_day)', 'source_table_fqn']
        )
        """
    )
    return table_ref


def write_dds_service_state(dst_cur, table_ref: str, loader_name: str, cutoff_day: date) -> None:
    """MERGE/UPSERT состояние загрузки в служебную таблицу.

    Одна строка на ``(loader_name)`` — обновляет ``snapshot_day``
    до текущего ``cutoff_day`` при каждом успешном прогоне.

    Args:
        dst_cur: Trino-курсор.
        table_ref: Полное имя служебной таблицы.
        loader_name: Имя загрузчика.
        cutoff_day: Дата среза (обновляется в строке).
    """
    dst_cur.execute(
        f"""
        MERGE INTO {table_ref} t
        USING (
            SELECT
                {sql_string(loader_name)} AS loader_name,
                {sql_date(cutoff_day)} AS snapshot_day,
                {sql_timestamptz(datetime.now(timezone.utc))} AS updated_at
        ) s
        ON t.loader_name = s.loader_name
        WHEN MATCHED THEN
            UPDATE SET
                snapshot_day = s.snapshot_day,
                updated_at = s.updated_at
        WHEN NOT MATCHED THEN
            INSERT (loader_name, snapshot_day, updated_at)
            VALUES (s.loader_name, s.snapshot_day, s.updated_at)
        """
    )


# Строка в журнале загрузки: upsert по (snapshot_day, source_table_fqn); один MERGE + ретрай Iceberg (параллельные dds__*).
def merge_table_load_log_row(
    dst_cur,
    log_ref: str,
    *,
    source_fqn: str,
    snapshot_day: date,
    dds_table: str,
    source_mode: str,
    rows_loaded: int,
    dq_status: str,
    dq_note: str,
) -> None:
    """Записать/обновить строку журнала загрузки таблицы DDS.

    UPSERT по составному ключу ``(snapshot_day, source_table_fqn)``.
    Повторный вызов для той же таблицы за тот же день обновляет существующую
    строку, а не добавляет дубликат.

    Использует MERGE с Iceberg-ретраем, так как параллельно могут
    писаться другие dds_*-таблицы.

    Args:
        dst_cur: Trino-курсор.
        log_ref: Полное имя таблицы лога.
        source_fqn: Полное имя исходной таблицы (например ``public.orders``).
        snapshot_day: Дата среза.
        dds_table: Логическое имя таблицы DDS.
        source_mode: Режим источника (``incremental`` / ``full``).
        rows_loaded: Количество загруженных строк.
        dq_status: Статус DQ-проверок (``OK``, ``WARN`` и т.п.).
        dq_note: Свободный текст примечания.
    """
    loaded_at = datetime.now(timezone.utc)
    merge_sql = f"""
        MERGE INTO {log_ref} t
        USING (
            SELECT
                {sql_string(source_fqn)} AS source_table_fqn,
                {sql_date(snapshot_day)} AS snapshot_day,
                {sql_string(dds_table)} AS dds_table,
                {sql_string(source_mode)} AS source_mode,
                {sql_bigint(rows_loaded)} AS rows_loaded,
                {sql_string(dq_status)} AS dq_status,
                {sql_string(dq_note)} AS dq_note,
                {sql_timestamptz(loaded_at)} AS loaded_at
        ) s
        ON t.snapshot_day = s.snapshot_day
           AND t.source_table_fqn = s.source_table_fqn
        WHEN MATCHED THEN
            UPDATE SET
                dds_table = s.dds_table,
                source_mode = s.source_mode,
                rows_loaded = s.rows_loaded,
                dq_status = s.dq_status,
                dq_note = s.dq_note,
                loaded_at = s.loaded_at
        WHEN NOT MATCHED THEN
            INSERT (
                source_table_fqn, snapshot_day, dds_table, source_mode,
                rows_loaded, dq_status, dq_note, loaded_at
            )
            VALUES (
                s.source_table_fqn, s.snapshot_day, s.dds_table, s.source_mode,
                s.rows_loaded, s.dq_status, s.dq_note, s.loaded_at
            )
        """
    execute_trino_write_with_iceberg_retry(
        dst_cur,
        merge_sql,
        target_ref=log_ref,
        op="merge_dds_table_load_log",
        attempts=ICEBERG_HOTSPOT_WRITE_RETRY_ATTEMPTS,
    )


# Манифест пакетного initial (dds_initial); ежедневные dds_jobs обычно передают --skip-manifest.


def ensure_manifest_table(dst_cur, catalog: str, dds_schema: str) -> str:
    """Создать (если нет) таблицу манифеста первичной загрузки.

    Используется только при пакетном dds_initial — хранит сводку
    по каждой таблице и дню среза.

    Args:
        dst_cur: Trino-курсор.
        catalog: Каталог Iceberg.
        dds_schema: Схема DDS.

    Returns:
        Полное имя таблицы ``catalog.dds_schema.dds_initial_manifest``.
    """
    manifest_ref = quote_table(catalog, dds_schema, "dds_initial_manifest")
    dst_cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {manifest_ref} (
            snapshot_day DATE,
            dds_table VARCHAR,
            source_mode VARCHAR,
            rows_loaded BIGINT,
            loaded_at TIMESTAMP(6) WITH TIME ZONE
        )
        """
    )
    return manifest_ref


def write_manifest_rows(dst_cur, manifest_ref: str, cutoff_day: date, stats: list[BuiltTableStats]) -> None:
    """Записать статистику сборки в манифест (DELETE + INSERT).

    Удаляет все строки за ``cutoff_day`` и вставляет свежий набор
    из аккумулированного списка ``BuiltTableStats``.

    Args:
        dst_cur: Trino-курсор.
        manifest_ref: Полное имя таблицы манифеста.
        cutoff_day: Дата среза.
        stats: Список накопленной статистики по таблицам.
    """
    if not stats:
        return
    values_sql = ", ".join(
        (
            f"({sql_date(cutoff_day)}, {sql_string(item.name)}, {sql_string(item.source_mode)}, "
            f"{item.rows}, current_timestamp)"
        )
        for item in stats
    )
    dst_cur.execute(f"DELETE FROM {manifest_ref} WHERE snapshot_day = {sql_date(cutoff_day)}")
    dst_cur.execute(
        f"""
        INSERT INTO {manifest_ref} (snapshot_day, dds_table, source_mode, rows_loaded, loaded_at)
        VALUES {values_sql}
        """
    )


def append_manifest_merge_one_day(
    dst_cur,
    manifest_ref: str,
    cutoff_day: date,
    merged_stats: list[BuiltTableStats],
) -> None:
    """Rebuild manifest rows for snapshot_day from accumulated stats list (bulk replace same day)."""
    write_manifest_rows(dst_cur, manifest_ref, cutoff_day, merged_stats)


# ===========================================================================
# Построение SELECT для DDS — incremental и full_snapshot
# ===========================================================================


def build_incremental_dds_sql(
    catalog: str,
    raw_schema: str,
    source_table: str,
    cutoff_day: date,
) -> str:
    """Режим incremental: прямая выборка из RAW state-таблицы + служебные колонки среза.

    Для каждой строки зеркала добавляет:
      - ``dds_snapshot_day`` — дата среза
      - ``dds_loaded_at`` — момент загрузки

    Args:
        catalog: Каталог Iceberg.
        raw_schema: Схема RAW-слоя.
        source_table: Короткое имя таблицы-источника (без схемы).
        cutoff_day: Дата среза.

    Returns:
        SQL SELECT с полным набором бизнес-колонок + мета-колонки DDS.
    """
    src_ref = quote_table(catalog, raw_schema, source_table)
    return (
        f"SELECT src.*, {sql_date(cutoff_day)} AS dds_snapshot_day, current_timestamp AS dds_loaded_at "
        f"FROM {src_ref} src"
    )


def build_full_snapshot_dds_sql(
    catalog: str,
    raw_schema: str,
    source_full_name: str,
    cutoff_day: date,
) -> str:
    """Режим full: последняя строка снимка по PK на cutoff_day (см. RAW snapshot).

    Использует оконную функцию ``row_number()`` по ``(source_pkey, snapshot_day DESC, extracted_at DESC)``
    — выбирает наиболее свежую версию строки на дату среза.

    Args:
        catalog: Каталог Iceberg.
        raw_schema: Схема RAW-слоя.
        source_full_name: Полное имя источника (public.orders).
        cutoff_day: Дата среза.

    Returns:
        SQL с CTE ``ranked`` и выборкой ``WHERE rn = 1``.
    """
    snapshot_table = target_table_name(source_full_name, "snapshot")
    snapshot_ref = quote_table(catalog, raw_schema, snapshot_table)
    return f"""
    WITH ranked AS (
        SELECT
            source_pkey,
            payload,
            snapshot_day,
            extracted_at,
            row_number() OVER (
                PARTITION BY source_pkey
                ORDER BY snapshot_day DESC, extracted_at DESC
            ) AS rn
        FROM {snapshot_ref}
        WHERE snapshot_day <= {sql_date(cutoff_day)}
    )
    SELECT
        source_pkey,
        payload,
        snapshot_day AS raw_snapshot_day,
        extracted_at AS raw_extracted_at,
        {sql_date(cutoff_day)} AS dds_snapshot_day,
        current_timestamp AS dds_loaded_at
    FROM ranked
    WHERE rn = 1
    """


# ===========================================================================
# Incremental: предикаты, PK-конкатенация, раскладка колонок
# ===========================================================================


def events_source_table_predicate_sql(event_alias: str, table_fqn: str) -> str:
    """Фильтр global_log для incremental: полное имя или короткий table.

    В ``*__events`` колонка ``source_table`` может храниться как
    ``public.orders`` или просто ``orders`` — фильтр проверяет оба варианта.

    Args:
        event_alias: Алиас таблицы событий (например ``e``).
        table_fqn: Полное qualified-имя (``public.orders``).

    Returns:
        Условие WHERE ``lower(source_table) IN (...)``.
    """
    _, short = split_table_name(table_fqn)
    full_l = event_alias + ".source_table"
    return (
        f"(lower({full_l}) = lower({sql_string(table_fqn)}) "
        f"OR lower({full_l}) = lower({sql_string(short)}))"
    )


def mirror_pk_concat_sql(alias: str, pk_columns: list[str]) -> str:
    """Строковый ключ как в журнале: concat через '-' с пустым вместо NULL.

    Используется для сопоставления ``source_pkey`` из ``*__events`` (строка)
    с составным первичным ключом зеркала (несколько колонок).

    Согласование с Postgres ``::text`` для PK: NULL-колонки дают ``''``
    вместо разрыва конкатенации.

    Args:
        alias: Алиас таблицы.
        pk_columns: Список имён колонок первичного ключа.

    Returns:
        SQL-выражение ``concat_ws('-', COALESCE(CAST(...)...)...)``.
    """
    parts = ", ".join(
        [f"COALESCE(CAST({alias}.{quote_ident(column)} AS VARCHAR), '')" for column in pk_columns]
    )
    return f"concat_ws('-', {parts})"


def fetch_incremental_dds_mirror_layout(dsn: str, table_fqn: str) -> tuple[list[str], list[str]]:
    """Получить первичный ключ и имена всех колонок зеркала из PG-источника.

    Для incremental DDS необходимо знать PK (для MERGE/DELETE по ключу)
    и полный набор колонок (бизнес-колонки + lineage ``_raw_load_source``,
    ``_raw_loaded_at``).

    Args:
        dsn: DSN подключения к Postgres-источнику.
        table_fqn: Полное имя таблицы (``public.orders``).

    Returns:
        Кортеж ``(primary_key: list[str], data_names: list[str])``.

    Raises:
        ValueError: Если у таблицы в Postgres нет первичного ключа.
    """
    source_schema, source_table = split_table_name(table_fqn)
    with connect_source(dsn) as pg_conn:
        with pg_conn.cursor() as pg_cur:
            cols = fetch_source_columns(pg_cur, source_schema=source_schema, source_table=source_table)
            primary_key = fetch_source_primary_key(pg_cur, source_schema=source_schema, source_table=source_table)
            if not primary_key:
                raise ValueError(f"Incremental DDS requires primary key at source for {table_fqn}")
            data_names = [c.name for c in cols] + [RAW_LOAD_SOURCE_COL, RAW_LOADED_AT_COL]
            return primary_key, data_names


# ===========================================================================
# Основная функция incremental DDS: MERGE + DELETE из *__events
# ===========================================================================


def run_incremental_dds_delta(
    dst_cur,
    *,
    pg_dsn: str,
    catalog: str,
    raw_schema: str,
    table_fqn: str,
    cutoff_day: date,
    day_tz: str,
    target_ref: str,
    dry_run: bool,
) -> tuple[int, int]:
    """MERGE затронутых строк из RAW-зеркала + DELETE по op=DELETE из __events за суточное окно TZ.

    Алгоритм:
      1. Построить суточное окно ``[day_start, day_end)`` в указанной TZ.
      2. Выбрать из ``*__events`` все PK с ``op IN ('INSERT', 'UPDATE')`` за окно.
      3. Соединить с RAW-зеркалом — получить актуальные строки для MERGE.
      4. Выбрать из ``*__events`` все PK с ``op = 'DELETE'`` за окно.
      5. Выполнить DELETE из целевой таблицы по выбранным ключам.
      6. Выполнить MERGE (UPSERT) дельты в целевую таблицу.

    Возвращает ``(rows_merge_source, distinct_delete_pkey_count)`` для журнала.

    Args:
        dst_cur: Trino-курсор.
        pg_dsn: DSN к Postgres-источнику (для чтения PK/колонок зеркала).
        catalog: Каталог Iceberg.
        raw_schema: Схема RAW-слоя.
        table_fqn: Полное имя исходной таблицы.
        cutoff_day: Дата среза.
        day_tz: Часовой пояс суточного окна (например ``Europe/Moscow``).
        target_ref: Полное qualified-имя целевой DDS-таблицы.
        dry_run: True — только подсчёт, без записи.

    Returns:
        Кортеж ``(rows_merge_source: int, distinct_delete_pkey_count: int)``.
    """
    # Суточное окно в локальной TZ
    _, day_start, day_end = build_day_window(cutoff_day.isoformat(), day_tz)
    primary_key, column_names = fetch_incremental_dds_mirror_layout(pg_dsn, table_fqn)

    # Разделение колонок на ключевые и не-ключевые
    pk_set = set(primary_key)
    non_pk_cols = [name for name in column_names if name not in pk_set]
    dds_meta_cols = ("dds_snapshot_day", "dds_loaded_at")
    all_target_cols = [*column_names, *dds_meta_cols]

    # MERGE-условие: совпадение по PK
    merge_on = " AND ".join(f"t.{quote_ident(c)} = d.{quote_ident(c)}" for c in primary_key)
    update_clause = ", ".join(
        f"{quote_ident(name)} = d.{quote_ident(name)}" for name in [*non_pk_cols, *dds_meta_cols]
    )
    insert_cols_sql = ", ".join(quote_ident(c) for c in all_target_cols)
    insert_vals_sql = ", ".join(f"d.{quote_ident(c)}" for c in all_target_cols)

    # Ссылки на RAW-таблицы
    _, short_mirror = split_table_name(table_fqn)
    mirror_ref = quote_table(catalog, raw_schema, short_mirror)
    events_tbl = target_table_name(table_fqn, "events")
    events_ref = quote_table(catalog, raw_schema, events_tbl)

    # WHERE для отбора событий за окно
    ev_where = (
        f"e.changed_at >= {sql_timestamptz(day_start)} "
        f"AND e.changed_at < {sql_timestamptz(day_end)} "
        f"AND {events_source_table_predicate_sql('e', table_fqn)} "
        "AND CAST(e.source_pkey AS VARCHAR) IS NOT NULL "
        "AND CAST(TRIM(CAST(e.source_pkey AS VARCHAR)) AS VARCHAR) <> ''"
    )

    # Дельта для MERGE: строки зеркала, чьи PK были затронуты INSERT/UPDATE
    pk_expr_mirror = mirror_pk_concat_sql("src", primary_key)
    delta_sql = (
        "SELECT "
        f"src.*, {sql_date(cutoff_day)} AS {quote_ident('dds_snapshot_day')}, "
        f"CAST(current_timestamp AS TIMESTAMP(6) WITH TIME ZONE) AS {quote_ident('dds_loaded_at')} "
        f"FROM {mirror_ref} src "
        "INNER JOIN ( "
        "    SELECT DISTINCT CAST(e.source_pkey AS VARCHAR) AS pk_str "
        f"FROM {events_ref} e WHERE {ev_where} "
        "    AND upper(e.op) IN ('INSERT', 'UPDATE') "
        ") touches ON CAST(touches.pk_str AS VARCHAR) = "
        f"{pk_expr_mirror}"
    )

    # WHERE для DELETE: события с op='DELETE' за окно
    delete_where = (
        f"e.changed_at >= {sql_timestamptz(day_start)} "
        f"AND e.changed_at < {sql_timestamptz(day_end)} "
        f"AND {events_source_table_predicate_sql('e', table_fqn)} "
        "AND upper(e.op) = 'DELETE' "
        "AND CAST(e.source_pkey AS VARCHAR) IS NOT NULL "
        "AND CAST(TRIM(CAST(e.source_pkey AS VARCHAR)) AS VARCHAR) <> ''"
    )

    pk_expr_target = mirror_pk_concat_sql("t", primary_key)

    # Подсчёт строк MERGE-дельты
    cnt_merge_sql = f"SELECT CAST(count(*) AS BIGINT) FROM ({delta_sql}) dds_delta"
    dst_cur.execute(cnt_merge_sql)
    rows_merge = int(dst_cur.fetchone()[0])

    # Подсчёт уникальных удаляемых ключей
    cnt_delete_sql = (
        "SELECT CAST(count(*) AS BIGINT) FROM ( "
        "SELECT DISTINCT CAST(e.source_pkey AS VARCHAR) pk "
        f"FROM {events_ref} e WHERE {delete_where}"
        ") x"
    )
    dst_cur.execute(cnt_delete_sql)
    distinct_delete_keys = int(dst_cur.fetchone()[0])

    if dry_run:
        LOGGER.info(
            "[INFO] dds_incremental dry-run: merge_source_rows=%s delete_distinct_pkey=%s",
            rows_merge,
            distinct_delete_keys,
        )
        return rows_merge, distinct_delete_keys

    # Шаг 1: удалить строки с op=DELETE
    delete_sql = (
        f"DELETE FROM {target_ref} t WHERE ({pk_expr_target}) IN "
        "("
        "    SELECT CAST(e.source_pkey AS VARCHAR) pk "
        f"FROM {events_ref} e WHERE {delete_where} "
        ")"
    )
    execute_trino_write_with_iceberg_retry(
        dst_cur,
        delete_sql,
        target_ref=target_ref,
        op="dds_incremental_delete",
    )

    # Шаг 2: MERGE (UPSERT) дельты
    merge_sql = (
        "MERGE INTO "
        + f"{target_ref} t USING ({delta_sql}) d ON ({merge_on}) "
        + "WHEN MATCHED THEN UPDATE SET "
        + update_clause
        + " WHEN NOT MATCHED THEN INSERT ("
        + insert_cols_sql
        + ") VALUES ("
        + insert_vals_sql
        + ")"
    )
    execute_trino_write_with_iceberg_retry(
        dst_cur,
        merge_sql,
        target_ref=target_ref,
        op="dds_incremental_merge",
    )
    LOGGER.info(
        "[INFO] dds_incremental applied: merge_source_rows=%s delete_distinct_pkey=%s window=[%s..%s)",
        rows_merge,
        distinct_delete_keys,
        day_start.isoformat(),
        day_end.isoformat(),
    )
    return rows_merge, distinct_delete_keys


def replace_table(dst_cur, target_ref: str, select_sql: str, dry_run: bool) -> int:
    """Пересоздание целевой таблицы DDS через DROP + CREATE AS.

    Используется для full-режима и для начальной загрузки incremental
    (когда целевая таблица ещё не существует).

    Args:
        dst_cur: Trino-курсор.
        target_ref: Полное имя целевой таблицы.
        select_sql: SELECT, формирующий содержимое таблицы.
        dry_run: True — только подсчёт, без DROP/CREATE.

    Returns:
        Число строк, которые были бы/были загружены.
    """
    count_sql = f"SELECT count(*) FROM ({select_sql}) t"
    dst_cur.execute(count_sql)
    rows = int(dst_cur.fetchone()[0])
    if dry_run:
        return rows
    dst_cur.execute(f"DROP TABLE IF EXISTS {target_ref}")
    dst_cur.execute(f"CREATE TABLE {target_ref} AS {select_sql}")
    return rows


# ===========================================================================
# Sidecar SQL: validate и clean (пути и выполнение)
# ===========================================================================


def validate_sql_paths(source_fqn: str) -> tuple[Path | None, Path | None]:
    """Найти файлы sidecar-SQL для проверки и очистки.

    Ищет в ``dds_validate/`` и ``dds_clean/`` файлы с именем,
    полученным заменой ``.`` на ``__``: ``public.orders`` → ``public__orders.sql``.

    Args:
        source_fqn: Полное qualified-имя исходной таблицы.

    Returns:
        Кортеж ``(validate_path | None, clean_path | None)``.
    """
    safe = source_fqn.replace(".", "__")
    v = DDS_VALIDATE_DIR / f"{safe}.sql"
    c = DDS_CLEAN_DIR / f"{safe}.sql"
    return (v if v.is_file() else None, c if c.is_file() else None)


def run_validate_and_clean(
    dst_cur,
    *,
    validate_path: Path | None,
    clean_path: Path | None,
    skip_validate_sql: bool,
    skip_clean_sql: bool,
) -> tuple[str, str]:
    """Выполнить sidecar-SQL: сначала clean (правки), затем validate (проверки).

    Порядок важен: clean меняет данные до того, как validate проверит
    их целостность. validate обычно содержит только SELECT/ASSERT
    и не меняет данные.

    Args:
        dst_cur: Trino-курсор.
        validate_path: Путь к SQL-файлу проверки или None.
        clean_path: Путь к SQL-файлу очистки или None.
        skip_validate_sql: True — пропустить validate даже при наличии файла.
        skip_clean_sql: True — пропустить clean даже при наличии файла.

    Returns:
        Кортеж ``(dq_status: str, dq_note: str)``.
    """
    notes: list[str] = []

    if not skip_clean_sql and clean_path:
        LOGGER.info("[INFO] dds_clean: executing %s", clean_path.name)
        clean_sql = clean_path.read_text(encoding="utf-8")
        for stmt in _split_sql_statements(clean_sql):
            dst_cur.execute(stmt)

    if not skip_validate_sql and validate_path:
        LOGGER.info("[INFO] dds_validate: executing %s", validate_path.name)
        validate_sql = validate_path.read_text(encoding="utf-8")
        for stmt in _split_sql_statements(validate_sql):
            dst_cur.execute(stmt)
        notes.append(f"validated:{validate_path.name}")

    dq_status = "OK" if not notes else ";".join(notes)
    dq_note = ";".join(notes) if notes else "no_sidecar_sql_or_ok"
    return dq_status, dq_note


def _split_sql_statements(sql_text: str) -> list[str]:
    """Разбить SQL-текст на отдельные операторы по ``;``.

    Игнорирует строки, целиком состоящие из комментариев ``--``,
    а также пустые строки после split.

    Args:
        sql_text: Текст SQL-файла.

    Returns:
        Список SQL-операторов (без завершающей ``;``).
    """
    parts = [p.strip() for p in sql_text.split(";") if p.strip() and not p.strip().startswith("--")]
    return parts


def coarse_raw_row_count(dst_cur, catalog: str, raw_schema: str, table: Any) -> int:
    """Грубый count(*) по RAW-зеркалу: state для incremental, snapshot для full.

    Используется для pre-count в логе перед загрузкой — даёт понимание
    объёма источника без детального анализа.

    Args:
        dst_cur: Trino-курсор.
        catalog: Каталог Iceberg.
        raw_schema: Схема RAW-слоя.
        table: Объект таблицы из конфига (имеет .name и .mode).

    Returns:
        Количество строк в RAW-зеркале или снимке.
    """
    _, source_table = split_table_name(table.name)
    if table.mode == "incremental":
        ref = quote_table(catalog, raw_schema, source_table)
    else:
        snap = target_table_name(table.name, "snapshot")
        ref = quote_table(catalog, raw_schema, snap)
    dst_cur.execute(f"SELECT count(*) FROM {ref}")
    return int(dst_cur.fetchone()[0])


# ===========================================================================
# CLI-аргументы и точка входа
# ===========================================================================


def parse_args(implicit_default_table: str | None = None) -> argparse.Namespace:
    """Разобрать аргументы командной строки для загрузки одной таблицы.

    Аргументы:

    * ``--table`` — имя источника из конфига, например ``public.orders``
      или просто ``orders`` (схема по умолчанию ``public``).
    * ``--config`` — путь к JSON-файлу конфигурации
      (по умолчанию ``/opt/airflow/scripts/raw_load_config.json``).
    * ``--cutoff-day`` — дата исторического среза в формате ``YYYY-MM-DD``
      (по умолчанию ``2023-12-31``). Записывается в ``dds_snapshot_day``.
    * ``--log-level`` — уровень логирования (по умолчанию ``INFO``,
      из переменной окружения ``RAW_LOADER_LOG_LEVEL``).
    * ``--dry-run`` — только подсчёт строк, без DROP/CREATE в DDS.
    * ``--skip-pre-count`` — не логировать ``count(*)`` RAW-источника.
    * ``--write-service-state`` — обновить ``dds_load_service_state``
      до ``cutoff_day`` (обычно только для пакетного initial).
    * ``--skip-manifest`` — не трогать ``dds_initial_manifest``
      (ежедневные задачи всегда передают этот флаг).
    * ``--skip-load-log`` — не писать строку в ``dds_table_load_log``.
    * ``--skip-validate-sql`` — не выполнять ``dds_validate/<table>.sql``.
    * ``--skip-clean-sql`` — не выполнять ``dds_clean/<table>.sql``.
    * ``--dds-full-rebuild`` — для incremental-таблиц: принудительный
      DROP+CREATE по полному RAW-зеркалу вместо дельты.
    * ``--dds-day-tz`` — часовой пояс суточного окна для
      dds_incremental (календарный день ``--cutoff-day``),
      по умолчанию ``Europe/Moscow``.
    * ``--dds-legacy-flat`` — игнорировать ``dds.scd2`` и писать
      плоские Iceberg-таблицы (старое поведение до SCD2+VIEW).

    Args:
        implicit_default_table: Имя таблицы по умолчанию, если
            ``--table`` не передан явно.

    Returns:
        Namespace с разобранными аргументами.
    """
    parser = argparse.ArgumentParser(description="DQ/clean/load one RAW table → DDS Iceberg tables")
    parser.add_argument(
        "--table",
        default=implicit_default_table,
        required=implicit_default_table is None,
        help="Имя источника из config, например public.orders или orders",
    )
    parser.add_argument(
        "--config",
        default="/opt/airflow/scripts/raw_load_config.json",
        help="Path to JSON config",
    )
    parser.add_argument(
        "--cutoff-day",
        default="2023-12-31",
        help="Historical/snapshot cutoff YYYY-MM-DD (dds_snapshot_day metadata)",
    )
    parser.add_argument(
        "--log-level",
        default=os.getenv("RAW_LOADER_LOG_LEVEL", "INFO"),
        help="Logging level",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Count target rows only, no DROP/CREATE in DDS",
    )
    parser.add_argument(
        "--skip-pre-count",
        action="store_true",
        help="Не логировать count(*) источника RAW перед загрузкой",
    )
    parser.add_argument(
        "--write-service-state",
        action="store_true",
        help="После загрузки обновить dds.dds_load_service_state до cutoff-day (глобально; для пайплайна по таблицам обычно не нужно)",
    )
    parser.add_argument(
        "--skip-manifest",
        action="store_true",
        help="Не трогать dds.dds_initial_manifest (для пакетного скрипта initial)",
    )
    parser.add_argument(
        "--skip-load-log",
        action="store_true",
        help="Не писать строку в dds.dds_table_load_log",
    )
    parser.add_argument(
        "--skip-validate-sql",
        action="store_true",
        help="Не выполнять dds_validate/<table>.sql если файл есть",
    )
    parser.add_argument(
        "--skip-clean-sql",
        action="store_true",
        help="Не выполнять dds_clean/<table>.sql если файл есть",
    )
    parser.add_argument(
        "--dds-full-rebuild",
        action="store_true",
        help="Incremental таблицы: принудительно DROP+CREATE по полному RAW-зеркалу (старое поведение).",
    )
    parser.add_argument(
        "--dds-day-tz",
        default=os.getenv("DDS_DAY_TZ") or os.getenv("RAW_LOADER_TZ") or "Europe/Moscow",
        help="Timezone суточного окна для dds_incremental (календарный день — --cutoff-day); как у load_raw_day --tz.",
    )
    parser.add_argument(
        "--dds-legacy-flat",
        action="store_true",
        help="Игнорировать dds.scd2 и писать плоские Iceberg-таблицы (старое поведение до SCD2+VIEW).",
    )
    return parser.parse_args()


# ===========================================================================
# Ядро: загрузка одной таблицы из RAW в DDS
# ===========================================================================


def load_one_table(
    *,
    args: argparse.Namespace | None = None,
    implicit_default_table: str | None = None,
) -> BuiltTableStats:
    """Ядро: одна строка конфига таблицы → DDL в DDS.

    Переисользуется как ``load_dds_initial`` (пакетно), так и
    тонкими обёртками ``dds_jobs/*.py`` (по одной таблице).

    Алгоритм:
      1. Разбор аргументов и конфига, определение ``cutoff_day``.
      2. Подключение к Trino, создание схемы DDS при необходимости.
      3. Опциональный ``pre-count`` RAW-источника.
      4. Выполнение sidecar DQ: ``dds_clean`` → ``dds_validate``.
      5. Определение режима загрузки (SCD2 или плоский, incremental или full).
      6. Загрузка данных в DDS (MERGE/INSERT для incremental, DROP+CREATE для full).
      7. Запись в журнал ``dds_table_load_log`` и опционально в ``dds_load_service_state``.

    Args:
        args: Namespace аргументов CLI. Если None, разбираются заново
            с учётом ``implicit_default_table``.
        implicit_default_table: Имя таблицы по умолчанию для ``--table``.

    Returns:
        ``BuiltTableStats`` с именем DDS-таблицы, числом строк и режимом.

    Raises:
        ValueError: Если таблица не найдена в конфиге.
    """
    # Разбор аргументов и конфигурации
    if args is None:
        args = parse_args(implicit_default_table=implicit_default_table)
    elif implicit_default_table and args.table is None:
        args.table = implicit_default_table

    setup_logging(args.log_level)
    cutoff_day = date.fromisoformat(args.cutoff_day)

    config = load_config(args.config)
    target_cfg = config.get("target", {})
    source_cfg = config["source"]
    source_default_schema = str(source_cfg.get("default_schema", "public"))
    raw_schema_default = str(target_cfg.get("default_schema", "raw"))
    dds_schema = dds_schema_name(config)

    # Поиск конфига таблицы
    normalized = normalize_table_arg(str(args.table), source_default_schema)
    table_configs = parse_table_configs(config["tables"], default_source_schema=source_default_schema)
    table_cfg = find_table_cfg(table_configs, normalized)

    trino_target = build_trino_target_config(target_cfg, fallback_schema=raw_schema_default)
    raw_schema = trino_target.schema

    # Пути к sidecar-SQL
    validate_path, clean_path = validate_sql_paths(table_cfg.name)
    dq_status_precheck = "PENDING"

    LOGGER.info(
        "[INFO] dds_one_table start: source=%s mode=%s cutoff=%s raw=%s.%s dry_run=%s",
        table_cfg.name,
        table_cfg.mode,
        cutoff_day.isoformat(),
        trino_target.catalog,
        raw_schema,
        args.dry_run,
    )

    with connect_trino(trino_target) as dst_conn:
        with dst_conn.cursor() as dst_cur:
            # Проверка соединения с Trino
            dst_cur.execute(trino_target.verify_connection_sql)
            _ = dst_cur.fetchone()

            # Создать схему DDS при необходимости
            dst_cur.execute(f"CREATE SCHEMA IF NOT EXISTS {quote_schema(trino_target.catalog, dds_schema)}")

            # Опциональный pre-count RAW-источника для диагностики
            if not getattr(args, "skip_pre_count", False):
                try:
                    n_src = coarse_raw_row_count(dst_cur, trino_target.catalog, raw_schema, table_cfg)
                    LOGGER.info("[INFO] dq_pre_count raw_mirror_or_snapshot rows=%s table=%s", n_src, table_cfg.name)
                except Exception as exc:  # noqa: BLE001
                    dq_status_precheck = f"WARN:pre_count_failed:{exc}"
                    LOGGER.warning("[WARN] pre_count_failed table=%s: %s", table_cfg.name, exc)

            # DQ sidecar: правки и проверки до пересборки DDS.
            dq_status, dq_note = run_validate_and_clean(
                dst_cur,
                validate_path=validate_path,
                clean_path=clean_path,
                skip_validate_sql=getattr(args, "skip_validate_sql", False),
                skip_clean_sql=getattr(args, "skip_clean_sql", False),
            )
            if dq_status_precheck.startswith("WARN"):
                dq_status = "WARN"
                dq_note = dq_status_precheck + ";" + dq_note

            # ================================================================
            # Определение стратегии загрузки: SCD2 или плоская, incr. или full
            # ================================================================

            _, source_table = split_table_name(table_cfg.name)
            dds_fragment = dds_cfg(config)
            legacy_flat = bool(getattr(args, "dds_legacy_flat", False))
            use_scd2 = bool(dds_scd2.dds_scd2_applies(dds_fragment, table_name=table_cfg.name) and not legacy_flat)

            if table_cfg.mode == "incremental":
                logical_dds_name = f"dds_{source_table}"
                select_sql = build_incremental_dds_sql(
                    catalog=trino_target.catalog,
                    raw_schema=raw_schema,
                    source_table=source_table,
                    cutoff_day=cutoff_day,
                )
            else:
                logical_dds_name = f"dds_{source_table}_snapshot"
                select_sql = build_full_snapshot_dds_sql(
                    catalog=trino_target.catalog,
                    raw_schema=raw_schema,
                    source_full_name=table_cfg.name,
                    cutoff_day=cutoff_day,
                )

            logical_ref = quote_table(trino_target.catalog, dds_schema, logical_dds_name)

            # --- Ветка SCD2 ---
            if use_scd2:
                day_tz_scd = str(getattr(args, "dds_day_tz", "") or "Europe/Moscow").strip()
                pg_dsn_scd = build_pg_dsn(source_cfg)
                force_full_rebuild = bool(getattr(args, "dds_full_rebuild", False))
                physical_before: bool | None = None
                physical_table_nm: str

                # Определение имени физической SCD2-таблицы
                if table_cfg.mode == "incremental":
                    physical_table_nm = dds_scd2.scd2_incremental_physical_name(source_table)
                    physical_before = dds_scd2.iceberg_base_table_exists(
                        dst_cur,
                        catalog=trino_target.catalog,
                        schema=dds_schema,
                        table=physical_table_nm,
                    )
                else:
                    physical_table_nm = dds_scd2.scd2_full_physical_name(source_table)
                    physical_before = dds_scd2.iceberg_base_table_exists(
                        dst_cur,
                        catalog=trino_target.catalog,
                        schema=dds_schema,
                        table=physical_table_nm,
                    )

                # При первом запуске (bootstrap) — очистка VIEW и возможного мусора
                if not physical_before and not args.dry_run:
                    dds_scd2.drop_view_best_effort(
                        dst_cur,
                        catalog=trino_target.catalog,
                        dds_schema=dds_schema,
                        view_name=logical_dds_name,
                    )
                    dst_cur.execute(f"DROP TABLE IF EXISTS {logical_ref}")

                if table_cfg.mode == "incremental":
                    rows = dds_scd2.run_incremental_scd2(
                        dst_cur,
                        pg_dsn=pg_dsn_scd,
                        catalog=trino_target.catalog,
                        raw_schema=raw_schema,
                        dds_schema=dds_schema,
                        physical_table=physical_table_nm,
                        logical_view_name=logical_dds_name,
                        table_fqn=table_cfg.name,
                        cutoff_day=cutoff_day,
                        day_tz=day_tz_scd,
                        dry_run=args.dry_run,
                        force_rebuild=force_full_rebuild,
                    )
                    dq_note = dq_note + f";dds_scd2=incremental;phys={physical_table_nm};tz={day_tz_scd}"
                else:
                    rows = dds_scd2.run_full_snapshot_scd2(
                        dst_cur,
                        catalog=trino_target.catalog,
                        raw_schema=raw_schema,
                        dds_schema=dds_schema,
                        physical_table=physical_table_nm,
                        logical_view_name=logical_dds_name,
                        source_full_name=table_cfg.name,
                        cutoff_day=cutoff_day,
                        dry_run=args.dry_run,
                        force_rebuild=force_full_rebuild,
                    )
                    dq_note = dq_note + f";dds_scd2=full_snapshot;phys={physical_table_nm}"

                dds_table_name = logical_dds_name
                LOGGER.info(
                    "[INFO] dds_scd2 path table=%s phys=%s rows=%s",
                    dds_table_name,
                    physical_table_nm,
                    rows,
                )

            # --- Ветка incremental плоская (без SCD2) ---
            elif table_cfg.mode == "incremental":
                force_full_rebuild = bool(getattr(args, "dds_full_rebuild", False))
                dds_exists = target_table_exists(dst_cur, dds_schema, logical_dds_name)

                # Если таблица уже существует и не форсирован rebuild — дельта
                use_delta = bool(not force_full_rebuild and dds_exists)
                if use_delta:
                    day_tz = str(getattr(args, "dds_day_tz", "") or "Europe/Moscow").strip()
                    pg_dsn = build_pg_dsn(source_cfg)
                    merge_n, delete_n = run_incremental_dds_delta(
                        dst_cur,
                        pg_dsn=pg_dsn,
                        catalog=trino_target.catalog,
                        raw_schema=raw_schema,
                        table_fqn=table_cfg.name,
                        cutoff_day=cutoff_day,
                        day_tz=day_tz,
                        target_ref=logical_ref,
                        dry_run=args.dry_run,
                    )
                    rows = merge_n + delete_n
                    dq_note = dq_note + f";dds_incremental_delta_tz={day_tz}"
                    LOGGER.info(
                        "[INFO] dds_incremental path: delta_window tz=%s merge_rows=%s delete_distinct_pkeys=%s",
                        day_tz,
                        merge_n,
                        delete_n,
                    )
                else:
                    # Первый запуск или --dds-full-rebuild: полное пересоздание
                    rows = replace_table(
                        dst_cur=dst_cur,
                        target_ref=logical_ref,
                        select_sql=select_sql,
                        dry_run=args.dry_run,
                    )
                    LOGGER.info(
                        "[INFO] dds_incremental path: full_rebuild rebuild=%s dds_exists=%s rows=%s",
                        force_full_rebuild,
                        dds_exists,
                        rows,
                    )
                dds_table_name = logical_dds_name
            else:
                # --- Ветка full (snapshot) плоская ---
                rows = replace_table(dst_cur=dst_cur, target_ref=logical_ref, select_sql=select_sql, dry_run=args.dry_run)
                dds_table_name = logical_dds_name

            stats = BuiltTableStats(name=dds_table_name, rows=rows, source_mode=table_cfg.mode)

            # Запись в служебные таблицы (только при реальном прогоне)
            if not args.dry_run:
                # Журнал загрузки
                if not getattr(args, "skip_load_log", False):
                    log_ref = ensure_dds_table_load_log(dst_cur, trino_target.catalog, dds_schema)
                    merge_table_load_log_row(
                        dst_cur,
                        log_ref,
                        source_fqn=table_cfg.name,
                        snapshot_day=cutoff_day,
                        dds_table=dds_table_name,
                        source_mode=table_cfg.mode,
                        rows_loaded=rows,
                        dq_status=dq_status,
                        dq_note=dq_note,
                    )

                # Состояние загрузки (обычно только для initial)
                if args.write_service_state:
                    service_ref = ensure_dds_service_table(
                        dst_cur,
                        catalog=trino_target.catalog,
                        dds_schema=dds_schema,
                        state_table=dds_service_table_name(config),
                    )
                    write_dds_service_state(
                        dst_cur,
                        table_ref=service_ref,
                        loader_name=dds_loader_name(config),
                        cutoff_day=cutoff_day,
                    )

                commit_fn = getattr(dst_conn, "commit", None)
                if callable(commit_fn):
                    commit_fn()

            LOGGER.info("[INFO] dds_one_table done: %s rows=%s dry_run=%s", dds_table_name, rows, args.dry_run)
            return stats


def main(implicit_default_table: str | None = None) -> None:
    """Точка входа CLI: разбор аргументов → вызов ``load_one_table``.

    Args:
        implicit_default_table: Имя таблицы по умолчанию для ``--table``.
    """
    args = parse_args(implicit_default_table=implicit_default_table)
    load_one_table(args=args)


def cli_for_table(source_fqn: str) -> None:
    """Вызвать загрузку одной таблицы из тонкого скрипта ``dds_jobs/``.

    Принимает ``source_fqn`` (например ``"public.orders"``) и пробрасывает
    все остальные аргументы CLI от вызывающего скрипта (``sys.argv``).

    Пример использования в ``dds_jobs/dds_public_orders.py``::

        from dds_table_runner import cli_for_table
        if __name__ == "__main__":
            cli_for_table("public.orders")

    Args:
        source_fqn: Полное qualified-имя таблицы (``public.orders``).
    """
    import sys

    sys.argv = [sys.argv[0], "--table", source_fqn] + sys.argv[1:]
    main()


if __name__ == "__main__":
    main()
