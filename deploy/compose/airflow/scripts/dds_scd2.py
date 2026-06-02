"""SCD Type 2 для DDS: Iceberg ``*_scd2`` + VIEW прежних имён (``is_current = TRUE``).

Алгоритм Slowly Changing Dimension Type 2 для инкрементальных и full-snapshot таблиц.

Общие принципы SCD2 в этом модуле
----------------------------------

1. **Физическая таблица** ``dds_<entity>_scd2`` хранит все версии строк с техническими
   колонками: ``row_attr_hash``, ``scd_version_id``, ``valid_from``, ``valid_to``,
   ``is_current``, ``scd_row_op``.

2. **Логическое VIEW** ``dds_<entity>`` — выборка ``WHERE is_current = TRUE``,
   что даёт витринам и MART-слою привычное имя без необходимости переписывать запросы.

3. **Версионирование** основано на сравнении ``row_attr_hash`` — хеша всех не-PK
   колонок через ``xxhash64``. Изменение любого атрибута даёт новый хеш → закрытие
   старой версии + открытие новой.

Incremental SCD2 (run_incremental_scd2)
---------------------------------------

Затронутые PK из ``*__events`` за сутки, актуальный атрибутный ряд из RAW-зеркала,
сравнение ``row_attr_hash`` без PK/full row.

Алгоритм дневной дельты:
  1. Из ``*__events`` за суточное окно выбираются все PK с ``op IN ('INSERT', 'UPDATE')``.
  2. Для каждого такого PK сравнивается ``row_attr_hash`` текущей ``is_current``-версии
     в физической SCD2-таблице и хеш строки в RAW-зеркале.
  3. Если хеш изменился (или INSERT нового PK) — старая версия закрывается
     (``valid_to = day_end - 1μs``, ``is_current = FALSE``), вставляется новая.
  4. Если ``op = 'DELETE'`` — текущая версия закрывается без вставки новой.
  5. ``scd_version_id`` — хеш от ``concat(pk, '|', current_timestamp)``,
     уникальный идентификатор версии для MERGE-сопоставления.

При первом запуске (bootstrap) — полный скан RAW-зеркала с ``valid_from = day_start``.

Full snapshot SCD2 (run_full_snapshot_scd2)
-------------------------------------------

Строки снимка за ``snapshot_day = cutoff``, сравнение хеша ``payload``
через ``from_big_endian_64(xxhash64(to_utf8(...)))``.

Алгоритм:
  1. Из ``*_snapshot`` выбираются все строки с ``snapshot_day = cutoff_day``.
  2. Для каждого ``source_pkey`` сравнивается ``row_attr_hash`` (хеш ``payload``)
     с текущей ``is_current`` версией.
  3. Изменённые/новые — аналогично: закрытие + вставка.
  4. Удалённые (PK нет в сегодняшнем снимке) — закрытие.
  5. При bootstrap — ранжирование по ``snapshot_day DESC, extracted_at DESC``,
     выбор последней версии на ``cutoff_day``.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any

from load_raw_day import (
    RAW_LOAD_SOURCE_COL,
    RAW_LOADED_AT_COL,
    SourceColumn,
    build_day_window,
    connect_source,
    execute_trino_write_with_iceberg_retry,
    fetch_source_columns,
    fetch_source_primary_key,
    quote_ident,
    quote_table,
    split_table_name,
    sql_date,
    sql_string,
    sql_timestamptz,
    target_table_name,
)

LOGGER = logging.getLogger(__name__)

# Технические колонки SCD2 — добавляются ко всем бизнес-колонкам в физической таблице.
# row_attr_hash:    хеш не-PK атрибутов для сравнения версий.
# scd_version_id:   уникальный ID версии (хеш PK + timestamp).
# valid_from/valid_to:  период действия версии (NULL = открыт).
# is_current:       TRUE для текущей актуальной версии.
# dds_snapshot_day: дата среза DDS.
# scd_row_op:       метка операции (BOOTSTRAP, UPSERT, CLOSE, SNAPSHOT_DELTA).
# dds_loaded_at:    момент загрузки.
SCD_TECH_COLS = (
    "row_attr_hash BIGINT",
    "scd_version_id BIGINT",
    "valid_from TIMESTAMP(6) WITH TIME ZONE NOT NULL",
    "valid_to TIMESTAMP(6) WITH TIME ZONE",
    "is_current BOOLEAN NOT NULL",
    "dds_snapshot_day DATE",
    "scd_row_op VARCHAR",
    "dds_loaded_at TIMESTAMP(6) WITH TIME ZONE NOT NULL",
)


# ===========================================================================
# Проверка включения SCD2 в конфиге
# ===========================================================================


def dds_cfg_scd2_enabled(dds_fragment: dict[str, Any]) -> bool:
    """Включён ли SCD2 глобально в секции ``dds.scd2`` конфига.

    Args:
        dds_fragment: Словарь секции ``dds`` из конфига.

    Returns:
        True если ``dds.scd2 == true``.
    """
    return bool(dds_fragment.get("scd2", False))


def dds_scd2_applies(dds_fragment: dict[str, Any], *, table_name: str) -> bool:
    """Применять ли SCD2 к конкретной таблице.

    Учитывает:
      - Глобальный флаг ``dds.scd2``.
      - Опциональный whitelist ``dds.scd2_tables_only`` — если задан,
        SCD2 применяется только к таблицам из этого списка.

    Args:
        dds_fragment: Словарь секции ``dds`` из конфига.
        table_name: Полное имя таблицы (например ``public.orders``).

    Returns:
        True если для этой таблицы включён SCD2.
    """
    if not dds_cfg_scd2_enabled(dds_fragment):
        return False
    pilot = dds_fragment.get("scd2_tables_only")
    if pilot is None:
        return True
    pilot_set = {str(x).lower().strip() for x in pilot}
    return table_name.lower().strip() in pilot_set


# ===========================================================================
# Именование физических SCD2-таблиц
# ===========================================================================


def scd2_incremental_physical_name(short_source_table: str) -> str:
    """Имя физической SCD2-таблицы для incremental-источника.

    Args:
        short_source_table: Короткое имя таблицы (без схемы), например ``orders``.

    Returns:
        Имя ``dds_orders_scd2``.
    """
    return f"dds_{short_source_table}_scd2"


def scd2_full_physical_name(short_source_table: str) -> str:
    """Имя физической SCD2-таблицы для full-snapshot источника.

    Args:
        short_source_table: Короткое имя таблицы (без схемы), например ``segments``.

    Returns:
        Имя ``dds_segments_snapshot_scd2``.
    """
    return f"dds_{short_source_table}_snapshot_scd2"


# ===========================================================================
# Проверка существования и создание VIEW
# ===========================================================================


def iceberg_base_table_exists(dst_cur, catalog: str, schema: str, table: str) -> bool:
    """Проверка физической Iceberg-таблицы в текущем каталоге сессии Trino.

    Не фильтруем ``table_catalog`` литералом из конфига: в Trino значение в
    ``information_schema`` может отличаться по регистру от ``TRINO_CATALOG``,
    из‑за чего проверка давала ложный ``False`` и SCD2 каждый день делал
    bootstrap (полный скан RAW-зеркала). Схема+имя + ``BASE TABLE`` достаточно:
    ``connect_trino`` уже привязывает соединение к нужному каталогу.

    Args:
        dst_cur: Trino-курсор.
        catalog: Каталог Iceberg (аргумент сохранён для совместимости;
            фактически каталог задаётся при ``connect``).
        schema: Схема (``dds``).
        table: Имя таблицы.

    Returns:
        True если физическая таблица существует.
    """
    _ = catalog  # каталог задаётся при connect(..., catalog=...); оставляем аргумент для вызовов.
    dst_cur.execute(
        f"""
        SELECT 1
        FROM information_schema.tables
        WHERE lower(table_schema) = lower({sql_string(schema)})
          AND lower(table_name) = lower({sql_string(table)})
          AND table_type = {sql_string("BASE TABLE")}
        LIMIT 1
        """
    )
    return dst_cur.fetchone() is not None


def drop_view_best_effort(dst_cur, catalog: str, dds_schema: str, view_name: str) -> None:
    """Удалить VIEW (если существует), игнорируя ошибки.

    Используется при bootstrap SCD2 для очистки потенциального мусора
    перед созданием физической таблицы и нового VIEW.

    Args:
        dst_cur: Trino-курсор.
        catalog: Каталог Iceberg.
        dds_schema: Схема DDS.
        view_name: Имя VIEW для удаления.
    """
    try:
        dst_cur.execute(f"DROP VIEW IF EXISTS {quote_table(catalog, dds_schema, view_name)}")
    except Exception as exc:  # pragma: no cover
        LOGGER.debug("[DEBUG] drop view %s: %s", view_name, exc)


# ===========================================================================
# Предикаты и раскладка колонок для событийного окна
# ===========================================================================


def events_predicate(event_alias: str, table_fqn: str) -> str:
    """Условие WHERE для фильтрации ``*__events`` по имени исходной таблицы.

    Как и в ``dds_table_runner.events_source_table_predicate_sql``,
    проверяет и полное (``public.orders``) и короткое (``orders``) имя.

    Args:
        event_alias: Алиас таблицы событий (например ``ev``).
        table_fqn: Полное qualified-имя исходной таблицы.

    Returns:
        SQL-условие для WHERE.
    """
    _, short = split_table_name(table_fqn)
    lhs = f"{event_alias}.source_table"
    return (
        f"(lower({lhs}) = lower({sql_string(table_fqn)}) "
        f"OR lower({lhs}) = lower({sql_string(short)}))"
    )


def mirror_pk_concat(alias: str, pk_columns: list[str]) -> str:
    """Конкатенация составного PK в строку через ``concat_ws``.

    Используется для сопоставления ``source_pkey`` из ``*__events``
    (один VARCHAR) с реальным составным ключом таблицы.

    NULL-значения заменяются на ``''`` для стабильности конкатенации.

    Args:
        alias: Алиас таблицы в SQL-запросе.
        pk_columns: Список имён колонок первичного ключа.

    Returns:
        SQL-выражение ``concat_ws('-', ...)``.
    """
    inner = ", ".join(
        f"COALESCE(CAST({alias}.{quote_ident(column)} AS VARCHAR), '')" for column in pk_columns
    )
    return f"concat_ws('-', {inner})"


def _events_window_where(event_alias: str, table_fqn: str, day_start: datetime, day_end: datetime) -> str:
    """Полное WHERE-условие для окна событий: фильтр по времени, таблице, PK.

    Используется и для incremental SCD2, и для DELETE-отбора.

    Args:
        event_alias: Алиас таблицы событий.
        table_fqn: Полное имя исходной таблицы.
        day_start: Начало суточного окна (datetime).
        day_end: Конец суточного окна (исключительно).

    Returns:
        SQL-условие для WHERE.
    """
    return (
        f"{event_alias}.changed_at >= {sql_timestamptz(day_start)} "
        f"AND {event_alias}.changed_at < {sql_timestamptz(day_end)} "
        f"AND {events_predicate(event_alias, table_fqn)} "
        f"AND CAST({event_alias}.source_pkey AS VARCHAR) IS NOT NULL "
        f"AND CAST(TRIM(CAST({event_alias}.source_pkey AS VARCHAR)) AS VARCHAR) <> ''"
    )


def _count_bigint(dst_cur, sql_text: str) -> int:
    """Выполнить COUNT-запрос и вернуть результат как int.

    Вспомогательная функция для dry-run и логирования объёмов дельты.

    Args:
        dst_cur: Trino-курсор.
        sql_text: SQL с SELECT count(*)... .

    Returns:
        Число строк (int).
    """
    dst_cur.execute(sql_text)
    return int(dst_cur.fetchone()[0])


# ===========================================================================
# Получение PK и колонок из Postgres-источника
# ===========================================================================


def fetch_mirror_cols_with_lineage(dsn: str, table_fqn: str) -> tuple[list[str], list[SourceColumn]]:
    """Колонки как в iceberg RAW incremental-зеркале: Postgres + lineage.

    Возвращает первичный ключ (list[str]) и расширенный список колонок
    (бизнес-колонки Postgres + ``_raw_load_source`` + ``_raw_loaded_at``).

    Args:
        dsn: DSN подключения к Postgres.
        table_fqn: Полное имя таблицы (``public.orders``).

    Returns:
        Кортеж ``(primary_key: list[str], cols_extended: list[SourceColumn])``.

    Raises:
        ValueError: Если у таблицы нет первичного ключа.
    """
    source_schema, source_table = split_table_name(table_fqn)
    with connect_source(dsn) as pg_conn:
        with pg_conn.cursor() as pg_cur:
            cols = fetch_source_columns(pg_cur, source_schema=source_schema, source_table=source_table)
            primary_key = fetch_source_primary_key(pg_cur, source_schema=source_schema, source_table=source_table)
            if not primary_key:
                raise ValueError(f"SCD2 incremental requires PK for {table_fqn}")
            cols_extended = cols + [
                SourceColumn(name=RAW_LOAD_SOURCE_COL, trino_type="varchar"),
                SourceColumn(name=RAW_LOADED_AT_COL, trino_type="timestamp(6) with time zone"),
            ]
            return primary_key, cols_extended


# ===========================================================================
# Вычисление row_attr_hash — хеш не-PK атрибутов для сравнения версий
# ===========================================================================


def non_pk_for_hash(primary_key: list[str], cols: list[SourceColumn]) -> list[str]:
    """Список имён не-PK колонок для построения ``row_attr_hash``.

    Исключает колонки первичного ключа из хеширования атрибутов —
    хеш должен меняться только при изменении данных, а не при переносе ключа.

    Args:
        primary_key: Список имён колонок PK.
        cols: Полный список колонок (SourceColumn).

    Returns:
        Список имён не-PK колонок в порядке их следования.
    """
    pk_set = set(primary_key)
    ordered: list[str] = []
    seen: set[str] = set()
    for column in cols:
        if column.name not in pk_set and column.name not in seen:
            ordered.append(column.name)
            seen.add(column.name)
    return ordered


def mirror_attr_hash(alias: str, non_pk_cols: list[str]) -> str:
    """SQL-выражение для вычисления ``row_attr_hash`` строки зеркала.

    Хеш строится как ``from_big_endian_64(xxhash64(to_utf8(concat(a, ', ', b, ...))))``
    — конкатенация всех не-PK колонок через ``, `` и хеширование xxhash64,
    результат приводится к BIGINT для хранения в колонке ``row_attr_hash``.

    Args:
        alias: Алиас таблицы (``m`` для зеркала, ``t_inner`` для физической).
        non_pk_cols: Список имён не-PK колонок.

    Returns:
        SQL-выражение для вычисления хеша.

    Raises:
        ValueError: Если список не-PK колонок пуст.
    """
    if not non_pk_cols:
        raise ValueError("scd2 needs non-PK columns for row hash")
    parts = ", ', ', ".join(f"CAST({alias}.{quote_ident(name)} AS VARCHAR)" for name in non_pk_cols)
    # Trino: xxhash64 даёт VARBINARY; для BIGINT колонки row_attr_hash — from_big_endian_64.
    return f"from_big_endian_64(xxhash64(to_utf8(concat({parts}))))"


# ===========================================================================
# DDL: создание физических SCD2-таблиц
# ===========================================================================


def ddl_incremental_scd2(
    *,
    catalog: str,
    dds_schema: str,
    physical_table: str,
    cols: list[SourceColumn],
    primary_key: list[str],
) -> str:
    """DDL для создания физической SCD2-таблицы incremental-источника.

    Колонки: бизнес-колонки из Postgres + lineage + технические SCD2-колонки.
    Партиционирование: bucket по первому PK-столбцу (или ``id``,
    если он есть в PK). 64 корзины для равномерного распределения.

    Args:
        catalog: Каталог Iceberg.
        dds_schema: Схема DDS.
        physical_table: Имя физической таблицы (``dds_orders_scd2``).
        cols: Список колонок (бизнес + lineage).
        primary_key: Список колонок первичного ключа.

    Returns:
        SQL ``CREATE TABLE IF NOT EXISTS``.
    """
    ref = quote_table(catalog, dds_schema, physical_table)
    col_defs_sql = ",\n".join(
        [f"{quote_ident(c.name)} {c.trino_type}" for c in cols] + list(SCD_TECH_COLS)
    )
    # Выбор колонки для партиционирования: предпочтительно "id", иначе первая из PK
    partition_pk = next((c for c in primary_key if c.lower() == "id"), primary_key[0])
    partition_expr = f"bucket({quote_ident(partition_pk)}, 64)"
    return f"""
CREATE TABLE IF NOT EXISTS {ref} (
{col_defs_sql}
)
WITH (
    partitioning = ARRAY[{sql_string(partition_expr)}]
)
"""


def ddl_full_scd2(*, catalog: str, dds_schema: str, physical_table: str) -> str:
    """DDL для создания физической SCD2-таблицы full-snapshot источника.

    Фиксированный набор колонок: ``source_pkey``, ``payload``,
    ``raw_snapshot_day``, ``raw_extracted_at`` + технические SCD2-колонки.
    Партиционирование: bucket по ``source_pkey`` (32 корзины).

    Args:
        catalog: Каталог Iceberg.
        dds_schema: Схема DDS.
        physical_table: Имя физической таблицы (``dds_segments_snapshot_scd2``).

    Returns:
        SQL ``CREATE TABLE IF NOT EXISTS``.
    """
    ref = quote_table(catalog, dds_schema, physical_table)
    bucket = 'bucket(\"source_pkey\", 32)'
    return f"""
CREATE TABLE IF NOT EXISTS {ref} (
    {quote_ident("source_pkey")} VARCHAR,
    {quote_ident("payload")} VARCHAR,
    {quote_ident("raw_snapshot_day")} DATE,
    {quote_ident("raw_extracted_at")} TIMESTAMP(6) WITH TIME ZONE,
    row_attr_hash BIGINT,
    scd_version_id BIGINT,
    valid_from TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    valid_to TIMESTAMP(6) WITH TIME ZONE,
    is_current BOOLEAN NOT NULL,
    {quote_ident("dds_snapshot_day")} DATE,
    scd_row_op VARCHAR,
    dds_loaded_at TIMESTAMP(6) WITH TIME ZONE NOT NULL
)
WITH (
    partitioning = ARRAY[{sql_string(bucket)}]
)
"""


# ===========================================================================
# Создание логических VIEW (is_current = TRUE)
# ===========================================================================


def recreate_incremental_current_view(
    dst_cur,
    *,
    catalog: str,
    dds_schema: str,
    view_name: str,
    physical_ref: str,
    business_columns: list[str],
    dry_run: bool,
) -> None:
    """Создать/заменить VIEW ``dds_<entity>`` → физическая таблица с ``is_current = TRUE``.

    Для incremental: VIEW содержит все бизнес-колонки + ``dds_snapshot_day``,
    ``dds_loaded_at``. Без технических SCD2-колонок.

    Args:
        dst_cur: Trino-курсор.
        catalog: Каталог Iceberg.
        dds_schema: Схема DDS.
        view_name: Имя VIEW (логическое имя dds_*).
        physical_ref: Полное имя физической таблицы.
        business_columns: Список бизнес-колонок для включения в VIEW.
        dry_run: True — только логирование, без выполнения DDL.
    """
    view_ref = quote_table(catalog, dds_schema, view_name)
    col_list = ", ".join(quote_ident(c) for c in business_columns)
    ddl = (
        f"CREATE OR REPLACE VIEW {view_ref} AS "
        f"SELECT {col_list} FROM {physical_ref} WHERE {quote_ident('is_current')} = TRUE"
    )
    if dry_run:
        LOGGER.info("[INFO] scd2 VIEW (dry-run skip): %s", ddl[:200])
        return
    dst_cur.execute(ddl)


def recreate_full_current_view(
    dst_cur,
    *,
    catalog: str,
    dds_schema: str,
    view_name: str,
    physical_ref: str,
    dry_run: bool,
) -> None:
    """Создать/заменить VIEW для full-snapshot источника с ``is_current = TRUE``.

    Для full: фиксированный набор колонок — ``source_pkey``, ``payload``,
    ``raw_snapshot_day``, ``raw_extracted_at``, ``dds_snapshot_day``, ``dds_loaded_at``.

    Args:
        dst_cur: Trino-курсор.
        catalog: Каталог Iceberg.
        dds_schema: Схема DDS.
        view_name: Имя VIEW.
        physical_ref: Полное имя физической таблицы.
        dry_run: True — только логирование.
    """
    view_ref = quote_table(catalog, dds_schema, view_name)
    cols = ", ".join(
        quote_ident(c)
        for c in (
            "source_pkey",
            "payload",
            "raw_snapshot_day",
            "raw_extracted_at",
            "dds_snapshot_day",
            "dds_loaded_at",
        )
    )
    ddl = (
        f"CREATE OR REPLACE VIEW {view_ref} AS SELECT {cols} FROM {physical_ref} "
        f"WHERE {quote_ident('is_current')} = TRUE"
    )
    if dry_run:
        LOGGER.info("[INFO] scd2 VIEW (dry-run skip): %s", ddl[:200])
        return
    dst_cur.execute(ddl)


# ===========================================================================
# Основная функция: incremental SCD2 (дневная дельта)
# ===========================================================================


def run_incremental_scd2(
    dst_cur,
    *,
    pg_dsn: str,
    catalog: str,
    raw_schema: str,
    dds_schema: str,
    physical_table: str,
    logical_view_name: str,
    table_fqn: str,
    cutoff_day: date,
    day_tz: str,
    dry_run: bool,
    force_rebuild: bool,
) -> int:
    """Incremental SCD2: дельта из ``*__events`` за сутки + сравнение row_attr_hash.

    Алгоритм дневного цикла:

    1. **Bootstrap** (если физической таблицы нет или ``force_rebuild``):
       полный скан RAW-зеркала с ``valid_from = day_start``, все строки
       получают ``is_current = TRUE``, ``scd_row_op = 'BOOTSTRAP'``.

    2. **Дневная дельта** (если таблица уже существует):
       - Из ``*__events`` отбираются PK с ``op IN ('INSERT','UPDATE')`` и ``op = 'DELETE'``.
       - Для UPDATE: сравнивается ``row_attr_hash`` текущей версии и RAW-зеркала.
         При расхождении → ``CLOSE`` старой + ``UPSERT`` новой.
       - Для INSERT (новый PK без текущей версии) → ``UPSERT`` новой.
       - Для DELETE → ``CLOSE`` текущей версии.

    ``scd_version_id`` генерируется как хеш от ``concat(pk, '|', current_timestamp)``
    — обеспечивает уникальность для MERGE-сопоставления.

    ``valid_from`` для новых версий = ``day_end`` (начало следующего дня в TZ).
    ``valid_to`` для закрываемых = ``day_end - 1μs``.

    Args:
        dst_cur: Trino-курсор.
        pg_dsn: DSN к Postgres-источнику.
        catalog: Каталог Iceberg.
        raw_schema: Схема RAW-слоя.
        dds_schema: Схема DDS.
        physical_table: Имя физической SCD2-таблицы.
        logical_view_name: Имя логического VIEW (dds_*).
        table_fqn: Полное qualified-имя исходной таблицы.
        cutoff_day: Дата среза.
        day_tz: Часовой пояс для суточного окна.
        dry_run: True — только подсчёт, без записи.
        force_rebuild: True — принудительный bootstrap (игнорировать существование).

    Returns:
        Общее количество затронутых строк (bootstrap rows или expire+insert).
    """
    # Получение PK и колонок из Postgres-источника
    pk, pg_cols_ext = fetch_mirror_cols_with_lineage(pg_dsn, table_fqn)
    non_pk = non_pk_for_hash(pk, pg_cols_ext)

    # Короткое имя таблицы для ссылок на RAW-зеркало
    mirror_table = split_table_name(table_fqn)[1]
    mirror_ref = quote_table(catalog, raw_schema, mirror_table)
    events_ref = quote_table(catalog, raw_schema, target_table_name(table_fqn, "events"))
    phys_ref = quote_table(catalog, dds_schema, physical_table)

    # Суточное окно в локальной TZ
    _, day_start, day_end = build_day_window(cutoff_day.isoformat(), day_tz)
    # valid_to для закрываемых: последняя микросекунда дня
    valid_to_close = sql_timestamptz(day_end - timedelta(microseconds=1))
    # valid_from для новых: начало следующего дня
    valid_from_open = sql_timestamptz(day_end)

    # DDL для создания физической таблицы
    ddl_create = ddl_incremental_scd2(
        catalog=catalog,
        dds_schema=dds_schema,
        physical_table=physical_table,
        cols=pg_cols_ext,
        primary_key=pk,
    )

    # Выражения для хеша и PK-конкатенации
    mh = mirror_attr_hash("m", non_pk)          # row_attr_hash зеркала
    mv = mirror_pk_concat("m", pk)               # строковый PK зеркала

    # Список бизнес-колонок для SELECT
    biz_select = ", ".join(f"m.{quote_ident(c.name)}" for c in pg_cols_ext)

    # WHERE для окна событий
    iw = _events_window_where("ev", table_fqn, day_start, day_end)

    # Подзапрос: уникальные PK с op IN ('INSERT', 'UPDATE') за окно
    iu_pks_sql = (
        f"SELECT DISTINCT CAST(ev.source_pkey AS VARCHAR) AS pk FROM {events_ref} ev WHERE {iw} "
        "AND upper(ev.op) IN ('INSERT', 'UPDATE')"
    )

    # ================================================================
    # Bootstrap SELECT: полный скан зеркала для первого запуска
    # ================================================================
    # scd_version_id при bootstrap: хеш (pk || '|BOOT')
    # valid_from = day_start (начало окна)
    bootstrap_select = (
        f"SELECT {biz_select}, "
        f"{mirror_attr_hash('m', non_pk)} AS {quote_ident('row_attr_hash')}, "
        f"from_big_endian_64(xxhash64(to_utf8(concat({mirror_pk_concat('m', pk)}, {sql_string('|BOOT')})))) "
        f"AS {quote_ident('scd_version_id')}, "
        f"{sql_timestamptz(day_start)} AS {quote_ident('valid_from')}, "
        f"CAST(NULL AS TIMESTAMP(6) WITH TIME ZONE) AS {quote_ident('valid_to')}, "
        f"CAST(TRUE AS BOOLEAN) AS {quote_ident('is_current')}, "
        f"{sql_date(cutoff_day)} AS {quote_ident('dds_snapshot_day')}, "
        f"{sql_string('BOOTSTRAP')} AS {quote_ident('scd_row_op')}, "
        f"CAST(current_timestamp AS TIMESTAMP(6) WITH TIME ZONE) AS {quote_ident('dds_loaded_at')} "
        f"FROM {mirror_ref} m"
    )
    bootstrap_insert = f"INSERT INTO {phys_ref} {bootstrap_select}"

    # Проверка необходимости bootstrap
    exists = iceberg_base_table_exists(dst_cur, catalog, dds_schema, physical_table)
    rebuild = force_rebuild or not exists

    # Бизнес-колонки для VIEW (без технических SCD2)
    biz_view_cols = [c.name for c in pg_cols_ext] + ["dds_snapshot_day", "dds_loaded_at"]

    # --- Ветка BOOTSTRAP ---
    if rebuild:
        LOGGER.info(
            "[INFO] scd2 incremental: %s phys=%s -> BOOTSTRAP (iceberg_table_exists=%s force_rebuild=%s)",
            table_fqn,
            physical_table,
            exists,
            force_rebuild,
        )
        bootstrap_count_sql = f"SELECT CAST(count(*) AS BIGINT) FROM ({bootstrap_select}) bi"
        n_boot = _count_bigint(dst_cur, bootstrap_count_sql)
        if dry_run:
            return n_boot
        drop_view_best_effort(dst_cur, catalog, dds_schema, logical_view_name)
        dst_cur.execute(f"DROP TABLE IF EXISTS {phys_ref}")
        dst_cur.execute(ddl_create)
        execute_trino_write_with_iceberg_retry(
            dst_cur,
            bootstrap_insert,
            target_ref=phys_ref,
            op="scd2_incremental_bootstrap_insert",
        )
        recreate_incremental_current_view(
            dst_cur,
            catalog=catalog,
            dds_schema=dds_schema,
            view_name=logical_view_name,
            physical_ref=phys_ref,
            business_columns=biz_view_cols,
            dry_run=False,
        )
        LOGGER.info("[INFO] scd2 incremental bootstrap rows=%s table=%s", n_boot, table_fqn)
        return n_boot

    # --- Ветка дневной дельты ---
    LOGGER.info(
        "[INFO] scd2 incremental: %s phys=%s -> DELTA_DAY (events window)",
        table_fqn,
        physical_table,
    )

    # Подзапрос: уникальные PK с op = 'DELETE' за окно
    del_pks_sql = (
        f"SELECT DISTINCT CAST(ev.source_pkey AS VARCHAR) AS pk FROM {events_ref} ev WHERE {iw} "
        "AND upper(ev.op) = 'DELETE'"
    )

    # ================================================================
    # expire_using: строки, которые нужно закрыть
    # Включает:
    #   - DELETE: PK есть в del_pks_sql
    #   - UPDATE: PK есть в iu_pks_sql И row_attr_hash изменился
    # ================================================================
    expire_using = (
        f"SELECT DISTINCT t_inner.* FROM {phys_ref} t_inner WHERE t_inner.{quote_ident('is_current')} = TRUE "
        f"AND ( CAST({mirror_pk_concat('t_inner', pk)} AS VARCHAR) IN ({del_pks_sql}) "
        f"OR ( CAST({mirror_pk_concat('t_inner', pk)} AS VARCHAR) IN ({iu_pks_sql}) "
        # Сравнение хеша: зеркало vs текущая версия
        f"AND EXISTS ( SELECT 1 FROM {mirror_ref} m WHERE "
        f"CAST({mirror_pk_concat('m', pk)} AS VARCHAR) = CAST({mirror_pk_concat('t_inner', pk)} AS VARCHAR) "
        f"AND {mh} <> t_inner.row_attr_hash ) "
        ") ) "
    )

    # MERGE для закрытия версий: valid_to = day_end - 1μs, is_current = FALSE
    expire_merge_sql = (
        f"MERGE INTO {phys_ref} t USING ({expire_using}) src ON "
        f"t.{quote_ident('scd_version_id')} = src.{quote_ident('scd_version_id')} "
        f"WHEN MATCHED THEN UPDATE SET "
        f"{quote_ident('valid_to')} = {valid_to_close}, "
        f"{quote_ident('is_current')} = FALSE, "
        f"{quote_ident('scd_row_op')} = {sql_string('CLOSE')}, "
        f"{quote_ident('dds_loaded_at')} = CAST(current_timestamp AS TIMESTAMP(6) WITH TIME ZONE)"
    )

    # ================================================================
    # insert_select: новые строки для вставки
    # Вставляются PK из iu_pks_sql, для которых ещё нет is_current версии
    # (либо новый PK, либо старая версия уже закрыта выше)
    # scd_version_id = хеш (pk || '|' || current_timestamp)
    # ================================================================
    insert_select = (
        f"SELECT {biz_select}, "
        f"{mh} AS row_attr_hash, "
        f"from_big_endian_64(xxhash64(to_utf8(concat(CAST({mv} AS VARCHAR), "
        f"{sql_string('|')}, CAST(CAST(current_timestamp AS TIMESTAMP(6) WITH TIME ZONE) AS VARCHAR))))) "
        f"AS scd_version_id, "
        f"{valid_from_open} AS valid_from, "
        f"CAST(NULL AS TIMESTAMP(6) WITH TIME ZONE) AS valid_to, CAST(TRUE AS BOOLEAN) AS is_current, "
        f"{sql_date(cutoff_day)} AS dds_snapshot_day, {sql_string('UPSERT')} AS scd_row_op, "
        f"CAST(current_timestamp AS TIMESTAMP(6) WITH TIME ZONE) AS dds_loaded_at "
        f"FROM {mirror_ref} m WHERE CAST({mv} AS VARCHAR) IN ({iu_pks_sql}) "
        # Анти-джойн: только если нет текущей is_current версии с этим PK
        f"AND NOT EXISTS ( SELECT 1 FROM {phys_ref} cur WHERE cur.{quote_ident('is_current')} = TRUE "
        f"AND CAST({mirror_pk_concat('cur', pk)} AS VARCHAR) = CAST({mv} AS VARCHAR) )"
    )

    insert_sql = f"INSERT INTO {phys_ref} {insert_select}"

    # Подсчёт объёмов дельты
    n_expire = _count_bigint(dst_cur, f"SELECT CAST(count(*) AS BIGINT) FROM ({expire_using}) ex")
    n_ins = _count_bigint(dst_cur, f"SELECT CAST(count(*) AS BIGINT) FROM ({insert_select}) ins")

    if dry_run:
        LOGGER.info("[INFO] scd2 incremental dry-run close=%s open=%s", n_expire, n_ins)
        return n_expire + n_ins

    # Шаг 1: закрыть устаревшие/удалённые версии
    if n_expire > 0:
        execute_trino_write_with_iceberg_retry(
            dst_cur,
            expire_merge_sql,
            target_ref=phys_ref,
            op="scd2_incremental_expire",
        )

    # Шаг 2: вставить новые версии
    if n_ins > 0:
        execute_trino_write_with_iceberg_retry(
            dst_cur,
            insert_sql,
            target_ref=phys_ref,
            op="scd2_incremental_insert",
        )

    # Шаг 3: обновить VIEW
    recreate_incremental_current_view(
        dst_cur,
        catalog=catalog,
        dds_schema=dds_schema,
        view_name=logical_view_name,
        physical_ref=phys_ref,
        business_columns=biz_view_cols,
        dry_run=False,
    )
    LOGGER.info("[INFO] scd2 incremental daily close=%s open=%s", n_expire, n_ins)
    return n_expire + n_ins


# ===========================================================================
# Основная функция: full snapshot SCD2
# ===========================================================================


def run_full_snapshot_scd2(
    dst_cur,
    *,
    catalog: str,
    raw_schema: str,
    dds_schema: str,
    physical_table: str,
    logical_view_name: str,
    source_full_name: str,
    cutoff_day: date,
    dry_run: bool,
    force_rebuild: bool,
) -> int:
    """Full snapshot SCD2: сравнение снимка за ``snapshot_day = cutoff_day`` с is_current.

    Алгоритм:

    1. **Bootstrap** (если таблицы нет или ``force_rebuild``):
       - Ранжирование всех строк ``*_snapshot`` по ``(source_pkey, snapshot_day DESC, extracted_at DESC)``.
       - Выбор последней версии (``rn = 1``) на ``cutoff_day``.
       - ``row_attr_hash`` = ``from_big_endian_64(xxhash64(to_utf8(payload)))``.

    2. **Дневная дельта**:
       - Выборка снимка за ``snapshot_day = cutoff_day``.
       - Сравнение ``row_attr_hash``: если изменился → ``CLOSE`` + ``SNAPSHOT_DELTA``.
       - PK, отсутствующие в сегодняшнем снимке → ``CLOSE`` (строка удалена в источнике).
       - Новые PK (есть в снимке, нет в текущих) → ``SNAPSHOT_DELTA``.

    ``scd_version_id`` для дельты: хеш ``(source_pkey || '|' || current_timestamp)``.

    Args:
        dst_cur: Trino-курсор.
        catalog: Каталог Iceberg.
        raw_schema: Схема RAW-слоя.
        dds_schema: Схема DDS.
        physical_table: Имя физической SCD2-таблицы.
        logical_view_name: Имя логического VIEW.
        source_full_name: Полное qualified-имя источника.
        cutoff_day: Дата среза.
        dry_run: True — только подсчёт.
        force_rebuild: True — принудительный bootstrap.

    Returns:
        Общее количество затронутых строк.
    """
    # RAW snapshot table
    snapshot_tbl = target_table_name(source_full_name, "snapshot")
    snap_ref = quote_table(catalog, raw_schema, snapshot_tbl)
    phys_ref = quote_table(catalog, dds_schema, physical_table)

    # Сегодняшний снимок: строки с snapshot_day = cutoff_day
    # h = row_attr_hash как хеш payload
    today_select = (
        "SELECT s.source_pkey AS source_pkey, s.payload AS payload, "
        "s.snapshot_day AS rsnap, s.extracted_at AS rex, "
        "from_big_endian_64(xxhash64(to_utf8(CAST(s.payload AS VARCHAR)))) AS h "
        f"FROM {snap_ref} s WHERE s.{quote_ident('snapshot_day')} = {sql_date(cutoff_day)}"
    )

    # DDL физической таблицы
    ddl_create = ddl_full_scd2(catalog=catalog, dds_schema=dds_schema, physical_table=physical_table)

    # valid_from для bootstrap: начало дня cutoff в UTC
    cutoff_utc_floor = datetime.combine(cutoff_day, datetime.min.time(), tzinfo=timezone.utc)
    valid_from_boot_lit = sql_timestamptz(cutoff_utc_floor)

    # ================================================================
    # Bootstrap: ранжирование + выбор последней версии на cutoff_day
    # ================================================================
    bootstrap_ranked_sql = f"""
WITH ranked AS (
    SELECT source_pkey,
           payload,
           snapshot_day AS rsnap,
           extracted_at AS rex,
           row_number() OVER (
               PARTITION BY source_pkey ORDER BY snapshot_day DESC, extracted_at DESC
           ) AS rn,
           from_big_endian_64(xxhash64(to_utf8(CAST(payload AS VARCHAR)))) AS h
    FROM {snap_ref}
    WHERE snapshot_day <= {sql_date(cutoff_day)}
),
last_row AS (
    SELECT source_pkey,
           CAST(payload AS VARCHAR) AS payload,
           rsnap,
           rex,
           h
    FROM ranked
    WHERE rn = 1
)
SELECT source_pkey,
       payload,
       rsnap AS raw_snapshot_day,
       rex AS raw_extracted_at,
       h AS row_attr_hash,
       from_big_endian_64(xxhash64(to_utf8(concat(CAST(source_pkey AS VARCHAR), {sql_string('|BOOT')}))))
           AS scd_version_id,
       {valid_from_boot_lit} AS valid_from,
       CAST(NULL AS TIMESTAMP(6) WITH TIME ZONE) AS valid_to,
       CAST(TRUE AS BOOLEAN) AS is_current,
       {sql_date(cutoff_day)} AS dds_snapshot_day,
       {sql_string('BOOTSTRAP')} AS scd_row_op,
       CAST(current_timestamp AS TIMESTAMP(6) WITH TIME ZONE) AS dds_loaded_at
FROM last_row"""

    bootstrap_insert = f"INSERT INTO {phys_ref}\n{bootstrap_ranked_sql}"
    exists = iceberg_base_table_exists(dst_cur, catalog, dds_schema, physical_table)
    rebuild = force_rebuild or not exists

    # --- Ветка BOOTSTRAP ---
    if rebuild:
        LOGGER.info(
            "[INFO] scd2 full snapshot: %s phys=%s -> BOOTSTRAP (iceberg_table_exists=%s force_rebuild=%s)",
            source_full_name,
            physical_table,
            exists,
            force_rebuild,
        )
        n_boot = _count_bigint(
            dst_cur,
            f"SELECT CAST(count(*) AS BIGINT) FROM (\n{bootstrap_ranked_sql}\n) bx",
        )
        if dry_run:
            return n_boot
        drop_view_best_effort(dst_cur, catalog, dds_schema, logical_view_name)
        dst_cur.execute(f"DROP TABLE IF EXISTS {phys_ref}")
        dst_cur.execute(ddl_create)
        execute_trino_write_with_iceberg_retry(
            dst_cur,
            bootstrap_insert,
            target_ref=phys_ref,
            op="scd2_full_bootstrap_insert",
        )
        recreate_full_current_view(
            dst_cur,
            catalog=catalog,
            dds_schema=dds_schema,
            view_name=logical_view_name,
            physical_ref=phys_ref,
            dry_run=False,
        )
        LOGGER.info("[INFO] scd2 full bootstrap rows=%s table=%s", n_boot, source_full_name)
        return n_boot

    # --- Ветка дневной дельты ---
    LOGGER.info(
        "[INFO] scd2 full snapshot: %s phys=%s -> DELTA_DAY (snapshot_day = cutoff)",
        source_full_name,
        physical_table,
    )

    # valid_from для новых/изменённых строк: текущий момент
    open_ts = sql_timestamptz(datetime.now(timezone.utc))

    # ================================================================
    # expire_using: строки, которые нужно закрыть
    #   - PK нет в сегодняшнем снимке (удалены)
    #   - row_attr_hash изменился по сравнению с текущей версией
    # ================================================================
    expire_using = (
        f"SELECT c.* FROM {phys_ref} c WHERE c.{quote_ident('is_current')} = TRUE "
        f"AND ( c.{quote_ident('source_pkey')} NOT IN ( "
        f"SELECT tt.source_pkey FROM ({today_select}) tt ) "
        f"OR EXISTS ( SELECT 1 FROM ({today_select}) t WHERE "
        f"t.source_pkey = c.{quote_ident('source_pkey')} AND t.h <> c.row_attr_hash ) )"
    )

    # MERGE для закрытия: valid_to = now(), is_current = FALSE
    expire_merge_sql = (
        f"MERGE INTO {phys_ref} t USING ({expire_using}) src ON "
        f"t.{quote_ident('scd_version_id')} = src.{quote_ident('scd_version_id')} "
        f"WHEN MATCHED THEN UPDATE SET "
        f"{quote_ident('valid_to')} = CAST(current_timestamp AS TIMESTAMP(6) WITH TIME ZONE), "
        f"{quote_ident('is_current')} = FALSE, "
        f"{quote_ident('scd_row_op')} = {sql_string('CLOSE')}, "
        f"{quote_ident('dds_loaded_at')} = CAST(current_timestamp AS TIMESTAMP(6) WITH TIME ZONE)"
    )

    # ================================================================
    # insert_select: новые строки из сегодняшнего снимка
    # Вставляются строки из today_select, для которых нет is_current версии
    # (либо новый PK, либо старая уже закрыта)
    # ================================================================
    insert_select = (
        f"SELECT t.source_pkey, CAST(t.payload AS VARCHAR), "
        f"t.rsnap AS raw_snapshot_day, t.rex AS raw_extracted_at, "
        "t.h AS row_attr_hash, "
        "from_big_endian_64(xxhash64(to_utf8(concat(CAST(t.source_pkey AS VARCHAR), "
        f"{sql_string('|')}, CAST(CAST(current_timestamp AS TIMESTAMP(6) WITH TIME ZONE) AS VARCHAR))))) "
        f"AS scd_version_id, {open_ts} AS valid_from, "
        "CAST(NULL AS TIMESTAMP(6) WITH TIME ZONE) AS valid_to, CAST(TRUE AS BOOLEAN) AS is_current, "
        f"{sql_date(cutoff_day)} AS dds_snapshot_day, {sql_string('SNAPSHOT_DELTA')} AS scd_row_op, "
        "CAST(current_timestamp AS TIMESTAMP(6) WITH TIME ZONE) AS dds_loaded_at "
        f"FROM ({today_select}) t WHERE NOT EXISTS ( "
        f"SELECT 1 FROM {phys_ref} cur WHERE cur.{quote_ident('is_current')} = TRUE "
        f"AND cur.{quote_ident('source_pkey')} = t.source_pkey )"
    )

    insert_sql = f"INSERT INTO {phys_ref}\n{insert_select}"

    # Подсчёт объёмов
    n_expire = _count_bigint(dst_cur, f"SELECT CAST(count(*) AS BIGINT) FROM ({expire_using}) ex")
    n_ins = _count_bigint(dst_cur, f"SELECT CAST(count(*) AS BIGINT) FROM ({insert_select}) ins")

    if dry_run:
        LOGGER.info("[INFO] scd2 full dry-run close=%s open=%s", n_expire, n_ins)
        return n_expire + n_ins

    # Шаг 1: закрыть устаревшие/удалённые версии
    if n_expire > 0:
        execute_trino_write_with_iceberg_retry(
            dst_cur,
            expire_merge_sql,
            target_ref=phys_ref,
            op="scd2_full_expire",
        )

    # Шаг 2: вставить новые версии
    if n_ins > 0:
        execute_trino_write_with_iceberg_retry(
            dst_cur,
            insert_sql,
            target_ref=phys_ref,
            op="scd2_full_insert",
        )

    # Шаг 3: обновить VIEW
    recreate_full_current_view(
        dst_cur,
        catalog=catalog,
        dds_schema=dds_schema,
        view_name=logical_view_name,
        physical_ref=phys_ref,
        dry_run=False,
    )
    LOGGER.info("[INFO] scd2 full daily close=%s open=%s", n_expire, n_ins)
    return n_expire + n_ins
