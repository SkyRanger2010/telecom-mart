"""Одна календарная дата: чтение из PostgreSQL (HALK) и запись в Iceberg RAW через Trino.

Режимы по таблице (``tables[]`` в ``raw_load_config.json``):
  incremental — события за суточное окно из журнала источника (``source.log_schema`` /
  ``log_table``, по умолчанию ``public.global_log``), нормализация в ``*__events`` и
  актуальное состояние в ``*__state`` (плюс ``*__events_invalid`` при браке).
  full — снимок всей таблицы в ``*__snapshot`` (нужен ``primary_key`` в конфиге).

Окно дня задаётся ``--day`` и ``--tz`` (локальные полночь..полночь → UTC для фильтра лога).

Опционально ``--table schema.table`` (или без схемы — см. ``source.default_schema`` в JSON):
загружает только одну строку из ``tables[]``; удобно для отдельных задач Airflow на таблицу.

Исторический массив дат без DAG: скрипт ``load_raw_period_batch.py``. Ежедневный пайплайн —
``telecom_kpi_daily`` запускает по экземпляру этого модуля на каждую таблицу.
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import os
import random
import time as time_module
from decimal import Decimal, InvalidOperation
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Callable, Iterable
from uuid import UUID
from zoneinfo import ZoneInfo

import psycopg
import trino.dbapi

LOGGER = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Конфигурационные константы — лимиты, ретраи и поведение
# ---------------------------------------------------------------------------
# Значения по умолчанию можно переопределить через переменные окружения.

# Имена служебных колонок в целевых таблицах Iceberg: источник и временная метка загрузки.
RAW_LOAD_SOURCE_COL = "raw_load_source"
RAW_LOADED_AT_COL = "raw_loaded_at"

# Максимальная длина SQL-запроса INSERT в символах.
# Если батч превышает лимит, он дробится на несколько statement'ов.
MAX_TRINO_INSERT_QUERY_TEXT = 850_000
# Максимальное число уникальных партиций (дней changed_at) в одном MERGE запросе событий.
# При превышении батч разбивается, чтобы избежать открытия слишком многих партиций.
MAX_EVENT_OPEN_PARTITIONS_PER_QUERY = 90
# Максимальное число строк в одном MERGE для таблиц событий (*__events).
MAX_EVENT_ROWS_PER_MERGE = int(os.getenv("RAW_LOADER_MAX_EVENT_ROWS_PER_MERGE", "20000"))
# Верхний предел строк в одной операции записи в Trino/Iceberg.
# Используется как cap для всех batch_size при вставке.
MAX_TRINO_ROWS_PER_WRITE = int(os.getenv("RAW_LOADER_MAX_TRINO_ROWS_PER_WRITE", "20000"))
# Количество попыток ретрая при ошибках сетевой связности к Trino.
TRINO_WRITE_RETRY_ATTEMPTS = int(os.getenv("RAW_LOADER_TRINO_WRITE_RETRY_ATTEMPTS", "6"))
# Базовая задержка (сек) для экспоненциального backoff при ретраях Trino.
# Удваивается с каждой попыткой, capped на MAX_SLEEP_SECONDS.
TRINO_WRITE_RETRY_BASE_SECONDS = float(os.getenv("RAW_LOADER_TRINO_WRITE_RETRY_BASE_SECONDS", "2.0"))
# Верхняя граница (сек) для сна при экспоненциальном backoff.
TRINO_WRITE_RETRY_MAX_SLEEP_SECONDS = float(os.getenv("RAW_LOADER_TRINO_WRITE_RETRY_MAX_SLEEP_SECONDS", "30.0"))
# Ретраи для «горячих» таблиц Iceberg (raw_load_manifest, dds_table_load_log):
# параллельные писатели из многих raw__/dds__ задач чаще дают конфликты коммита —
# удваиваем число попыток, минимум 12.
_ICEBERG_HOTSPOT_ATTEMPTS_DEFAULT = str(max(TRINO_WRITE_RETRY_ATTEMPTS * 2, 12))
ICEBERG_HOTSPOT_WRITE_RETRY_ATTEMPTS = max(
    1,
    int(os.getenv("RAW_LOADER_ICEBERG_HOTSPOT_RETRY_ATTEMPTS", _ICEBERG_HOTSPOT_ATTEMPTS_DEFAULT)),
)
# Если True, для UPDATE-событий предпочитаем полный new_values вместо diff'а.
# Это снижает фрагментацию MERGE по changed_columns и держит батчи UPDATE крупными.
PREFER_FULL_UPDATE_PAYLOAD = os.getenv("RAW_LOADER_PREFER_FULL_UPDATE_PAYLOAD", "1").lower() not in {
    "0",
    "false",
    "no",
}
# Если True, все UPDATE сливаются в один MERGE с CASE WHEN (унифицированный подход).
# Если False — группируем UPDATE по набору изменившихся колонок и делаем отдельный MERGE
# для каждой группы (может снижать lock contention на стороне Iceberg).
UNIFIED_UPDATE_MERGE = os.getenv("RAW_LOADER_UNIFIED_UPDATE_MERGE", "1").lower() not in {
    "0",
    "false",
    "no",
}


@dataclass
class TableConfig:
    """Конфигурация одной таблицы источника из секции ``tables[]`` JSON-конфига.

    Attributes:
        name: Полное имя таблицы в формате ``schema.table``.
        mode: Режим загрузки — ``"incremental"`` (события из global_log) или
            ``"full"`` (полный снимок таблицы).
        primary_key: Список колонок первичного ключа; обязателен для режима ``full``.
    """

    name: str
    mode: str
    primary_key: list[str]


@dataclass
class TableStats:
    """Статистика загрузки одной таблицы за одну итерацию (день/период).

    Собирается в процессе выполнения и записывается в ``raw_load_manifest``.

    Attributes:
        rows_read: Всего строк прочитано из источника (global_log или сама таблица).
        rows_events_inserted: Число уникальных событий, записанных в *__events.
        rows_state_upserted: Суммарное число операций в зеркале (upsert + update + delete
            + backfilled + replayed).
        rows_backfilled: Строки, подтянутые обратным запросом в источник (backfill).
        rows_replayed: Операции, повторённые после backfill для восстановления финального состояния.
        rows_invalid: События, не прошедшие валидацию — записаны в *__events_invalid.
        rows_cast_to_null: Число приведений типов, упавших в NULL (проблемные значения).
        rows_snapshot_loaded: Строк загружено в *__snapshot (только для full-режима).
    """

    rows_read: int = 0
    rows_events_inserted: int = 0
    rows_state_upserted: int = 0
    rows_backfilled: int = 0
    rows_replayed: int = 0
    rows_invalid: int = 0
    rows_cast_to_null: int = 0
    rows_snapshot_loaded: int = 0


@dataclass
class TrinoTargetConfig:
    """Параметры подключения к Trino (целевой системе).

    Собирается из JSON-конфига и переменных окружения (см. build_trino_target_config).

    Attributes:
        host: Хост Trino-координатора.
        port: Порт (обычно 8080).
        user: Имя пользователя для аутентификации.
        catalog: Каталог Iceberg (например ``iceberg``).
        schema: Схема по умолчанию внутри каталога.
        http_scheme: ``http`` или ``https``.
        verify_connection_sql: SQL-запрос для проверки подключения (обычно ``SELECT 1``).
    """

    host: str
    port: int
    user: str
    catalog: str
    schema: str
    http_scheme: str
    verify_connection_sql: str


@dataclass
class SourceColumn:
    """Метаданные одной колонки источника, отображённой в целевой тип Trino.

    Attributes:
        name: Имя колонки в PostgreSQL-источнике.
        trino_type: Соответствующий тип Trino (VARCHAR, BIGINT, TIMESTAMP и т.д.).
    """

    name: str
    trino_type: str


@dataclass
class IncrementalMirrorMeta:
    """Метаданные инкрементального зеркала таблицы источника → Iceberg.

    Создаётся/загружается при первом обращении к таблице в процессе загрузки.

    Attributes:
        source_schema: Схема в PostgreSQL-источнике.
        source_table: Имя таблицы в PostgreSQL-источнике.
        target_table: Имя целевой таблицы в Iceberg (без префикса каталога/схемы).
        target_ref: Полная ссылка на целевую таблицу (catalog.schema.table).
        columns: Список колонок с маппингом типов (включая служебные raw_load_*).
        primary_key: Список колонок первичного ключа.
        created: True, если целевая таблица была только что создана (а не существовала ранее).
    """

    source_schema: str
    source_table: str
    target_table: str
    target_ref: str
    columns: list[SourceColumn]
    primary_key: list[str]
    created: bool = False


# ---------- CLI и чтение конфига ----------


def parse_args() -> argparse.Namespace:
    """Разбирает аргументы командной строки для загрузки одного дня.

    Returns:
        argparse.Namespace с атрибутами config, day, dry_run, tz, log_level, table.
    """
    parser = argparse.ArgumentParser(
        description="Load one calendar day to Iceberg schema via Trino",
    )
    parser.add_argument(
        "--config",
        default="/opt/airflow/scripts/raw_load_config.json",
        help="Path to JSON config",
    )
    parser.add_argument(
        "--day",
        required=True,
        help="Day in YYYY-MM-DD format (UTC window [day, day+1))",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Read source only, do not write to target",
    )
    parser.add_argument(
        "--tz",
        default="UTC",
        help="Timezone for day window (example: Europe/Moscow)",
    )
    parser.add_argument(
        "--log-level",
        default=os.getenv("RAW_LOADER_LOG_LEVEL", "INFO"),
        help="Logging level (DEBUG, INFO, WARNING, ERROR)",
    )
    parser.add_argument(
        "--table",
        default=None,
        metavar="TABLE",
        help="Только указанная таблица источника: schema.table или table (schema из конфига)",
    )
    return parser.parse_args()


def setup_logging(log_level: str) -> None:
    """Настраивает корневой логгер на указанный уровень.

    Args:
        log_level: Строковое имя уровня (DEBUG, INFO, WARNING, ERROR).
    """
    resolved_level = getattr(logging, log_level.upper(), logging.INFO)
    logging.basicConfig(
        level=resolved_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def load_config(path: str) -> dict[str, Any]:
    """Загружает JSON-конфиг из файла.

    Args:
        path: Путь к JSON-файлу конфигурации.

    Returns:
        Распарсенный словарь конфигурации.

    Raises:
        FileNotFoundError: Если файл не найден.
        json.JSONDecodeError: Если файл не является валидным JSON.
    """
    with open(path, "r", encoding="utf-8") as config_file:
        return json.load(config_file)


def build_pg_dsn(config: dict[str, Any]) -> str:
    """Собирает PostgreSQL DSN из конфига и переменных окружения.

    Приоритет: ``dsn_env`` (готовая строка DSN) → отдельные параметры
    (host, port, dbname, user, password, sslmode) через env-переменные
    или default-значения из JSON.

    Args:
        config: Секция ``source`` из JSON-конфига.

    Returns:
        Строка DSN в формате ``host=... port=... dbname=... user=... password=... sslmode=...``.

    Raises:
        ValueError: Если не найдены обязательные параметры подключения.
    """
    dsn_env = config.get("dsn_env")
    if dsn_env and os.getenv(dsn_env):
        return os.environ[dsn_env]

    host = os.getenv(config.get("host_env", ""), config.get("default_host"))
    port = os.getenv(config.get("port_env", ""), str(config.get("default_port", 5432)))
    dbname = os.getenv(config.get("db_env", ""), config.get("default_db"))
    user = os.getenv(config.get("user_env", ""))
    password = os.getenv(config.get("password_env", ""))
    sslmode = os.getenv(config.get("sslmode_env", ""), config.get("default_sslmode", "prefer"))

    missing = []
    if not host:
        missing.append("host")
    if not dbname:
        missing.append("db")
    if not user:
        missing.append("user")
    if not password:
        missing.append("password")
    if missing:
        raise ValueError(f"Missing required connection parts: {', '.join(missing)}")

    return (
        f"host={host} port={port} dbname={dbname} "
        f"user={user} password={password} sslmode={sslmode}"
    )


def build_trino_target_config(config: dict[str, Any], fallback_schema: str) -> TrinoTargetConfig:
    """Собирает конфигурацию подключения к Trino из JSON и переменных окружения.

    Args:
        config: Секция ``target`` из JSON-конфига.
        fallback_schema: Имя схемы по умолчанию, если не указано в конфиге.

    Returns:
        Заполненный TrinoTargetConfig.

    Raises:
        ValueError: Если не найдены обязательные параметры (host, user, catalog).
    """
    host = os.getenv(config.get("host_env", ""), config.get("default_host", "trino"))
    port = int(os.getenv(config.get("port_env", ""), str(config.get("default_port", 8080))))
    user = os.getenv(config.get("user_env", ""), config.get("default_user", "airflow"))
    catalog = os.getenv(config.get("catalog_env", ""), config.get("default_catalog", "iceberg"))
    http_scheme = os.getenv(config.get("http_scheme_env", ""), config.get("default_http_scheme", "http"))
    schema = str(config.get("default_schema", fallback_schema))
    verify_connection_sql = str(config.get("verify_connection_sql", "SELECT 1"))

    missing = []
    if not host:
        missing.append("host")
    if not user:
        missing.append("user")
    if not catalog:
        missing.append("catalog")
    if missing:
        raise ValueError(f"Missing required Trino target parts: {', '.join(missing)}")

    return TrinoTargetConfig(
        host=host,
        port=port,
        user=user,
        catalog=catalog,
        schema=schema,
        http_scheme=http_scheme,
        verify_connection_sql=verify_connection_sql,
    )


def connect_source(dsn: str):
    """Открывает PostgreSQL-соединение к источнику.

    Args:
        dsn: Строка подключения (результат build_pg_dsn).

    Returns:
        Объект psycopg.Connection.
    """
    return psycopg.connect(dsn)


def connect_trino(target: TrinoTargetConfig):
    """Открывает соединение к Trino через DBAPI.

    Args:
        target: Конфигурация целевого Trino.

    Returns:
        Объект trino.dbapi.Connection.
    """
    return trino.dbapi.connect(
        host=target.host,
        port=target.port,
        user=target.user,
        catalog=target.catalog,
        schema=target.schema,
        http_scheme=target.http_scheme,
    )


def commit_if_supported(conn, context: str) -> None:
    """Вызывает conn.commit(), если драйвер поддерживает; ошибки глушит.

    Trino DBAPI в некоторых режимах не поддерживает явный commit —
    эта функция безопасно пропускает такие случаи.

    Args:
        conn: Объект соединения (psycopg или trino.dbapi).
        context: Имя контекста для отладочного лога при ошибке.
    """
    commit_fn = getattr(conn, "commit", None)
    if commit_fn is None:
        return
    try:
        commit_fn()
    except Exception as exc:  # pragma: no cover - depends on driver/autocommit mode
        LOGGER.debug("[DEBUG] skip commit for %s: %s", context, exc)


def raw_load_source_id_from_config(source_cfg: dict[str, Any]) -> str:
    """Извлекает идентификатор источника (lineage) из конфига.

    Args:
        source_cfg: Секция ``source`` из JSON-конфига.

    Returns:
        Строковый идентификатор, используемый в колонке ``raw_load_source``.
    """
    return str(source_cfg.get("raw_load_source_id", "halk_postgresql"))


def lineage_literal_update_suffix(source_id: str, loaded_at: datetime) -> str:
    """Формирует SQL-фрагмент для обновления lineage-колонок в MERGE UPDATE.

    Args:
        source_id: Идентификатор источника (raw_load_source_id).
        loaded_at: Временная метка загрузки.

    Returns:
        Строка вида ``, raw_load_source = '...', raw_loaded_at = ...``.
    """
    return (
        ", "
        + f"{quote_ident(RAW_LOAD_SOURCE_COL)} = {sql_string(source_id)}, "
        + f"{quote_ident(RAW_LOADED_AT_COL)} = {sql_timestamptz(loaded_at)}"
    )


def stamp_payload_lineage(rows: Iterable[dict[str, Any]], source_id: str) -> datetime:
    """Подставляет в dict-строчки RAW-зеркала метки для MERGE INSERT/UPDATE из staging."""
    at = datetime.now(timezone.utc)
    for row in rows:
        row[RAW_LOAD_SOURCE_COL] = source_id
        row[RAW_LOADED_AT_COL] = at
    return at


def parse_table_configs(raw_tables: list[dict[str, Any]], default_source_schema: str) -> list[TableConfig]:
    """Парсит секцию ``tables[]`` JSON-конфига в список TableConfig.

    Для имён без схемы подставляет default_source_schema.

    Args:
        raw_tables: Список словарей из секции ``tables``.
        default_source_schema: Схема по умолчанию для имён без явной схемы.

    Returns:
        Список объектов TableConfig.

    Raises:
        ValueError: Если имя таблицы пустое или режим не ``incremental``/``full``.
    """
    tables: list[TableConfig] = []
    for item in raw_tables:
        raw_name = str(item["name"]).strip()
        if not raw_name:
            raise ValueError("Table name must not be empty")
        if "." in raw_name:
            schema_name, table_name = split_table_name(raw_name)
        else:
            schema_name = default_source_schema
            table_name = raw_name
        name = f"{schema_name}.{table_name}"
        mode = str(item["mode"]).lower()
        if mode not in {"incremental", "full"}:
            raise ValueError(f"Unsupported mode '{mode}' for table {name}")
        primary_key = [str(col) for col in item.get("primary_key", [])]
        tables.append(TableConfig(name=name, mode=mode, primary_key=primary_key))
    return tables


def normalize_source_table_arg(arg: str, default_source_schema: str) -> str:
    """Нормализует аргумент --table: добавляет схему по умолчанию, если не указана.

    Args:
        arg: Значение --table (может быть ``schema.table`` или просто ``table``).
        default_source_schema: Схема по умолчанию.

    Returns:
        Полное имя ``schema.table``.

    Raises:
        ValueError: Если arg пустой.
    """
    stripped = arg.strip()
    if not stripped:
        raise ValueError("--table must not be empty")
    if "." not in stripped:
        return f"{default_source_schema}.{stripped}"
    return stripped


def filter_table_configs_by_arg(
    table_configs: list[TableConfig],
    table_arg: str | None,
    default_source_schema: str,
) -> list[TableConfig]:
    """Оставляет одну таблицу из конфига (для Airflow одна задача = один объект RAW)."""
    if not table_arg:
        return table_configs
    want = normalize_source_table_arg(table_arg, default_source_schema).lower()
    filtered = [t for t in table_configs if t.name.lower() == want]
    if not filtered:
        available = ", ".join(sorted({c.name for c in table_configs}))
        raise ValueError(f"--table '{table_arg}' отсутствует в конфиге. Допустимо: {available}")
    return filtered


# ---------- Имена целевых таблиц RAW и литералы SQL для Trino ----------


def quote_ident(identifier: str) -> str:
    """Экранирует идентификатор SQL двойными кавычками (Trino-совместимо).

    Args:
        identifier: Имя колонки, таблицы или схемы.

    Returns:
        Экранированная строка, например ``"my_table"``.

    Raises:
        ValueError: Если identifier пустой.
    """
    if not identifier:
        raise ValueError("Identifier must not be empty")
    return '"' + identifier.replace('"', '""') + '"'


def quote_table(catalog: str, schema: str, table: str) -> str:
    """Формирует полную ссылку на таблицу: ``"cat"."schema"."table"``.

    Args:
        catalog: Каталог (например ``iceberg``).
        schema: Схема.
        table: Имя таблицы.

    Returns:
        Строка для вставки в SQL-запрос.
    """
    return f"{quote_ident(catalog)}.{quote_ident(schema)}.{quote_ident(table)}"


def quote_schema(catalog: str, schema: str) -> str:
    """Формирует ссылку на схему: ``"cat"."schema"``.

    Args:
        catalog: Каталог.
        schema: Схема.

    Returns:
        Строка для использования в CREATE SCHEMA и т.п.
    """
    return f"{quote_ident(catalog)}.{quote_ident(schema)}"


def split_table_name(full_name: str) -> tuple[str, str]:
    """Разбирает ``schema.table`` на кортеж (schema, table).

    Args:
        full_name: Имя в формате ``schema.table``.

    Returns:
        Кортеж ``(schema, table)``.

    Raises:
        ValueError: Если формат не ``schema.table``.
    """
    parts = full_name.split(".")
    if len(parts) != 2:
        raise ValueError(f"Table must be in schema.table format: {full_name}")
    return parts[0], parts[1]


def target_table_name(full_name: str, suffix: str) -> str:
    """Генерирует имя целевой таблицы: ``schema__table__suffix``, обрезанное до 63 символов.

    Args:
        full_name: Исходное ``schema.table``.
        suffix: Суффикс целевой таблицы (events, state, snapshot, ...).

    Returns:
        Имя целевой таблицы, не длиннее 63 символов.
    """
    schema_name, table_name = split_table_name(full_name)
    normalized = f"{schema_name}__{table_name}__{suffix}"
    return normalized[:63]


def sql_string(value: str | None) -> str:
    """Формирует SQL-литерал строки с экранированием.

    Удаляет NUL-символы (Trino/Iceberg их не принимает).

    Args:
        value: Строковое значение или None.

    Returns:
        SQL-литерал (``'text'``) или ``CAST(NULL AS VARCHAR)``.
    """
    if value is None:
        return "CAST(NULL AS VARCHAR)"
    # Trino/Iceberg не принимают символ NUL в VARCHAR литералах — вычищаем.
    sanitized = str(value).replace("\x00", "")
    return "'" + sanitized.replace("'", "''") + "'"


def sql_bigint(value: int | None) -> str:
    """Формирует SQL-литерал BIGINT.

    Args:
        value: Целое число или None.

    Returns:
        Число как строка или ``CAST(NULL AS BIGINT)``.
    """
    if value is None:
        return "CAST(NULL AS BIGINT)"
    return str(int(value))


def sql_boolean(value: bool | None) -> str:
    """Формирует SQL-литерал BOOLEAN.

    Args:
        value: Булево значение или None.

    Returns:
        ``TRUE``, ``FALSE`` или ``CAST(NULL AS BOOLEAN)``.
    """
    if value is None:
        return "CAST(NULL AS BOOLEAN)"
    return "TRUE" if value else "FALSE"


def sql_date(value: date | None) -> str:
    """Формирует SQL-литерал DATE.

    Args:
        value: Объект date или None.

    Returns:
        ``DATE 'YYYY-MM-DD'`` или ``CAST(NULL AS DATE)``.
    """
    if value is None:
        return "CAST(NULL AS DATE)"
    return f"DATE '{value.isoformat()}'"


def sql_timestamptz(value: datetime | None) -> str:
    """Формирует SQL-литерал TIMESTAMP(6) WITH TIME ZONE.

    Явно указывает точность (6), чтобы избежать расхождений с DDL Iceberg
    (по умолчанию Trino может дать TIMESTAMP(3)).

    Args:
        value: datetime с tzinfo или None. Наивные datetime приводятся к UTC.

    Returns:
        ``CAST(TIMESTAMP '...' AS TIMESTAMP(6) WITH TIME ZONE)`` или CAST(NULL ...).
    """
    if value is None:
        return "CAST(NULL AS TIMESTAMP(6) WITH TIME ZONE)"
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    dt = value.astimezone(timezone.utc)
    # Литерал TIMESTAMP без (6) в Trino часто становится TIMESTAMP(3); Iceberg DDL — TIMESTAMP(6).
    frac = dt.strftime("%Y-%m-%d %H:%M:%S.") + f"{dt.microsecond:06d}"
    escaped = frac.replace("'", "''")
    return f"CAST(TIMESTAMP '{escaped} UTC' AS TIMESTAMP(6) WITH TIME ZONE)"


def sql_json_text(value: Any) -> str:
    """Формирует SQL-литерал для JSON-значения (сериализует в строку).

    Args:
        value: Произвольное значение для json.dumps, или None.

    Returns:
        SQL-литерал строки или ``CAST(NULL AS VARCHAR)``.
    """
    if value is None:
        return "CAST(NULL AS VARCHAR)"
    return sql_string(json.dumps(value))


def row_exists(dst_cur, table_ref: str, where_sql: str) -> bool:
    """Проверяет существование строки в таблице Trino по условию.

    Args:
        dst_cur: Курсор Trino.
        table_ref: Полная ссылка на таблицу.
        where_sql: SQL-условие WHERE (без ключевого слова WHERE).

    Returns:
        True, если хотя бы одна строка удовлетворяет условию.
    """
    dst_cur.execute(f"SELECT 1 FROM {table_ref} WHERE {where_sql} LIMIT 1")
    return dst_cur.fetchone() is not None


# ---------- DDL RAW: события, state, snapshot, манифест ----------


def ensure_raw_structures(
    dst_cur,
    catalog: str,
    raw_schema: str,
    table: TableConfig,
) -> dict[str, str]:
    """Создаёт (если отсутствуют) целевые таблицы RAW для одной таблицы источника.

    Для incremental-режима создаёт *__events, *__events_invalid, *__state.
    Для full-режима — только *__snapshot.
    Также дополняет существующие таблицы колонками lineage (ALTER ADD COLUMN IF NOT EXISTS).

    Args:
        dst_cur: Курсор Trino.
        catalog: Каталог Iceberg.
        raw_schema: Схема RAW.
        table: Конфигурация таблицы.

    Returns:
        Словарь с ключами ``events``, ``state``, ``invalid``, ``snapshot`` — имена целевых таблиц.
    """
    state = target_table_name(table.name, "state")
    invalid = target_table_name(table.name, "events_invalid")
    snapshot = target_table_name(table.name, "snapshot")

    dst_cur.execute(f"CREATE SCHEMA IF NOT EXISTS {quote_schema(catalog, raw_schema)}")

    if table.mode == "incremental":
        dst_cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {quote_table(catalog, raw_schema, events)} (
                log_id BIGINT,
                source_table VARCHAR,
                source_pkey VARCHAR,
                op VARCHAR,
                changed_at TIMESTAMP(6) WITH TIME ZONE,
                old_values VARCHAR,
                new_values VARCHAR,
                old_diff VARCHAR,
                new_diff VARCHAR,
                ingested_at TIMESTAMP(6) WITH TIME ZONE,
                {quote_ident(RAW_LOAD_SOURCE_COL)} VARCHAR,
                {quote_ident(RAW_LOADED_AT_COL)} TIMESTAMP(6) WITH TIME ZONE
            )
            """
        )
        dst_cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {quote_table(catalog, raw_schema, invalid)} (
                log_id BIGINT,
                source_table VARCHAR,
                source_pkey VARCHAR,
                op VARCHAR,
                changed_at TIMESTAMP(6) WITH TIME ZONE,
                reason VARCHAR,
                old_values VARCHAR,
                new_values VARCHAR,
                old_diff VARCHAR,
                new_diff VARCHAR,
                ingested_at TIMESTAMP(6) WITH TIME ZONE,
                {quote_ident(RAW_LOAD_SOURCE_COL)} VARCHAR,
                {quote_ident(RAW_LOADED_AT_COL)} TIMESTAMP(6) WITH TIME ZONE
            )
            """
        )
        dst_cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {quote_table(catalog, raw_schema, state)} (
                source_pkey VARCHAR,
                payload VARCHAR,
                is_deleted BOOLEAN,
                last_op VARCHAR,
                last_log_id BIGINT,
                changed_at TIMESTAMP(6) WITH TIME ZONE,
                ingested_at TIMESTAMP(6) WITH TIME ZONE,
                {quote_ident(RAW_LOAD_SOURCE_COL)} VARCHAR,
                {quote_ident(RAW_LOADED_AT_COL)} TIMESTAMP(6) WITH TIME ZONE
            )
            """
        )
        ensure_raw_lineage_columns_exist(dst_cur, quote_table(catalog, raw_schema, events))
        ensure_raw_lineage_columns_exist(dst_cur, quote_table(catalog, raw_schema, invalid))
        ensure_raw_lineage_columns_exist(dst_cur, quote_table(catalog, raw_schema, state))
    else:
        dst_cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {quote_table(catalog, raw_schema, snapshot)} (
                snapshot_day DATE,
                source_pkey VARCHAR,
                payload VARCHAR,
                extracted_at TIMESTAMP(6) WITH TIME ZONE,
                {quote_ident(RAW_LOAD_SOURCE_COL)} VARCHAR,
                {quote_ident(RAW_LOADED_AT_COL)} TIMESTAMP(6) WITH TIME ZONE
            )
            """
        )
        ensure_raw_lineage_columns_exist(dst_cur, quote_table(catalog, raw_schema, snapshot))
    return {"events": events, "state": state, "invalid": invalid, "snapshot": snapshot}


def ensure_manifest_table(dst_cur, catalog: str, raw_schema: str) -> None:
    """Создаёт таблицу ``raw_load_manifest`` для журнала загрузок.

    Партиционирована по ``day(load_day)`` и ``source_table``.

    Args:
        dst_cur: Курсор Trino.
        catalog: Каталог Iceberg.
        raw_schema: Схема RAW.
    """
    manifest_ref = quote_table(catalog, raw_schema, "raw_load_manifest")
    dst_cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {manifest_ref} (
            load_day DATE,
            source_table VARCHAR,
            mode VARCHAR,
            tz_name VARCHAR,
            window_start_utc TIMESTAMP(6) WITH TIME ZONE,
            window_end_utc TIMESTAMP(6) WITH TIME ZONE,
            rows_read BIGINT,
            rows_events_inserted BIGINT,
            rows_state_upserted BIGINT,
            rows_invalid BIGINT,
            rows_snapshot_loaded BIGINT,
            status VARCHAR,
            details VARCHAR,
            started_at TIMESTAMP(6) WITH TIME ZONE,
            finished_at TIMESTAMP(6) WITH TIME ZONE
        )
        WITH (
            partitioning = ARRAY['day(load_day)', 'source_table']
        )
        """
    )


def chunked(items: list[Any], batch_size: int) -> Iterable[list[Any]]:
    """Делит список на батчи фиксированного размера.

    Args:
        items: Исходный список.
        batch_size: Размер одного батча.

    Yields:
        Списки длиной не более batch_size.

    Raises:
        ValueError: Если batch_size <= 0.
    """
    if batch_size <= 0:
        raise ValueError("batch_size must be greater than 0")
    for index in range(0, len(items), batch_size):
        yield items[index : index + batch_size]


def make_staging_table_name(base_table: str, suffix: str) -> str:
    """Генерирует уникальное имя для временной staging-таблицы.

    Имя строится как ``base_table__<suffix>__stg__<timestamp_microseconds>``,
    обрезается до 63 символов.

    Args:
        base_table: Базовое имя целевой таблицы (без каталога/схемы).
        suffix: Описательный суффикс (events, upsert, delete, ...).

    Returns:
        Уникальное имя staging-таблицы.
    """
    unique_suffix = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
    tail = f"__{suffix}__stg__{unique_suffix}"
    head_max = max(1, 63 - len(tail))
    return f"{base_table[:head_max]}{tail}"


def create_staging_table(dst_cur, target_ref: str, staging_ref: str) -> None:
    """Создаёт staging-таблицу как пустую копию целевой (CREATE ... AS SELECT ... WHERE FALSE).

    Предварительно удаляет staging-таблицу, если она существует.

    Args:
        dst_cur: Курсор Trino.
        target_ref: Полная ссылка на целевую таблицу (catalog.schema.table).
        staging_ref: Полная ссылка на staging-таблицу (catalog.schema.staging_name).
    """
    execute_trino_write(
        dst_cur=dst_cur,
        sql=f"DROP TABLE IF EXISTS {staging_ref}",
        op="drop_staging",
        target_ref=staging_ref,
    )
    execute_trino_write(
        dst_cur=dst_cur,
        sql=(
        f"""
        CREATE TABLE {staging_ref}
        AS SELECT * FROM {target_ref}
        WHERE FALSE
        """
        ),
        op="create_staging",
        target_ref=staging_ref,
    )


def ensure_raw_lineage_columns_exist(dst_cur, table_ref: str) -> None:
    """Iceberg: `CREATE TABLE IF NOT EXISTS` не меняет схему. Дополняем старые таблицы колонками RAW."""
    for stmt in (
        f"ALTER TABLE {table_ref} ADD COLUMN IF NOT EXISTS "
        f"{quote_ident(RAW_LOAD_SOURCE_COL)} VARCHAR",
        f"ALTER TABLE {table_ref} ADD COLUMN IF NOT EXISTS "
        f"{quote_ident(RAW_LOADED_AT_COL)} TIMESTAMP(6) WITH TIME ZONE",
    ):
        execute_trino_write(
            dst_cur=dst_cur,
            sql=stmt,
            op="ensure_lineage_cols",
            target_ref=table_ref,
        )


def changed_at_partition_key(value: Any) -> Any:
    """Извлекает ключ партиции (дату) из значения ``changed_at``.

    Для datetime возвращает date (в UTC), для date — как есть, иначе — исходное значение.
    Используется в chunk_rows_by_partition_limit для группировки строк по партициям.

    Args:
        value: Значение колонки changed_at.

    Returns:
        Ключ партиции (обычно date).
    """
    if isinstance(value, datetime):
        normalized = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        return normalized.astimezone(timezone.utc).date()
    if isinstance(value, date):
        return value
    return value


def chunk_rows_by_partition_limit(
    rows: list[tuple[Any, ...]],
    partition_value_index: int,
    max_partitions: int = MAX_EVENT_OPEN_PARTITIONS_PER_QUERY,
) -> Iterable[list[tuple[Any, ...]]]:
    """Разбивает строки на чанки так, чтобы в каждом было не более max_partitions уникальных партиций.

    Это предотвращает открытие слишком многих партиций в одном MERGE-запросе Iceberg.

    Args:
        rows: Строки для разбиения.
        partition_value_index: Индекс колонки changed_at в кортеже.
        max_partitions: Максимальное число уникальных партиций в одном чанке.

    Yields:
        Чанки строк, каждый с не более max_partitions партиций.

    Raises:
        ValueError: Если max_partitions <= 0.
    """
    if max_partitions <= 0:
        raise ValueError("max_partitions must be greater than 0")
    if not rows:
        return
    current_batch: list[tuple[Any, ...]] = []
    current_partitions: set[Any] = set()
    for row in rows:
        partition_key = changed_at_partition_key(row[partition_value_index])
        needs_flush = (
            current_batch
            and partition_key not in current_partitions
            and len(current_partitions) >= max_partitions
        )
        if needs_flush:
            yield current_batch
            current_batch = []
            current_partitions = set()
        current_batch.append(row)
        current_partitions.add(partition_key)
    if current_batch:
        yield current_batch


def execute_trino_write(
    dst_cur,
    sql: str,
    op: str,
    target_ref: str,
    rows: int | None = None,
) -> None:
    """Выполняет SQL-запрос к Trino с логированием.

    При ошибке пишет excerpt SQL (до 4000 символов) и полный traceback.

    Args:
        dst_cur: Курсор Trino.
        sql: SQL-запрос.
        op: Короткое имя операции для логов (drop_staging, merge_events, ...).
        target_ref: Полная ссылка на целевую таблицу.
        rows: Число затрагиваемых строк (для лога); None если не применимо.

    Raises:
        Exception: Пробрасывает оригинальное исключение после логирования.
    """
    if rows is None:
        LOGGER.info("[INFO] trino write start: op=%s target=%s", op, target_ref)
    else:
        LOGGER.info("[INFO] trino write start: op=%s target=%s rows=%s", op, target_ref, rows)
    try:
        dst_cur.execute(sql)
    except Exception:
        excerpt = sql if len(sql) <= 4000 else sql[:4000] + "\n...[sql truncated]"
        LOGGER.error("[ERROR] trino failing sql excerpt: %s", excerpt)
        if rows is None:
            LOGGER.exception("[ERROR] trino write failed: op=%s target=%s", op, target_ref)
        else:
            LOGGER.exception("[ERROR] trino write failed: op=%s target=%s rows=%s", op, target_ref, rows)
        raise
    if rows is None:
        LOGGER.info("[INFO] trino write done: op=%s target=%s", op, target_ref)
    else:
        LOGGER.info("[INFO] trino write done: op=%s target=%s rows=%s", op, target_ref, rows)


def _error_chain(exc: BaseException) -> Iterable[BaseException]:
    """Обходит цепочку исключений (__cause__, __context__) без зацикливания.

    Args:
        exc: Корневое исключение.

    Yields:
        Все исключения в цепочке.
    """
    visited: set[int] = set()
    stack: list[BaseException] = [exc]
    while stack:
        current = stack.pop()
        key = id(current)
        if key in visited:
            continue
        visited.add(key)
        yield current
        cause = getattr(current, "__cause__", None)
        if isinstance(cause, BaseException):
            stack.append(cause)
        context = getattr(current, "__context__", None)
        if isinstance(context, BaseException):
            stack.append(context)


def is_trino_iceberg_commit_conflict(exc: BaseException) -> bool:
    """MERGE raw_load_manifest из многих raw__*-задач: незапартиценные/пересекающиеся записи дают конфликт коммита."""
    markers = (
        "iceberg_commit_error",
        "failed to commit during write",
        "failed to commit the transaction during write",
        "conflicting files",
    )
    for chained in _error_chain(exc):
        ename = getattr(chained, "_error_name", None) or getattr(chained, "error_name", None)
        if ename == "ICEBERG_COMMIT_ERROR":
            return True
        text = str(chained).lower()
        if any(marker in text for marker in markers):
            return True
    return False


def is_trino_connectivity_error(exc: BaseException) -> bool:
    """Определяет, является ли исключение ошибкой сетевой связности Trino.

    Проверяет цепочку исключений на наличие маркеров: проблемы DNS,
    connection refused, HTTP-ошибки пула соединений и т.п.
    Используется для решения о ретрае записи — connectivity-ошибки retryable.

    Args:
        exc: Исключение для проверки.

    Returns:
        True, если ошибка похожа на transient connectivity issue.
    """
    transient_markers = (
        "name resolution",
        "failed to resolve",
        "temporary failure in name resolution",
        "connection refused",
        "max retries exceeded",
        "newconnectionerror",
        "httpconnectionpool",
        "trinoconnectionerror",
        "failed to fetch",
        "failed to execute",
    )
    for chained in _error_chain(exc):
        message = f"{type(chained).__name__}: {chained}".lower()
        if any(marker in message for marker in transient_markers):
            return True
    return False


def execute_trino_write_with_retry(
    dst_cur,
    sql: str,
    op: str,
    target_ref: str,
    rows: int | None = None,
    attempts: int = TRINO_WRITE_RETRY_ATTEMPTS,
) -> None:
    """Выполняет запись в Trino с ретраем при ошибках сетевой связности.

    Использует экспоненциальный backoff: ``base * 2^(attempt-1)``, capped.

    Args:
        dst_cur: Курсор Trino.
        sql: SQL-запрос.
        op: Имя операции.
        target_ref: Ссылка на таблицу.
        rows: Число строк (для лога).
        attempts: Максимальное число попыток.

    Raises:
        Exception: Если все попытки исчерпаны или ошибка не является connectivity-ошибкой.
    """
    max_attempts = max(1, attempts)
    for attempt in range(1, max_attempts + 1):
        try:
            execute_trino_write(
                dst_cur=dst_cur,
                sql=sql,
                op=op,
                target_ref=target_ref,
                rows=rows,
            )
            return
        except Exception as exc:
            if not is_trino_connectivity_error(exc) or attempt >= max_attempts:
                raise
            sleep_seconds = min(
                TRINO_WRITE_RETRY_BASE_SECONDS * (2 ** (attempt - 1)),
                TRINO_WRITE_RETRY_MAX_SLEEP_SECONDS,
            )
            LOGGER.warning(
                "[WARN] trino write retry due to connectivity issue: op=%s target=%s attempt=%s/%s sleep=%.1fs error=%s",
                op,
                target_ref,
                attempt,
                max_attempts,
                sleep_seconds,
                exc,
            )
            time_module.sleep(sleep_seconds)


def execute_trino_write_with_iceberg_retry(
    dst_cur,
    sql: str,
    *,
    target_ref: str,
    op: str,
    attempts: int | None = None,
) -> None:
    """Выполняет запись в Iceberg с ретраем при ICEBERG_COMMIT_ERROR и ошибках сети.

    Добавляет случайный jitter (до 35% от sleep) для снижения вероятности повторных
    конфликтов при параллельной записи из многих задач (raw__/dds__).

    Args:
        dst_cur: Курсор Trino.
        sql: SQL-запрос.
        target_ref: Ссылка на таблицу.
        op: Имя операции.
        attempts: Число попыток (None = стандартное TRINO_WRITE_RETRY_ATTEMPTS).

    Raises:
        Exception: Если все попытки исчерпаны или ошибка не recoverable.
    """
    """Ретрай одиночного SQL к Iceberg: ICEBERG_COMMIT_ERROR при параллельных raw__/dds__ + Trino-сеть."""
    max_attempts = max(1, attempts if attempts is not None else TRINO_WRITE_RETRY_ATTEMPTS)
    for attempt in range(1, max_attempts + 1):
        try:
            execute_trino_write(
                dst_cur=dst_cur,
                sql=sql,
                op=op,
                target_ref=target_ref,
            )
            return
        except Exception as exc:
            recoverable = is_trino_iceberg_commit_conflict(exc) or is_trino_connectivity_error(exc)
            if not recoverable or attempt >= max_attempts:
                raise
            sleep_seconds = min(
                TRINO_WRITE_RETRY_BASE_SECONDS * (2 ** (attempt - 1)),
                TRINO_WRITE_RETRY_MAX_SLEEP_SECONDS,
            )
            sleep_seconds += random.uniform(0, min(2.5, sleep_seconds * 0.35))
            LOGGER.warning(
                "[WARN] trino iceberg write retry: op=%s target=%s attempt=%s/%s sleep=%.2fs error=%s",
                op,
                target_ref,
                attempt,
                max_attempts,
                sleep_seconds,
                exc,
            )
            time_module.sleep(sleep_seconds)


def drop_staging_best_effort(dst_cur, staging_ref: str) -> None:
    """Удаляет staging-таблицу; ошибки глушит (best-effort).

    Staging-таблицы — временные, их потеря некритична.

    Args:
        dst_cur: Курсор Trino.
        staging_ref: Полная ссылка на staging-таблицу.
    """
    try:
        execute_trino_write(
            dst_cur=dst_cur,
            sql=f"DROP TABLE IF EXISTS {staging_ref}",
            op="drop_staging",
            target_ref=staging_ref,
        )
    except Exception as exc:
        LOGGER.warning(
            "[WARN] skip drop staging due to trino connectivity issue: target=%s error=%s",
            staging_ref,
            exc,
        )


# ---------- Запись в Trino батчами с ретраями ----------


def insert_values_batches(
    dst_cur,
    table_ref: str,
    rows: list[Any],
    batch_size: int,
    row_renderer: Callable[[Any], str],
    max_query_text_chars: int = MAX_TRINO_INSERT_QUERY_TEXT,
) -> None:
    """Вставляет строки в таблицу батчами, соблюдая лимиты на размер запроса и число строк.

    Каждый батч ограничен двумя параметрами:
    1. ``batch_size`` (или MAX_TRINO_ROWS_PER_WRITE) — максимум строк в одном INSERT.
    2. ``max_query_text_chars`` — максимальная длина SQL-запроса в символах.

    Как только любой из лимитов превышен, текущий батч отправляется и начинается новый.

    Args:
        dst_cur: Курсор Trino.
        table_ref: Полная ссылка на таблицу.
        rows: Строки для вставки (произвольные объекты, сериализуемые row_renderer).
        batch_size: Целевой размер батча в строках.
        row_renderer: Функция, преобразующая один элемент rows в SQL-строку значений.
        max_query_text_chars: Максимальная длина SQL-запроса.

    Raises:
        ValueError: Если batch_size или max_query_text_chars <= 0.
    """
    if max_query_text_chars <= 0:
        raise ValueError("max_query_text_chars must be greater than 0")
    if not rows:
        return
    LOGGER.info(
        "[INFO] trino insert batches start: table=%s rows_total=%s batch_size=%s max_query_text_chars=%s",
        table_ref,
        len(rows),
        batch_size,
        max_query_text_chars,
    )
    effective_batch_size = min(batch_size, max(1, MAX_TRINO_ROWS_PER_WRITE))
    if effective_batch_size != batch_size:
        LOGGER.info(
            "[INFO] trino insert batch_size capped: table=%s requested=%s effective=%s",
            table_ref,
            batch_size,
            effective_batch_size,
        )

    prefix_sql = f"INSERT INTO {table_ref} VALUES "
    max_values_chars = max_query_text_chars - len(prefix_sql)
    if max_values_chars <= 0:
        raise ValueError("max_query_text_chars is too small for INSERT statement prefix")

    current_rendered_rows: list[str] = []
    current_chars = 0
    statement_count = 0
    rows_written = 0

    for row in rows:
        rendered_row = row_renderer(row)
        separator_chars = 2 if current_rendered_rows else 0  # ",\n"
        rendered_len = len(rendered_row)
        fits_current_query = current_chars + separator_chars + rendered_len <= max_values_chars
        exceeds_rows_limit = len(current_rendered_rows) >= effective_batch_size

        if current_rendered_rows and (exceeds_rows_limit or not fits_current_query):
            values_sql = ",\n".join(current_rendered_rows)
            statement_rows = len(current_rendered_rows)
            execute_trino_write_with_retry(
                dst_cur=dst_cur,
                sql=f"{prefix_sql}{values_sql}",
                op="insert_values",
                target_ref=table_ref,
                rows=statement_rows,
            )
            statement_count += 1
            rows_written += statement_rows
            current_rendered_rows = []
            current_chars = 0

        current_rendered_rows.append(rendered_row)
        if current_chars:
            current_chars += 2
        current_chars += rendered_len

    if current_rendered_rows:
        values_sql = ",\n".join(current_rendered_rows)
        statement_rows = len(current_rendered_rows)
        execute_trino_write_with_retry(
            dst_cur=dst_cur,
            sql=f"{prefix_sql}{values_sql}",
            op="insert_values",
            target_ref=table_ref,
            rows=statement_rows,
        )
        statement_count += 1
        rows_written += statement_rows
    LOGGER.info(
        "[INFO] trino insert batches done: table=%s statements=%s rows_written=%s",
        table_ref,
        statement_count,
        rows_written,
    )


# ---------- Чтение журнала изменений (incremental) в источнике ----------


def fetch_incremental_rows(
    src_cur,
    day_start: datetime,
    day_end: datetime,
    tables: list[str],
    source_fetch_batch_size: int,
    source_log_schema: str,
    source_log_table: str,
) -> list[tuple[Any, ...]]:
    """Читает все строки из журнала изменений (global_log) за период одним запросом.

    Возвращает полный список — используется в bulk-режиме (load_incremental_table_bulk).

    Args:
        src_cur: Курсор PostgreSQL.
        day_start: Начало окна (UTC).
        day_end: Конец окна (UTC, исключительно).
        tables: Список имён таблиц для фильтрации (``schema.table``).
        source_fetch_batch_size: Размер fetchmany.
        source_log_schema: Схема журнала.
        source_log_table: Таблица журнала (обычно global_log).

    Returns:
        Полный список кортежей с полями: id, table, action, stamp, pkey,
        old_values, new_values, old_diff, new_diff (все hstore → jsonb).
    """
    if not tables:
        return []
    source_log_ref = f"{quote_ident(source_log_schema)}.{quote_ident(source_log_table)}"
    src_cur.execute(
        f"""
        SELECT
            gl.id,
            gl."table",
            gl.action,
            gl.stamp,
            gl.pkey,
            hstore_to_json(gl.old_values)::jsonb AS old_values,
            hstore_to_json(gl.new_values)::jsonb AS new_values,
            hstore_to_json(gl.old_diff)::jsonb AS old_diff,
            hstore_to_json(gl.new_diff)::jsonb AS new_diff
        FROM {source_log_ref} gl
        WHERE gl.stamp >= %s
          AND gl.stamp < %s
          AND gl."table" = ANY(%s::text[])
        """,
        (day_start, day_end, tables),
    )
    result: list[tuple[Any, ...]] = []
    while True:
        rows = src_cur.fetchmany(source_fetch_batch_size)
        if not rows:
            break
        result.extend(rows)
    return result


def iter_incremental_rows(
    src_cur,
    day_start: datetime,
    day_end: datetime,
    tables: list[str],
    source_fetch_batch_size: int,
    source_log_schema: str,
    source_log_table: str,
) -> Iterable[list[tuple[Any, ...]]]:
    """Потоковое чтение журнала изменений с пагинацией по (stamp, id).

    В отличие от fetch_incremental_rows, не загружает все строки в память —
    возвращает батчи через yield. Использует keyset-пагинацию (stamp > last_stamp
    OR (stamp = last_stamp AND id > last_id)) для эффективного обхода больших окон.

    Args:
        src_cur: Курсор PostgreSQL.
        day_start: Начало окна (UTC).
        day_end: Конец окна (UTC, исключительно).
        tables: Список имён таблиц для фильтрации.
        source_fetch_batch_size: Размер одного батча.
        source_log_schema: Схема журнала.
        source_log_table: Таблица журнала.

    Yields:
        Списки кортежей (батчи), каждый не более source_fetch_batch_size строк.
    """
    if not tables:
        return
    source_log_ref = f"{quote_ident(source_log_schema)}.{quote_ident(source_log_table)}"
    fetch_started_at = time_module.monotonic()
    fetched_rows_total = 0
    fetched_batches_total = 0
    LOGGER.info(
        "[INFO] source incremental fetch start: source=%s window=[%s..%s) tables=%s batch_size=%s",
        source_log_ref,
        day_start.isoformat(),
        day_end.isoformat(),
        len(tables),
        source_fetch_batch_size,
    )
    last_stamp: datetime | None = None
    last_id: int | None = None
    while True:
        if last_stamp is None or last_id is None:
            src_cur.execute(
                f"""
                SELECT
                    gl.id,
                    gl."table",
                    gl.action,
                    gl.stamp,
                    gl.pkey,
                    hstore_to_json(gl.old_values)::jsonb AS old_values,
                    hstore_to_json(gl.new_values)::jsonb AS new_values,
                    hstore_to_json(gl.old_diff)::jsonb AS old_diff,
                    hstore_to_json(gl.new_diff)::jsonb AS new_diff
                FROM {source_log_ref} gl
                WHERE gl.stamp >= %s
                  AND gl.stamp < %s
                  AND gl."table" = ANY(%s::text[])
                ORDER BY gl.stamp, gl.id
                LIMIT %s
                """,
                (day_start, day_end, tables, source_fetch_batch_size),
            )
        else:
            src_cur.execute(
                f"""
                SELECT
                    gl.id,
                    gl."table",
                    gl.action,
                    gl.stamp,
                    gl.pkey,
                    hstore_to_json(gl.old_values)::jsonb AS old_values,
                    hstore_to_json(gl.new_values)::jsonb AS new_values,
                    hstore_to_json(gl.old_diff)::jsonb AS old_diff,
                    hstore_to_json(gl.new_diff)::jsonb AS new_diff
                FROM {source_log_ref} gl
                WHERE gl.stamp >= %s
                  AND gl.stamp < %s
                  AND gl."table" = ANY(%s::text[])
                  AND (
                    gl.stamp > %s
                    OR (gl.stamp = %s AND gl.id > %s)
                  )
                ORDER BY gl.stamp, gl.id
                LIMIT %s
                """,
                (day_start, day_end, tables, last_stamp, last_stamp, last_id, source_fetch_batch_size),
            )
        rows = src_cur.fetchall()
        if not rows:
            break
        fetched_batches_total += 1
        fetched_rows_total += len(rows)
        last_stamp = rows[-1][3]
        last_id = int(rows[-1][0])
        LOGGER.info(
            "[INFO] source incremental fetch batch: source=%s batch=%s rows=%s rows_total=%s last_stamp=%s last_id=%s",
            source_log_ref,
            fetched_batches_total,
            len(rows),
            fetched_rows_total,
            last_stamp.isoformat() if isinstance(last_stamp, datetime) else last_stamp,
            last_id,
        )
        yield rows
    LOGGER.info(
        "[INFO] source incremental fetch done: source=%s batches=%s rows_total=%s elapsed_sec=%.2f",
        source_log_ref,
        fetched_batches_total,
        fetched_rows_total,
        time_module.monotonic() - fetch_started_at,
    )


def count_incremental_rows(
    src_cur,
    day_start: datetime,
    day_end: datetime,
    tables: list[str],
    source_log_schema: str,
    source_log_table: str,
) -> int:
    """Быстрый подсчёт числа строк в журнале за период (без загрузки данных).

    Args:
        src_cur: Курсор PostgreSQL.
        day_start: Начало окна (UTC).
        day_end: Конец окна (UTC, исключительно).
        tables: Список имён таблиц.
        source_log_schema: Схема журнала.
        source_log_table: Таблица журнала.

    Returns:
        Число строк (0 если таблиц нет).
    """
    if not tables:
        return 0
    source_log_ref = f"{quote_ident(source_log_schema)}.{quote_ident(source_log_table)}"
    started_at = time_module.monotonic()
    LOGGER.info(
        "[INFO] source incremental count start: source=%s window=[%s..%s) tables=%s",
        source_log_ref,
        day_start.isoformat(),
        day_end.isoformat(),
        len(tables),
    )
    src_cur.execute(
        f"""
        SELECT count(*)
        FROM {source_log_ref} gl
        WHERE gl.stamp >= %s
          AND gl.stamp < %s
          AND gl."table" = ANY(%s::text[])
        """,
        (day_start, day_end, tables),
    )
    count_value = int(src_cur.fetchone()[0])
    LOGGER.info(
        "[INFO] source incremental count done: source=%s rows=%s elapsed_sec=%.2f",
        source_log_ref,
        count_value,
        time_module.monotonic() - started_at,
    )
    return count_value


def map_postgres_type_to_trino(
    data_type: str,
    udt_name: str,
    numeric_precision: int | None,
    numeric_scale: int | None,
    datetime_precision: int | None,
) -> str:
    """Отображает PostgreSQL-тип колонки в соответствующий тип Trino.

    Учитывает как стандартные имена типов (data_type), так и пользовательские
    (udt_name — например, uuid может быть определён через домен).
    Для неизвестных типов возвращает VARCHAR.

    Args:
        data_type: Стандартное имя типа из information_schema (integer, varchar, ...).
        udt_name: Имя пользовательского типа (может отличаться для доменов).
        numeric_precision: Точность для numeric.
        numeric_scale: Масштаб для numeric.
        datetime_precision: Точность для time/timestamp.

    Returns:
        Строка с типом Trino (VARCHAR, BIGINT, TIMESTAMP(6) WITH TIME ZONE, ...).
    """
    normalized = data_type.lower()
    udt = udt_name.lower()
    if normalized in {"character varying", "character", "text", "json", "jsonb", "array"}:
        return "VARCHAR"
    if normalized == "boolean":
        return "BOOLEAN"
    if normalized == "smallint":
        return "SMALLINT"
    if normalized == "integer":
        return "INTEGER"
    if normalized == "bigint":
        return "BIGINT"
    if normalized == "real":
        return "REAL"
    if normalized == "double precision":
        return "DOUBLE"
    if normalized == "numeric":
        if numeric_precision is not None and numeric_scale is not None:
            return f"DECIMAL({numeric_precision},{numeric_scale})"
        return "DOUBLE"
    if normalized == "date":
        return "DATE"
    if normalized == "time without time zone":
        precision = min(6, datetime_precision or 6)
        return f"TIME({precision})"
    if normalized == "timestamp without time zone":
        precision = min(6, datetime_precision or 6)
        return f"TIMESTAMP({precision})"
    if normalized == "timestamp with time zone":
        precision = min(6, datetime_precision or 6)
        return f"TIMESTAMP({precision}) WITH TIME ZONE"
    if normalized == "uuid" or udt == "uuid":
        return "UUID"
    if normalized == "bytea":
        return "VARBINARY"
    return "VARCHAR"


def fetch_source_columns(src_cur, source_schema: str, source_table: str) -> list[SourceColumn]:
    """Получает метаданные колонок таблицы-источника из information_schema.

    Args:
        src_cur: Курсор PostgreSQL.
        source_schema: Схема источника.
        source_table: Таблица источника.

    Returns:
        Список SourceColumn с именами и маппингом на типы Trino.

    Raises:
        ValueError: Если таблица не найдена или не имеет колонок.
    """
    src_cur.execute(
        """
        SELECT
            c.column_name,
            c.data_type,
            c.udt_name,
            c.numeric_precision,
            c.numeric_scale,
            c.datetime_precision
        FROM information_schema.columns c
        WHERE c.table_schema = %s
          AND c.table_name = %s
        ORDER BY c.ordinal_position
        """,
        (source_schema, source_table),
    )
    columns: list[SourceColumn] = []
    for (
        column_name,
        data_type,
        udt_name,
        numeric_precision,
        numeric_scale,
        datetime_precision,
    ) in src_cur.fetchall():
        columns.append(
            SourceColumn(
                name=str(column_name),
                trino_type=map_postgres_type_to_trino(
                    data_type=str(data_type),
                    udt_name=str(udt_name),
                    numeric_precision=numeric_precision,
                    numeric_scale=numeric_scale,
                    datetime_precision=datetime_precision,
                ),
            )
        )
    if not columns:
        raise ValueError(f"Source table not found or has no columns: {source_schema}.{source_table}")
    return columns


def fetch_source_primary_key(src_cur, source_schema: str, source_table: str) -> list[str]:
    """Извлекает имена колонок первичного ключа из pg_index.

    Args:
        src_cur: Курсор PostgreSQL.
        source_schema: Схема источника.
        source_table: Таблица источника.

    Returns:
        Список имён колонок PK в порядке их определения.
    """
    src_cur.execute(
        """
        SELECT a.attname
        FROM pg_index i
        JOIN pg_class c
            ON c.oid = i.indrelid
        JOIN pg_namespace n
            ON n.oid = c.relnamespace
        JOIN unnest(i.indkey) WITH ORDINALITY AS keynums(attnum, ord)
            ON TRUE
        JOIN pg_attribute a
            ON a.attrelid = c.oid
           AND a.attnum = keynums.attnum
        WHERE i.indisprimary
          AND n.nspname = %s
          AND c.relname = %s
        ORDER BY keynums.ord
        """,
        (source_schema, source_table),
    )
    return [str(row[0]) for row in src_cur.fetchall()]


def target_table_exists(dst_cur, raw_schema: str, table_name: str) -> bool:
    """Проверяет существование таблицы в information_schema.

    Args:
        dst_cur: Курсор Trino.
        raw_schema: Схема.
        table_name: Имя таблицы.

    Returns:
        True, если таблица существует.
    """
    dst_cur.execute(
        f"""
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = {sql_string(raw_schema)}
          AND table_name = {sql_string(table_name)}
        LIMIT 1
        """
    )
    return dst_cur.fetchone() is not None


def bump_cast_to_null(counter: list[int] | None) -> None:
    """Инкрементирует счётчик неудачных приведений типов (если передан).

    Используется в to_trino_literal: когда значение не может быть приведено
    к целевому типу, оно заменяется на NULL, а счётчик увеличивается.

    Args:
        counter: Список из одного элемента [count] или None.
    """
    if counter is not None:
        counter[0] += 1


def to_trino_literal(value: Any, trino_type: str, cast_to_null_counter: list[int] | None = None) -> str:
    """Преобразует Python-значение в SQL-литерал для Trino указанного типа.

    При невозможности приведения (строку "abc" в BIGINT, бесконечный float и т.п.)
    возвращает CAST(NULL AS <type>) и инкрементирует cast_to_null_counter.

    Поддерживает: VARCHAR, BOOLEAN, SMALLINT, INTEGER, BIGINT, REAL, DOUBLE,
    DECIMAL(p,s), UUID, DATE, TIME, TIMESTAMP.

    Args:
        value: Исходное значение.
        trino_type: Целевой тип Trino (строка).
        cast_to_null_counter: Опциональный счётчик неудачных приведений.

    Returns:
        SQL-литерал (``'text'``, ``123``, ``TRUE``, ``CAST(NULL AS ...)``, ...).
    """
    normalized_type = trino_type.upper()
    null_literal = f"CAST(NULL AS {trino_type})"
    if value is None:
        return null_literal
    if normalized_type.startswith("VARCHAR"):
        return sql_string(str(value))
    if normalized_type == "BOOLEAN":
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        lowered = str(value).strip().lower()
        if lowered in {"true", "t", "1", "yes", "y"}:
            return "TRUE"
        if lowered in {"false", "f", "0", "no", "n"}:
            return "FALSE"
        bump_cast_to_null(cast_to_null_counter)
        return null_literal
    if normalized_type in {"SMALLINT", "INTEGER", "BIGINT"}:
        if isinstance(value, bool):
            return "1" if value else "0"
        if isinstance(value, int):
            return str(value)
        if isinstance(value, float):
            if not math.isfinite(value):
                bump_cast_to_null(cast_to_null_counter)
                return null_literal
            if value.is_integer():
                return str(int(value))
            bump_cast_to_null(cast_to_null_counter)
            return null_literal
        try:
            return str(int(str(value).strip()))
        except (TypeError, ValueError):
            bump_cast_to_null(cast_to_null_counter)
            return null_literal
    if normalized_type in {"REAL", "DOUBLE"} or normalized_type.startswith("DECIMAL"):
        if isinstance(value, bool):
            return "1" if value else "0"
        if isinstance(value, int):
            return str(value)
        if isinstance(value, float):
            if not math.isfinite(value):
                bump_cast_to_null(cast_to_null_counter)
                return null_literal
            return str(value)
        try:
            decimal_value = Decimal(str(value).strip().replace(",", "."))
            if not decimal_value.is_finite():
                bump_cast_to_null(cast_to_null_counter)
                return null_literal
            return str(decimal_value)
        except (InvalidOperation, TypeError, ValueError):
            bump_cast_to_null(cast_to_null_counter)
            return null_literal
    if normalized_type == "UUID":
        try:
            return sql_string(str(UUID(str(value).strip())))
        except (ValueError, TypeError, AttributeError):
            bump_cast_to_null(cast_to_null_counter)
            return null_literal
    if isinstance(value, datetime):
        return f"TRY_CAST({sql_string(value.isoformat(sep=' '))} AS {trino_type})"
    if isinstance(value, date):
        return f"TRY_CAST({sql_string(value.isoformat())} AS {trino_type})"
    if isinstance(value, time):
        return f"TRY_CAST({sql_string(value.isoformat())} AS {trino_type})"
    if isinstance(value, str):
        return f"TRY_CAST({sql_string(value)} AS {trino_type})"
    return f"TRY_CAST({sql_string(str(value))} AS {trino_type})"


# ---------- Разбор hstore/json полей событий и сборка строк для RAW ----------


def extract_change_dict(value: Any) -> dict[str, Any]:
    """Извлекает словарь изменений из JSONB-поля события.

    Если значение — dict, возвращает его с ключами-строками. Иначе — пустой словарь.

    Args:
        value: Значение из old_values/new_values/old_diff/new_diff.

    Returns:
        Словарь колонка→значение (может быть пустым).
    """


def extract_pk_values(
    pk_columns: list[str],
    candidates: list[dict[str, Any]],
) -> dict[str, Any] | None:
    """Пытается извлечь значения первичного ключа из списка кандидатов-словарей.

    Перебирает словари по порядку; возвращает первый, где ВСЕ колонки PK присутствуют
    и не None.

    Args:
        pk_columns: Список имён колонок первичного ключа.
        candidates: Список словарей (new_values, old_values, new_diff, old_diff).

    Returns:
        Словарь {column: value} или None, если ни один кандидат не содержит полного PK.
    """
    for candidate in candidates:
        if not candidate:
            continue
        if all(column in candidate and candidate[column] is not None for column in pk_columns):
            return {column: candidate[column] for column in pk_columns}
    return None


def ensure_incremental_raw_table(
    src_cur,
    dst_cur,
    catalog: str,
    raw_schema: str,
    table: TableConfig,
) -> IncrementalMirrorMeta:
    """Создаёт (при необходимости) и возвращает метаданные инкрементального зеркала.

    Если целевая таблица не существует — создаёт её с партиционированием по bucket(pk, 64).
    Если существует — дополняет колонками raw_load_source/raw_load_loaded_at.

    Args:
        src_cur: Курсор PostgreSQL для получения метаданных колонок и PK.
        dst_cur: Курсор Trino.
        catalog: Каталог Iceberg.
        raw_schema: Схема RAW.
        table: Конфигурация таблицы.

    Returns:
        Заполненный IncrementalMirrorMeta.

    Raises:
        ValueError: Если у таблицы нет первичного ключа в источнике.
    """
    source_schema, source_table = split_table_name(table.name)
    columns = fetch_source_columns(src_cur, source_schema=source_schema, source_table=source_table)
    primary_key = fetch_source_primary_key(src_cur, source_schema=source_schema, source_table=source_table)
    if not primary_key:
        raise ValueError(f"Incremental table {table.name} must have a primary key in source DB")

    columns.extend(
        [
            SourceColumn(name=RAW_LOAD_SOURCE_COL, trino_type="varchar"),
            SourceColumn(name=RAW_LOADED_AT_COL, trino_type="timestamp(6) with time zone"),
        ]
    )

    target_table = source_table
    target_ref = quote_table(catalog, raw_schema, target_table)
    exists = target_table_exists(dst_cur, raw_schema=raw_schema, table_name=target_table)
    meta = IncrementalMirrorMeta(
        source_schema=source_schema,
        source_table=source_table,
        target_table=target_table,
        target_ref=target_ref,
        columns=columns,
        primary_key=primary_key,
        created=not exists,
    )
    if not exists:
        column_defs_sql = ",\n".join(
            f"{quote_ident(column.name)} {column.trino_type}" for column in columns
        )
        partition_pk = next((column for column in primary_key if column.lower() == "id"), primary_key[0])
        partition_expr = f"bucket({quote_ident(partition_pk)}, 64)"
        dst_cur.execute(
            f"""
            CREATE TABLE {target_ref} (
                {column_defs_sql}
            )
            WITH (
                partitioning = ARRAY[{sql_string(partition_expr)}]
            )
            """
        )
    # Старые зеркала incremental без raw_load_*: CTAS staging иначе уже не совпадает с meta.columns.
    ensure_raw_lineage_columns_exist(dst_cur, target_ref)
    return meta


def ensure_incremental_events_table(
    dst_cur,
    catalog: str,
    raw_schema: str,
    table: TableConfig,
) -> str:
    """Создаёт (если нет) таблицу *__events с партиционированием по day(changed_at).

    Args:
        dst_cur: Курсор Trino.
        catalog: Каталог Iceberg.
        raw_schema: Схема RAW.
        table: Конфигурация таблицы.

    Returns:
        Полная ссылка (catalog.schema.table) на таблицу событий.
    """
    events_table = target_table_name(table.name, "events")
    events_ref = quote_table(catalog, raw_schema, events_table)
    dst_cur.execute(
        f"""
            CREATE TABLE IF NOT EXISTS {events_ref} (
                log_id BIGINT,
                source_table VARCHAR,
                source_pkey VARCHAR,
                op VARCHAR,
                changed_at TIMESTAMP(6) WITH TIME ZONE,
                old_values VARCHAR,
                new_values VARCHAR,
                old_diff VARCHAR,
                new_diff VARCHAR,
                ingested_at TIMESTAMP(6) WITH TIME ZONE,
                {quote_ident(RAW_LOAD_SOURCE_COL)} VARCHAR,
                {quote_ident(RAW_LOADED_AT_COL)} TIMESTAMP(6) WITH TIME ZONE
            )
        WITH (
            partitioning = ARRAY['day(changed_at)']
        )
        """
    )
    ensure_raw_lineage_columns_exist(dst_cur, events_ref)
    return events_ref


def ensure_incremental_invalid_table(
    dst_cur,
    catalog: str,
    raw_schema: str,
    table: TableConfig,
) -> str:
    """Создаёт (если нет) таблицу *__events_invalid для невалидных событий.

    Партиционирована по day(changed_at), имеет дополнительную колонку reason.

    Args:
        dst_cur: Курсор Trino.
        catalog: Каталог Iceberg.
        raw_schema: Схема RAW.
        table: Конфигурация таблицы.

    Returns:
        Полная ссылка на таблицу невалидных событий.
    """
    invalid_table = target_table_name(table.name, "events_invalid")
    invalid_ref = quote_table(catalog, raw_schema, invalid_table)
    dst_cur.execute(
        f"""
            CREATE TABLE IF NOT EXISTS {invalid_ref} (
                log_id BIGINT,
                source_table VARCHAR,
                source_pkey VARCHAR,
                op VARCHAR,
                changed_at TIMESTAMP(6) WITH TIME ZONE,
                reason VARCHAR,
                old_values VARCHAR,
                new_values VARCHAR,
                old_diff VARCHAR,
                new_diff VARCHAR,
                ingested_at TIMESTAMP(6) WITH TIME ZONE,
                {quote_ident(RAW_LOAD_SOURCE_COL)} VARCHAR,
                {quote_ident(RAW_LOADED_AT_COL)} TIMESTAMP(6) WITH TIME ZONE
            )
        WITH (
            partitioning = ARRAY['day(changed_at)']
        )
        """
    )
    ensure_raw_lineage_columns_exist(dst_cur, invalid_ref)
    return invalid_ref


def write_incremental_events(
    dst_cur,
    catalog: str,
    raw_schema: str,
    table: TableConfig,
    rows: list[tuple[Any, ...]],
    target_insert_batch_size: int,
    lineage_source_id: str,
) -> int:
    """Записывает события в *__events через staging + MERGE (дедупликация по log_id).

    Алгоритм:
    1. Создаёт staging-таблицу — копию структуры events.
    2. Вставляет строки в staging батчами (insert_values_batches).
    3. MERGE из staging в основную таблицу: WHEN NOT MATCHED BY log_id → INSERT.
    4. Удаляет staging.

    Строки разбиваются по партициям changed_at для соблюдения лимита открытых партиций.

    Args:
        dst_cur: Курсор Trino.
        catalog: Каталог Iceberg.
        raw_schema: Схема RAW.
        table: Конфигурация таблицы.
        rows: Список кортежей событий (log_id, source_table, source_pkey, op, ...).
        target_insert_batch_size: Размер батча вставки.
        lineage_source_id: Идентификатор источника.

    Returns:
        Число фактически вставленных новых событий (не дубликатов).
    """
    if not rows:
        return 0
    events_ref = ensure_incremental_events_table(
        dst_cur=dst_cur,
        catalog=catalog,
        raw_schema=raw_schema,
        table=table,
    )
    ingested_at = datetime.now(timezone.utc)
    event_rows = []
    for row in rows:
        (
            log_id,
            source_table,
            action,
            stamp,
            source_pkey,
            old_values,
            new_values,
            old_diff,
            new_diff,
        ) = row
        event_rows.append(
            (
                int(log_id),
                str(source_table),
                None if source_pkey is None else str(source_pkey),
                str(action).upper(),
                stamp,
                old_values,
                new_values,
                old_diff,
                new_diff,
                ingested_at,
                lineage_source_id,
                ingested_at,
            )
        )
    inserted_count_total = 0
    max_rows_per_event_merge = max(1, min(MAX_EVENT_ROWS_PER_MERGE, MAX_TRINO_ROWS_PER_WRITE))
    for partition_batch in chunk_rows_by_partition_limit(event_rows, partition_value_index=4):
        for event_batch in chunked(partition_batch, max_rows_per_event_merge):
            LOGGER.info(
                "[INFO] incremental events merge chunk: table=%s rows=%s max_rows_per_merge=%s",
                table.name,
                len(event_batch),
                max_rows_per_event_merge,
            )
            events_staging = make_staging_table_name(target_table_name(table.name, "events"), "events")
            events_staging_ref = quote_table(catalog, raw_schema, events_staging)
            create_staging_table(dst_cur, events_ref, events_staging_ref)
            try:
                insert_values_batches(
                    dst_cur=dst_cur,
                    table_ref=events_staging_ref,
                    rows=event_batch,
                    batch_size=target_insert_batch_size,
                    row_renderer=lambda item: (
                        f"({sql_bigint(item[0])}, {sql_string(item[1])}, {sql_string(item[2])}, "
                        f"{sql_string(item[3])}, {sql_timestamptz(item[4])}, {sql_json_text(item[5])}, "
                        f"{sql_json_text(item[6])}, {sql_json_text(item[7])}, {sql_json_text(item[8])}, "
                        f"{sql_timestamptz(item[9])}, {sql_string(item[10])}, {sql_timestamptz(item[11])})"
                    ),
                )
                dst_cur.execute(
                    f"""
                    SELECT count(*)
                    FROM {events_staging_ref} s
                    LEFT JOIN {events_ref} t
                        ON t.log_id = s.log_id
                    WHERE t.log_id IS NULL
                    """
                )
                inserted_count_total += int(dst_cur.fetchone()[0])
                execute_trino_write_with_retry(
                    dst_cur=dst_cur,
                    sql=(
                    f"""
                    MERGE INTO {events_ref} t
                    USING {events_staging_ref} s
                    ON t.log_id = s.log_id
                    WHEN NOT MATCHED THEN
                        INSERT (
                            log_id, source_table, source_pkey, op, changed_at,
                            old_values, new_values, old_diff, new_diff, ingested_at,
                            {quote_ident(RAW_LOAD_SOURCE_COL)}, {quote_ident(RAW_LOADED_AT_COL)}
                        )
                        VALUES (
                            s.log_id, s.source_table, s.source_pkey, s.op, s.changed_at,
                            s.old_values, s.new_values, s.old_diff, s.new_diff, s.ingested_at,
                            s.{quote_ident(RAW_LOAD_SOURCE_COL)}, s.{quote_ident(RAW_LOADED_AT_COL)}
                        )
                    """
                    ),
                    op="merge_events",
                    target_ref=events_ref,
                    rows=len(event_batch),
                )
            finally:
                drop_staging_best_effort(dst_cur=dst_cur, staging_ref=events_staging_ref)
    return inserted_count_total


def write_invalid_incremental_events(
    dst_cur,
    catalog: str,
    raw_schema: str,
    table: TableConfig,
    rows: list[tuple[Any, ...]],
    target_insert_batch_size: int,
    lineage_source_id: str,
) -> int:
    """Записывает невалидные события в *__events_invalid (INSERT, без дедупликации).

    Структура аналогична write_incremental_events, но используется INSERT (а не MERGE),
    так как невалидные события не требуют дедупликации.

    Args:
        dst_cur: Курсор Trino.
        catalog: Каталог Iceberg.
        raw_schema: Схема RAW.
        table: Конфигурация таблицы.
        rows: Список невалидных событий (с полем reason).
        target_insert_batch_size: Размер батча.
        lineage_source_id: Идентификатор источника.

    Returns:
        Число записанных строк.
    """
    if not rows:
        return 0
    invalid_ref = ensure_incremental_invalid_table(
        dst_cur=dst_cur,
        catalog=catalog,
        raw_schema=raw_schema,
        table=table,
    )
    ingested_at = datetime.now(timezone.utc)
    prepared_rows = []
    for row in rows:
        (
            log_id,
            source_table,
            source_pkey,
            op,
            changed_at,
            reason,
            old_values,
            new_values,
            old_diff,
            new_diff,
        ) = row
        prepared_rows.append(
            (
                None if log_id is None else int(log_id),
                str(source_table),
                None if source_pkey is None else str(source_pkey),
                str(op),
                changed_at,
                str(reason),
                old_values,
                new_values,
                old_diff,
                new_diff,
                ingested_at,
                lineage_source_id,
                ingested_at,
            )
        )
    inserted_total = 0
    max_rows_per_invalid_insert = max(1, min(MAX_EVENT_ROWS_PER_MERGE, MAX_TRINO_ROWS_PER_WRITE))
    for partition_batch in chunk_rows_by_partition_limit(prepared_rows, partition_value_index=4):
        for invalid_batch in chunked(partition_batch, max_rows_per_invalid_insert):
            invalid_staging = make_staging_table_name(target_table_name(table.name, "events_invalid"), "invalid")
            invalid_staging_ref = quote_table(catalog, raw_schema, invalid_staging)
            create_staging_table(dst_cur, invalid_ref, invalid_staging_ref)
            try:
                insert_values_batches(
                    dst_cur=dst_cur,
                    table_ref=invalid_staging_ref,
                    rows=invalid_batch,
                    batch_size=target_insert_batch_size,
                    row_renderer=lambda item: (
                        f"({sql_bigint(item[0])}, {sql_string(item[1])}, {sql_string(item[2])}, "
                        f"{sql_string(item[3])}, {sql_timestamptz(item[4])}, {sql_string(item[5])}, "
                        f"{sql_json_text(item[6])}, {sql_json_text(item[7])}, {sql_json_text(item[8])}, "
                        f"{sql_json_text(item[9])}, {sql_timestamptz(item[10])}, {sql_string(item[11])}, "
                        f"{sql_timestamptz(item[12])})"
                    ),
                )
                execute_trino_write_with_retry(
                    dst_cur=dst_cur,
                    sql=f"INSERT INTO {invalid_ref} SELECT * FROM {invalid_staging_ref}",
                    op="insert_invalid_events",
                    target_ref=invalid_ref,
                    rows=len(invalid_batch),
                )
                inserted_total += len(invalid_batch)
            finally:
                drop_staging_best_effort(dst_cur=dst_cur, staging_ref=invalid_staging_ref)
    return inserted_total


def apply_delete_ops(
    dst_cur,
    catalog: str,
    raw_schema: str,
    meta: IncrementalMirrorMeta,
    delete_keys: list[dict[str, Any]],
    target_insert_batch_size: int,
    cast_to_null_counter: list[int] | None = None,
) -> int:
    """Применяет DELETE-операции к зеркалу: MERGE ... WHEN MATCHED THEN DELETE.

    Создаёт staging с PK-колонками, вставляет ключи, делает MERGE по условию
    совпадения всех PK-колонок.

    Args:
        dst_cur: Курсор Trino.
        catalog: Каталог Iceberg.
        raw_schema: Схема RAW.
        meta: Метаданные зеркала.
        delete_keys: Список словарей с PK-значениями для удаления.
        target_insert_batch_size: Размер батча.
        cast_to_null_counter: Счётчик неудачных приведений.

    Returns:
        Число удалённых строк.
    """
    if not delete_keys:
        return 0
    pk_columns = meta.primary_key
    pk_columns_sql = ", ".join(quote_ident(column) for column in pk_columns)
    type_by_name = {column.name: column.trino_type for column in meta.columns}
    merge_condition = " AND ".join(f"t.{quote_ident(col)} = s.{quote_ident(col)}" for col in pk_columns)
    deleted_total = 0
    for delete_batch in chunked(delete_keys, max(1, MAX_TRINO_ROWS_PER_WRITE)):
        staging_table = make_staging_table_name(meta.target_table, "delete")
        staging_ref = quote_table(catalog, raw_schema, staging_table)
        execute_trino_write_with_retry(
            dst_cur=dst_cur,
            sql=(
            f"""
            CREATE TABLE {staging_ref}
            AS SELECT {pk_columns_sql}
            FROM {meta.target_ref}
            WHERE FALSE
            """
            ),
            op="create_staging_delete",
            target_ref=staging_ref,
        )
        try:
            insert_values_batches(
                dst_cur=dst_cur,
                table_ref=staging_ref,
                rows=delete_batch,
                batch_size=target_insert_batch_size,
                row_renderer=lambda item: (
                    "("
                    + ", ".join(
                        to_trino_literal(
                            item.get(column),
                            type_by_name[column],
                            cast_to_null_counter=cast_to_null_counter,
                        )
                        for column in pk_columns
                    )
                    + ")"
                ),
            )
            execute_trino_write_with_retry(
                dst_cur=dst_cur,
                sql=(
                f"""
                MERGE INTO {meta.target_ref} t
                USING {staging_ref} s
                ON {merge_condition}
                WHEN MATCHED THEN DELETE
                """
                ),
                op="merge_delete",
                target_ref=meta.target_ref,
                rows=len(delete_batch),
            )
            deleted_total += len(delete_batch)
        finally:
            drop_staging_best_effort(dst_cur=dst_cur, staging_ref=staging_ref)
    return deleted_total


def apply_upsert_ops(
    dst_cur,
    catalog: str,
    raw_schema: str,
    meta: IncrementalMirrorMeta,
    upsert_rows: list[dict[str, Any]],
    target_insert_batch_size: int,
    cast_to_null_counter: list[int] | None = None,
) -> int:
    """Применяет UPSERT-операции (INSERT+UPDATE): MERGE с полным набором колонок.

    WHEN MATCHED → UPDATE всех не-PK колонок.
    WHEN NOT MATCHED → INSERT всей строки.

    Args:
        dst_cur: Курсор Trino.
        catalog: Каталог Iceberg.
        raw_schema: Схема RAW.
        meta: Метаданные зеркала.
        upsert_rows: Список словарей со всеми колонками для upsert.
        target_insert_batch_size: Размер батча.
        cast_to_null_counter: Счётчик неудачных приведений.

    Returns:
        Число обработанных строк.
    """
    if not upsert_rows:
        return 0
    type_by_name = {column.name: column.trino_type for column in meta.columns}
    column_names = [column.name for column in meta.columns]
    pk_set = set(meta.primary_key)
    merge_condition = " AND ".join(
        f"t.{quote_ident(col)} = s.{quote_ident(col)}" for col in meta.primary_key
    )
    non_pk_columns = [column for column in column_names if column not in pk_set]
    insert_cols_sql = ", ".join(quote_ident(column) for column in column_names)
    insert_vals_sql = ", ".join(f"s.{quote_ident(column)}" for column in column_names)
    upserted_total = 0
    for upsert_batch in chunked(upsert_rows, max(1, MAX_TRINO_ROWS_PER_WRITE)):
        staging_table = make_staging_table_name(meta.target_table, "upsert")
        staging_ref = quote_table(catalog, raw_schema, staging_table)
        create_staging_table(dst_cur, meta.target_ref, staging_ref)
        try:
            insert_values_batches(
                dst_cur=dst_cur,
                table_ref=staging_ref,
                rows=upsert_batch,
                batch_size=target_insert_batch_size,
                row_renderer=lambda item: (
                    "("
                    + ", ".join(
                        to_trino_literal(
                            item.get(column_name),
                            type_by_name[column_name],
                            cast_to_null_counter=cast_to_null_counter,
                        )
                        for column_name in column_names
                    )
                    + ")"
                ),
            )
            if non_pk_columns:
                update_set_sql = ", ".join(
                    f"{quote_ident(column)} = s.{quote_ident(column)}" for column in non_pk_columns
                )
                execute_trino_write_with_retry(
                    dst_cur=dst_cur,
                    sql=(
                    f"""
                    MERGE INTO {meta.target_ref} t
                    USING {staging_ref} s
                    ON {merge_condition}
                    WHEN MATCHED THEN
                        UPDATE SET {update_set_sql}
                    WHEN NOT MATCHED THEN
                        INSERT ({insert_cols_sql})
                        VALUES ({insert_vals_sql})
                    """
                    ),
                    op="merge_upsert",
                    target_ref=meta.target_ref,
                    rows=len(upsert_batch),
                )
            else:
                execute_trino_write_with_retry(
                    dst_cur=dst_cur,
                    sql=(
                    f"""
                    MERGE INTO {meta.target_ref} t
                    USING {staging_ref} s
                    ON {merge_condition}
                    WHEN NOT MATCHED THEN
                        INSERT ({insert_cols_sql})
                        VALUES ({insert_vals_sql})
                    """
                    ),
                    op="merge_insert_only",
                    target_ref=meta.target_ref,
                    rows=len(upsert_batch),
                )
            upserted_total += len(upsert_batch)
        finally:
            drop_staging_best_effort(dst_cur=dst_cur, staging_ref=staging_ref)
    return upserted_total


def apply_update_ops(
    dst_cur,
    catalog: str,
    raw_schema: str,
    meta: IncrementalMirrorMeta,
    update_rows: list[tuple[dict[str, Any], dict[str, Any]]],
    target_insert_batch_size: int,
    cast_to_null_counter: list[int] | None = None,
    lineage_refresh_source: str | None = None,
) -> int:
    """Применяет UPDATE-операции к зеркалу.

    Два режима (определяется флагом UNIFIED_UPDATE_MERGE):

    1. **Унифицированный**: все UPDATE в одном MERGE с CASE WHEN для каждой колонки
       (``WHEN s.__set_col THEN s.col ELSE t.col``). Плюс: один проход. Минус: широкая
       staging-таблица (PK + все не-PK колонки + флаги __set_*).

    2. **Групповой**: UPDATE группируются по набору изменившихся колонок, для каждой
       группы — отдельный MERGE с узкой staging-таблицей (только затронутые колонки).
       Плюс: меньше lock contention в Iceberg. Минус: больше отдельных MERGE.

    Также добавляет обновление lineage-колонок (raw_load_source, raw_loaded_at),
    если задан lineage_refresh_source.

    Args:
        dst_cur: Курсор Trino.
        catalog: Каталог Iceberg.
        raw_schema: Схема RAW.
        meta: Метаданные зеркала.
        update_rows: Список кортежей (pk_values, diff_values).
        target_insert_batch_size: Размер батча.
        cast_to_null_counter: Счётчик неудачных приведений.
        lineage_refresh_source: Если задан — обновляет lineage-колонки в том же MERGE.

    Returns:
        Число обновлённых строк.
    """
    if not update_rows:
        return 0
    type_by_name = {column.name: column.trino_type for column in meta.columns}
    allowed_columns = set(type_by_name.keys())
    pk_set = set(meta.primary_key)
    non_pk_columns = [column.name for column in meta.columns if column.name not in pk_set]
    if not non_pk_columns:
        return 0

    lineage_skip_cols: frozenset[str] = (
        frozenset({RAW_LOAD_SOURCE_COL, RAW_LOADED_AT_COL})
        if lineage_refresh_source
        else frozenset()
    )
    merge_non_pk_columns = [column for column in non_pk_columns if column not in lineage_skip_cols]

    if UNIFIED_UPDATE_MERGE:
        prepared_rows: list[tuple[dict[str, Any], dict[str, Any], set[str]]] = []
        for pk_values, diff_values in update_rows:
            changed_column_set = {
                column
                for column in diff_values.keys()
                if column in allowed_columns and column not in pk_set
            }
            changed_column_set -= lineage_skip_cols
            if not changed_column_set:
                continue
            prepared_rows.append((pk_values, diff_values, changed_column_set))

        if not prepared_rows:
            return 0

        selected_columns_sql = ", ".join(
            [quote_ident(column) for column in meta.primary_key]
            + [
                (
                    f"CAST(NULL AS {type_by_name[column]}) AS {quote_ident(column)}"
                )
                for column in merge_non_pk_columns
            ]
            + [
                f"CAST(FALSE AS BOOLEAN) AS {quote_ident(f'__set_{column}')}"
                for column in merge_non_pk_columns
            ]
        )
        merge_condition = " AND ".join(
            f"t.{quote_ident(col)} = s.{quote_ident(col)}" for col in meta.primary_key
        )
        update_set_sql = ", ".join(
            (
                f"{quote_ident(column)} = CASE "
                f"WHEN s.{quote_ident(f'__set_{column}')} THEN s.{quote_ident(column)} "
                f"ELSE t.{quote_ident(column)} END"
            )
            for column in merge_non_pk_columns
        )
        if lineage_refresh_source:
            lineage_at = datetime.now(timezone.utc)
            update_set_sql = (
                update_set_sql
                + lineage_literal_update_suffix(lineage_refresh_source, lineage_at)
            )
        updated_total = 0
        for prepared_batch in chunked(prepared_rows, max(1, MAX_TRINO_ROWS_PER_WRITE)):
            staging_table = make_staging_table_name(meta.target_table, "update")
            staging_ref = quote_table(catalog, raw_schema, staging_table)
            execute_trino_write_with_retry(
                dst_cur=dst_cur,
                sql=(
                f"""
                CREATE TABLE {staging_ref}
                AS SELECT {selected_columns_sql}
                FROM {meta.target_ref}
                WHERE FALSE
                """
                ),
                op="create_staging_update",
                target_ref=staging_ref,
            )
            try:
                insert_values_batches(
                    dst_cur=dst_cur,
                    table_ref=staging_ref,
                    rows=prepared_batch,
                    batch_size=target_insert_batch_size,
                    row_renderer=lambda item: (
                        "("
                        + ", ".join(
                            [
                                to_trino_literal(
                                    item[0].get(column),
                                    type_by_name[column],
                                    cast_to_null_counter=cast_to_null_counter,
                                )
                                for column in meta.primary_key
                            ]
                            + [
                                (
                                    to_trino_literal(
                                        item[1].get(column),
                                        type_by_name[column],
                                        cast_to_null_counter=cast_to_null_counter,
                                    )
                                    if column in item[2]
                                    else f"CAST(NULL AS {type_by_name[column]})"
                                )
                                for column in merge_non_pk_columns
                            ]
                            + ["TRUE" if column in item[2] else "FALSE" for column in merge_non_pk_columns]
                        )
                        + ")"
                    ),
                )
                execute_trino_write_with_retry(
                    dst_cur=dst_cur,
                    sql=(
                    f"""
                    MERGE INTO {meta.target_ref} t
                    USING {staging_ref} s
                    ON {merge_condition}
                    WHEN MATCHED THEN
                        UPDATE SET {update_set_sql}
                    """
                    ),
                    op="merge_update",
                    target_ref=meta.target_ref,
                    rows=len(prepared_batch),
                )
                updated_total += len(prepared_batch)
            finally:
                drop_staging_best_effort(dst_cur=dst_cur, staging_ref=staging_ref)
        return updated_total

    grouped_rows: dict[tuple[str, ...], list[tuple[dict[str, Any], dict[str, Any]]]] = {}
    for pk_values, diff_values in update_rows:
        changed_columns = tuple(
            column
            for column in sorted(diff_values.keys())
            if column in allowed_columns
            and column not in meta.primary_key
            and column not in lineage_skip_cols
        )
        if not changed_columns:
            continue
        grouped_rows.setdefault(changed_columns, []).append((pk_values, diff_values))

    updated_count = 0
    for changed_columns, rows in grouped_rows.items():
        all_columns = meta.primary_key + list(changed_columns)
        selected_cols_sql = ", ".join(quote_ident(column) for column in all_columns)
        merge_condition = " AND ".join(
            f"t.{quote_ident(col)} = s.{quote_ident(col)}" for col in meta.primary_key
        )
        update_set_sql = ", ".join(
            f"{quote_ident(column)} = s.{quote_ident(column)}" for column in changed_columns
        )
        if lineage_refresh_source:
            lineage_at = datetime.now(timezone.utc)
            update_set_sql = (
                update_set_sql
                + lineage_literal_update_suffix(lineage_refresh_source, lineage_at)
            )
        for update_batch in chunked(rows, max(1, MAX_TRINO_ROWS_PER_WRITE)):
            staging_table = make_staging_table_name(meta.target_table, "update")
            staging_ref = quote_table(catalog, raw_schema, staging_table)
            execute_trino_write_with_retry(
                dst_cur=dst_cur,
                sql=(
                f"""
                CREATE TABLE {staging_ref}
                AS SELECT {selected_cols_sql}
                FROM {meta.target_ref}
                WHERE FALSE
                """
                ),
                op="create_staging_update",
                target_ref=staging_ref,
            )
            try:
                insert_values_batches(
                    dst_cur=dst_cur,
                    table_ref=staging_ref,
                    rows=update_batch,
                    batch_size=target_insert_batch_size,
                    row_renderer=lambda item: (
                        "("
                        + ", ".join(
                            to_trino_literal(
                                item[0].get(column),
                                type_by_name[column],
                                cast_to_null_counter=cast_to_null_counter,
                            )
                            for column in meta.primary_key
                        )
                        + ", "
                        + ", ".join(
                            to_trino_literal(
                                item[1].get(column),
                                type_by_name[column],
                                cast_to_null_counter=cast_to_null_counter,
                            )
                            for column in changed_columns
                        )
                        + ")"
                    ),
                )
                execute_trino_write_with_retry(
                    dst_cur=dst_cur,
                    sql=(
                    f"""
                    MERGE INTO {meta.target_ref} t
                    USING {staging_ref} s
                    ON {merge_condition}
                    WHEN MATCHED THEN
                        UPDATE SET {update_set_sql}
                    """
                    ),
                    op="merge_update",
                    target_ref=meta.target_ref,
                    rows=len(update_batch),
                )
                updated_count += len(update_batch)
            finally:
                drop_staging_best_effort(dst_cur=dst_cur, staging_ref=staging_ref)
    return updated_count


def find_missing_target_pk_rows(
    dst_cur,
    catalog: str,
    raw_schema: str,
    meta: IncrementalMirrorMeta,
    candidate_pk_rows: list[dict[str, Any]],
    target_insert_batch_size: int,
    cast_to_null_counter: list[int] | None = None,
) -> list[dict[str, Any]]:
    """Находит PK, которые есть в кандидатах, но отсутствуют в целевом зеркале.

    Используется для backfill: если событие INSERT/UPDATE ссылается на ключ,
    которого ещё нет в зеркале, нужно подтянуть полную строку из источника.

    Алгоритм:
    1. Дедуплицирует candidate_pk_rows по PK.
    2. Создаёт staging со списком ключей.
    3. LEFT JOIN с целевой таблицей, WHERE target.PK IS NULL → missing.

    Args:
        dst_cur: Курсор Trino.
        catalog: Каталог Iceberg.
        raw_schema: Схема RAW.
        meta: Метаданные зеркала.
        candidate_pk_rows: Кандидаты PK для проверки.
        target_insert_batch_size: Размер батча.
        cast_to_null_counter: Счётчик неудачных приведений.

    Returns:
        Список словарей {column: value} для отсутствующих PK.
    """
    if not candidate_pk_rows:
        return []
    pk_columns = meta.primary_key
    unique_rows: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()
    for row in candidate_pk_rows:
        key = tuple(row.get(column) for column in pk_columns)
        if key in seen:
            continue
        seen.add(key)
        unique_rows.append({column: row.get(column) for column in pk_columns})

    pk_columns_sql = ", ".join(quote_ident(column) for column in pk_columns)
    staging_table = make_staging_table_name(meta.target_table, "missing_pk")
    staging_ref = quote_table(catalog, raw_schema, staging_table)
    dst_cur.execute(
        f"""
        CREATE TABLE {staging_ref}
        AS SELECT {pk_columns_sql}
        FROM {meta.target_ref}
        WHERE FALSE
        """
    )
    type_by_name = {column.name: column.trino_type for column in meta.columns}
    first_pk = quote_ident(pk_columns[0])
    merge_condition = " AND ".join(f"t.{quote_ident(col)} = s.{quote_ident(col)}" for col in pk_columns)
    select_pk_sql = ", ".join(f"s.{quote_ident(col)}" for col in pk_columns)
    try:
        insert_values_batches(
            dst_cur=dst_cur,
            table_ref=staging_ref,
            rows=unique_rows,
            batch_size=target_insert_batch_size,
            row_renderer=lambda item: (
                "("
                + ", ".join(
                    to_trino_literal(
                        item.get(column),
                        type_by_name[column],
                        cast_to_null_counter=cast_to_null_counter,
                    )
                    for column in pk_columns
                )
                + ")"
            ),
        )
        dst_cur.execute(
            f"""
            SELECT {select_pk_sql}
            FROM {staging_ref} s
            LEFT JOIN {meta.target_ref} t
              ON {merge_condition}
            WHERE t.{first_pk} IS NULL
            """
        )
        missing_rows = dst_cur.fetchall()
    finally:
        dst_cur.execute(f"DROP TABLE IF EXISTS {staging_ref}")
    return [
        {column: value for column, value in zip(pk_columns, row, strict=False)}
        for row in missing_rows
    ]


def fetch_source_rows_by_pk(
    src_cur,
    meta: IncrementalMirrorMeta,
    pk_rows: list[dict[str, Any]],
    source_fetch_batch_size: int,
) -> list[dict[str, Any]]:
    """Подтягивает полные строки из источника по списку первичных ключей (backfill).

    Разбивает запрос на батчи по ``source_fetch_batch_size`` ключей.

    Args:
        src_cur: Курсор PostgreSQL.
        meta: Метаданные зеркала (схема, таблица, колонки).
        pk_rows: Список словарей с PK-значениями.
        source_fetch_batch_size: Размер батча ключей в одном запросе.

    Returns:
        Список словарей с полными строками из источника.
    """
    if not pk_rows:
        return []
    started_at = time_module.monotonic()
    q_schema = quote_ident(meta.source_schema)
    q_table = quote_ident(meta.source_table)
    select_columns_sql = ", ".join(f"t.{quote_ident(column.name)}" for column in pg_columns_only)
    pk_expr_sql = ", ".join(f"t.{quote_ident(column)}" for column in meta.primary_key)
    value_tpl = "(" + ", ".join(["%s"] * len(meta.primary_key)) + ")"
    pg_columns_only = [
        column
        for column in meta.columns
        if column.name not in {RAW_LOAD_SOURCE_COL, RAW_LOADED_AT_COL}
    ]
    fetched_rows: list[dict[str, Any]] = []
    total_batches = math.ceil(len(pk_rows) / source_fetch_batch_size)
    LOGGER.info(
        "[INFO] source backfill fetch start: table=%s keys=%s batch_size=%s batches=%s",
        f"{meta.source_schema}.{meta.source_table}",
        len(pk_rows),
        source_fetch_batch_size,
        total_batches,
    )
    batch_no = 0
    for batch in chunked(pk_rows, source_fetch_batch_size):
        batch_no += 1
        placeholders = ", ".join(value_tpl for _ in batch)
        flat_params: list[Any] = []
        for row in batch:
            flat_params.extend(row.get(column) for column in meta.primary_key)
        src_cur.execute(
            f"""
            SELECT {select_columns_sql}
            FROM {q_schema}.{q_table} t
            WHERE ({pk_expr_sql}) IN ({placeholders})
            """,
            tuple(flat_params),
        )
        for source_row in src_cur.fetchall():
            fetched_rows.append(
                {
                    column.name: value
                    for column, value in zip(pg_columns_only, source_row, strict=False)
                }
            )
        LOGGER.info(
            "[INFO] source backfill fetch batch: table=%s batch=%s/%s requested_keys=%s fetched_rows_total=%s",
            f"{meta.source_schema}.{meta.source_table}",
            batch_no,
            total_batches,
            len(batch),
            len(fetched_rows),
        )
    LOGGER.info(
        "[INFO] source backfill fetch done: table=%s keys=%s fetched_rows=%s elapsed_sec=%.2f",
        f"{meta.source_schema}.{meta.source_table}",
        len(pk_rows),
        len(fetched_rows),
        time_module.monotonic() - started_at,
    )
    return fetched_rows


# ---------- Слияние событий дня в state (bulk и streaming-движки) ----------


def load_incremental_table_bulk(
    src_cur,
    dst_cur,
    catalog: str,
    raw_schema: str,
    table: TableConfig,
    rows: list[tuple[Any, ...]],
    source_fetch_batch_size: int,
    target_insert_batch_size: int,
    lineage_source_id: str,
) -> TableStats:
    """Загружает инкрементальные события в режиме bulk (все строки сразу в памяти).

    Алгоритм:
    1. Сортирует события по (stamp, log_id).
    2. Записывает все события в *__events (дедупликация по log_id).
    3. Создаёт/загружает зеркало (ensure_incremental_raw_table).
    4. Проходит события, строит финальное состояние (final_state) —
       для каждого PK последняя операция (INSERT→upsert, UPDATE→update, DELETE→delete).
       UPDATE с PREFER_FULL_UPDATE_PAYLOAD использует new_values вместо diff.
       Последовательные UPDATE сливаются (merge payloads).
    5. Применяет delete_ops, upsert_ops, update_ops к зеркалу.
    6. Backfill: для PK, которых нет в зеркале, подтягивает полные строки из источника.
    7. Replay: повторяет операции дня для backfill-ключей, чтобы восстановить состояние.

    Args:
        src_cur: Курсор PostgreSQL.
        dst_cur: Курсор Trino.
        catalog: Каталог Iceberg.
        raw_schema: Схема RAW.
        table: Конфигурация таблицы.
        rows: Все строки из global_log за день.
        source_fetch_batch_size: Размер батча для backfill-запросов.
        target_insert_batch_size: Размер батча для записи в Trino.
        lineage_source_id: Идентификатор источника.

    Returns:
        TableStats с статистикой загрузки.
    """
    stats = TableStats(rows_read=len(rows))
    if not rows:
        return stats

    ordered_rows = sorted(rows, key=lambda item: (item[3], int(item[0])))
    stats.rows_events_inserted = write_incremental_events(
        dst_cur=dst_cur,
        catalog=catalog,
        raw_schema=raw_schema,
        table=table,
        rows=ordered_rows,
        target_insert_batch_size=target_insert_batch_size,
        lineage_source_id=lineage_source_id,
    )
    # -----------------------------------------------------------------------
    # Шаг 1: построение финального состояния (final_state) по событиям дня.
    # final_state: key(PK) → (operation, pk_values, payload)
    #   operation: "delete" | "upsert" | "update"
    #   pk_values: {col: val} — значения первичного ключа
    #   payload: полный словарь (для upsert) или diff (для update)
    # Последовательные UPDATE сливаются (merge payloads).
    # INSERT перезаписывает предыдущее состояние для того же PK.
    # События без полного PK → invalid_event_rows.
    # -----------------------------------------------------------------------
    final_state: dict[tuple[Any, ...], tuple[str, dict[str, Any], dict[str, Any]]] = {}
    invalid_event_rows: list[tuple[Any, ...]] = []
    cast_to_null_counter = [0]

    for row in ordered_rows:
        (
            _log_id,
            _source_table,
            action,
            _stamp,
            _source_pkey,
            old_values_raw,
            new_values_raw,
            old_diff_raw,
            new_diff_raw,
        ) = row
        op = str(action).upper()
        old_values = extract_change_dict(old_values_raw)
        new_values = extract_change_dict(new_values_raw)
        old_diff = extract_change_dict(old_diff_raw)
        new_diff = extract_change_dict(new_diff_raw)

        pk_values = extract_pk_values(
            mirror.primary_key,
            candidates=[new_values, old_values, new_diff, old_diff],
        )
        if pk_values is None:
            invalid_event_rows.append(
                (
                    _log_id,
                    table.name,
                    _source_pkey,
                    op,
                    _stamp,
                    "missing_primary_key",
                    old_values,
                    new_values,
                    old_diff,
                    new_diff,
                )
            )
            continue
        key = tuple(pk_values[column] for column in mirror.primary_key)

        if op == "DELETE":
            final_state[key] = ("delete", pk_values, {})
            continue
        if op == "INSERT":
            final_state[key] = ("upsert", pk_values, new_values)
            continue
        if op == "UPDATE":
            # Prefer full row payload for UPDATE events when available.
            # This significantly reduces merge fragmentation by changed_columns
            # and keeps UPDATE batches large in Iceberg.
            if PREFER_FULL_UPDATE_PAYLOAD:
                update_payload = new_values or new_diff
            else:
                update_payload = new_diff or new_values
            previous = final_state.get(key)
            if previous is not None and previous[0] in {"upsert", "update"}:
                merged_payload = dict(previous[2])
                merged_payload.update(update_payload)
                final_state[key] = (previous[0], pk_values, merged_payload)
            else:
                final_state[key] = ("update", pk_values, dict(update_payload))
            continue
        invalid_event_rows.append(
            (
                _log_id,
                table.name,
                _source_pkey,
                op,
                _stamp,
                f"unsupported_action:{op}",
                old_values,
                new_values,
                old_diff,
                new_diff,
            )
        )

    delete_ops: list[dict[str, Any]] = []
    upsert_ops: list[dict[str, Any]] = []
    update_ops: list[tuple[dict[str, Any], dict[str, Any]]] = []
    candidate_backfill_keys: list[dict[str, Any]] = []
    for operation, pk_values, payload in final_state.values():
        if operation == "delete":
            delete_ops.append(pk_values)
        elif operation == "upsert":
            upsert_ops.append(payload)
            candidate_backfill_keys.append(pk_values)
        elif operation == "update":
            update_ops.append((pk_values, payload))
            candidate_backfill_keys.append(pk_values)

    mirror_stamp_payloads = [
        *upsert_ops,
        *[payload for _pk_values, payload in update_ops],
    ]
    stamp_payload_lineage(mirror_stamp_payloads, lineage_source_id)

    deleted_count = apply_delete_ops(
        dst_cur=dst_cur,
        catalog=catalog,
        raw_schema=raw_schema,
        meta=mirror,
        delete_keys=delete_ops,
        target_insert_batch_size=target_insert_batch_size,
        cast_to_null_counter=cast_to_null_counter,
    )
    upserted_count = apply_upsert_ops(
        dst_cur=dst_cur,
        catalog=catalog,
        raw_schema=raw_schema,
        meta=mirror,
        upsert_rows=upsert_ops,
        target_insert_batch_size=target_insert_batch_size,
        cast_to_null_counter=cast_to_null_counter,
    )
    updated_count = apply_update_ops(
        dst_cur=dst_cur,
        catalog=catalog,
        raw_schema=raw_schema,
        meta=mirror,
        update_rows=update_ops,
        target_insert_batch_size=target_insert_batch_size,
        cast_to_null_counter=cast_to_null_counter,
        lineage_refresh_source=lineage_source_id,
    )
    missing_pk_rows = find_missing_target_pk_rows(
        dst_cur=dst_cur,
        catalog=catalog,
        raw_schema=raw_schema,
        meta=mirror,
        candidate_pk_rows=candidate_backfill_keys,
        target_insert_batch_size=target_insert_batch_size,
        cast_to_null_counter=cast_to_null_counter,
    )
    backfilled_count = 0
    replayed_count = 0
    if missing_pk_rows:
        source_rows = fetch_source_rows_by_pk(
            src_cur=src_cur,
            meta=mirror,
            pk_rows=missing_pk_rows,
            source_fetch_batch_size=source_fetch_batch_size,
        )
        if source_rows:
            stamp_payload_lineage(source_rows, lineage_source_id)
            backfilled_count = apply_upsert_ops(
                dst_cur=dst_cur,
                catalog=catalog,
                raw_schema=raw_schema,
                meta=mirror,
                upsert_rows=source_rows,
                target_insert_batch_size=target_insert_batch_size,
                cast_to_null_counter=cast_to_null_counter,
            )
        LOGGER.info(
            "[INFO] incremental backfill: table=%s requested=%s fetched=%s upserted=%s",
            table.name,
            len(missing_pk_rows),
            len(source_rows) if missing_pk_rows else 0,
            backfilled_count,
        )
        backfilled_key_set = {
            tuple(item.get(column) for column in mirror.primary_key)
            for item in missing_pk_rows
        }
        replay_delete_ops: list[dict[str, Any]] = []
        replay_upsert_ops: list[dict[str, Any]] = []
        replay_update_ops: list[tuple[dict[str, Any], dict[str, Any]]] = []
        for key in backfilled_key_set:
            operation_data = final_state.get(key)
            if operation_data is None:
                continue
            operation, pk_values, payload = operation_data
            if operation == "delete":
                replay_delete_ops.append(pk_values)
            elif operation == "upsert":
                replay_upsert_ops.append(payload)
            elif operation == "update":
                replay_update_ops.append((pk_values, payload))
        replay_mirror_payloads = [
            *replay_upsert_ops,
            *[payload for _pk_values, payload in replay_update_ops],
        ]
        stamp_payload_lineage(replay_mirror_payloads, lineage_source_id)
        replay_deleted_count = apply_delete_ops(
            dst_cur=dst_cur,
            catalog=catalog,
            raw_schema=raw_schema,
            meta=mirror,
            delete_keys=replay_delete_ops,
            target_insert_batch_size=target_insert_batch_size,
            cast_to_null_counter=cast_to_null_counter,
        )
        replay_upserted_count = apply_upsert_ops(
            dst_cur=dst_cur,
            catalog=catalog,
            raw_schema=raw_schema,
            meta=mirror,
            upsert_rows=replay_upsert_ops,
            target_insert_batch_size=target_insert_batch_size,
            cast_to_null_counter=cast_to_null_counter,
        )
        replay_updated_count = apply_update_ops(
            dst_cur=dst_cur,
            catalog=catalog,
            raw_schema=raw_schema,
            meta=mirror,
            update_rows=replay_update_ops,
            target_insert_batch_size=target_insert_batch_size,
            cast_to_null_counter=cast_to_null_counter,
            lineage_refresh_source=lineage_source_id,
        )
        replayed_count = replay_deleted_count + replay_upserted_count + replay_updated_count
        LOGGER.info(
            "[INFO] incremental replay after backfill: table=%s keys=%s replayed=%s",
            table.name,
            len(backfilled_key_set),
            replayed_count,
        )
    invalid_rows = write_invalid_incremental_events(
        dst_cur=dst_cur,
        catalog=catalog,
        raw_schema=raw_schema,
        table=table,
        rows=invalid_event_rows,
        target_insert_batch_size=target_insert_batch_size,
        lineage_source_id=lineage_source_id,
    )

    stats.rows_state_upserted = (
        upserted_count + updated_count + deleted_count + backfilled_count + replayed_count
    )
    stats.rows_backfilled = backfilled_count
    stats.rows_replayed = replayed_count
    stats.rows_invalid = invalid_rows
    stats.rows_cast_to_null = cast_to_null_counter[0]
    return stats


def load_incremental_table_streaming(
    src_cur,
    dst_cur,
    catalog: str,
    raw_schema: str,
    table: TableConfig,
    day_start: datetime,
    day_end: datetime,
    source_log_schema: str,
    source_log_table: str,
    source_fetch_batch_size: int,
    target_insert_batch_size: int,
    lineage_source_id: str,
) -> TableStats:
    """Загружает инкрементальные события в потоковом режиме (streaming).

    В отличие от bulk-режима:
    - Не загружает все строки в память — читает батчами через iter_incremental_rows.
    - События пишутся в *__events по мере чтения (не ждём полной загрузки).
    - Состояние (final_state) накапливается инкрементально в словаре.
    - В конце — те же шаги: merge в зеркало, backfill, replay.

    Предпочтительный режим для production: меньше памяти, прогресс виден раньше.

    Args:
        src_cur: Курсор PostgreSQL.
        dst_cur: Курсор Trino.
        catalog: Каталог Iceberg.
        raw_schema: Схема RAW.
        table: Конфигурация таблицы.
        day_start: Начало окна (UTC).
        day_end: Конец окна (UTC, исключительно).
        source_log_schema: Схема журнала.
        source_log_table: Таблица журнала.
        source_fetch_batch_size: Размер батча чтения из источника.
        target_insert_batch_size: Размер батча записи в Trino.
        lineage_source_id: Идентификатор источника.

    Returns:
        TableStats с статистикой загрузки.
    """
    stream_started_at = time_module.monotonic()
    stats = TableStats()
    table_aliases = sorted({table.name, split_table_name(table.name)[1]})
    total_rows_to_read = count_incremental_rows(
        src_cur=src_cur,
        day_start=day_start,
        day_end=day_end,
        tables=table_aliases,
        source_log_schema=source_log_schema,
        source_log_table=source_log_table,
    )
    LOGGER.info(
        "[INFO] incremental stream count: table=%s rows=%s",
        table.name,
        total_rows_to_read,
    )
    if total_rows_to_read == 0:
        return stats

    final_state: dict[tuple[Any, ...], tuple[str, dict[str, Any], dict[str, Any]]] = {}
    invalid_event_rows: list[tuple[Any, ...]] = []
    cast_to_null_counter = [0]
    mirror: IncrementalMirrorMeta | None = None

    for rows_batch in iter_incremental_rows(
        src_cur=src_cur,
        day_start=day_start,
        day_end=day_end,
        tables=table_aliases,
        source_fetch_batch_size=source_fetch_batch_size,
        source_log_schema=source_log_schema,
        source_log_table=source_log_table,
    ):
        if mirror is None:
            mirror = ensure_incremental_raw_table(
                src_cur=src_cur,
                dst_cur=dst_cur,
                catalog=catalog,
                raw_schema=raw_schema,
                table=table,
            )
            if mirror.created:
                LOGGER.info("[INFO] raw mirror created: table=%s", table.name)
        stats.rows_read += len(rows_batch)
        stats.rows_events_inserted += write_incremental_events(
            dst_cur=dst_cur,
            catalog=catalog,
            raw_schema=raw_schema,
            table=table,
            rows=rows_batch,
            target_insert_batch_size=target_insert_batch_size,
            lineage_source_id=lineage_source_id,
        )
        LOGGER.info(
            "[INFO] incremental stream progress: table=%s read=%s/%s",
            table.name,
            stats.rows_read,
            total_rows_to_read,
        )
        for row in rows_batch:
            (
                _log_id,
                _source_table,
                action,
                _stamp,
                _source_pkey,
                old_values_raw,
                new_values_raw,
                old_diff_raw,
                new_diff_raw,
            ) = row
            op = str(action).upper()
            old_values = extract_change_dict(old_values_raw)
            new_values = extract_change_dict(new_values_raw)
            old_diff = extract_change_dict(old_diff_raw)
            new_diff = extract_change_dict(new_diff_raw)
            if mirror is None:
                continue
            pk_values = extract_pk_values(
                mirror.primary_key,
                candidates=[new_values, old_values, new_diff, old_diff],
            )
            if pk_values is None:
                invalid_event_rows.append(
                    (
                        _log_id,
                        table.name,
                        _source_pkey,
                        op,
                        _stamp,
                        "missing_primary_key",
                        old_values,
                        new_values,
                        old_diff,
                        new_diff,
                    )
                )
                continue
            key = tuple(pk_values[column] for column in mirror.primary_key)
            if op == "DELETE":
                final_state[key] = ("delete", pk_values, {})
                continue
            if op == "INSERT":
                final_state[key] = ("upsert", pk_values, new_values)
                continue
            if op == "UPDATE":
                if PREFER_FULL_UPDATE_PAYLOAD:
                    update_payload = new_values or new_diff
                else:
                    update_payload = new_diff or new_values
                previous = final_state.get(key)
                if previous is not None and previous[0] in {"upsert", "update"}:
                    merged_payload = dict(previous[2])
                    merged_payload.update(update_payload)
                    final_state[key] = (previous[0], pk_values, merged_payload)
                else:
                    final_state[key] = ("update", pk_values, dict(update_payload))
                continue
            invalid_event_rows.append(
                (
                    _log_id,
                    table.name,
                    _source_pkey,
                    op,
                    _stamp,
                    f"unsupported_action:{op}",
                    old_values,
                    new_values,
                    old_diff,
                    new_diff,
                )
            )

    if stats.rows_read == 0:
        LOGGER.info(
            "[INFO] source incremental processing done: table=%s rows_read=0 elapsed_sec=%.2f",
            table.name,
            time_module.monotonic() - stream_started_at,
        )
        return stats
    if mirror is None:
        raise RuntimeError(f"Incremental mirror is not initialized for table {table.name}")

    delete_ops: list[dict[str, Any]] = []
    upsert_ops: list[dict[str, Any]] = []
    update_ops: list[tuple[dict[str, Any], dict[str, Any]]] = []
    candidate_backfill_keys: list[dict[str, Any]] = []
    for operation, pk_values, payload in final_state.values():
        if operation == "delete":
            delete_ops.append(pk_values)
        elif operation == "upsert":
            upsert_ops.append(payload)
            candidate_backfill_keys.append(pk_values)
        elif operation == "update":
            update_ops.append((pk_values, payload))
            candidate_backfill_keys.append(pk_values)

    mirror_stamp_payloads = [
        *upsert_ops,
        *[payload for _pk_values, payload in update_ops],
    ]
    stamp_payload_lineage(mirror_stamp_payloads, lineage_source_id)

    deleted_count = apply_delete_ops(
        dst_cur=dst_cur,
        catalog=catalog,
        raw_schema=raw_schema,
        meta=mirror,
        delete_keys=delete_ops,
        target_insert_batch_size=target_insert_batch_size,
        cast_to_null_counter=cast_to_null_counter,
    )
    upserted_count = apply_upsert_ops(
        dst_cur=dst_cur,
        catalog=catalog,
        raw_schema=raw_schema,
        meta=mirror,
        upsert_rows=upsert_ops,
        target_insert_batch_size=target_insert_batch_size,
        cast_to_null_counter=cast_to_null_counter,
    )
    updated_count = apply_update_ops(
        dst_cur=dst_cur,
        catalog=catalog,
        raw_schema=raw_schema,
        meta=mirror,
        update_rows=update_ops,
        target_insert_batch_size=target_insert_batch_size,
        cast_to_null_counter=cast_to_null_counter,
        lineage_refresh_source=lineage_source_id,
    )
    missing_pk_rows = find_missing_target_pk_rows(
        dst_cur=dst_cur,
        catalog=catalog,
        raw_schema=raw_schema,
        meta=mirror,
        candidate_pk_rows=candidate_backfill_keys,
        target_insert_batch_size=target_insert_batch_size,
        cast_to_null_counter=cast_to_null_counter,
    )
    backfilled_count = 0
    replayed_count = 0
    if missing_pk_rows:
        source_rows = fetch_source_rows_by_pk(
            src_cur=src_cur,
            meta=mirror,
            pk_rows=missing_pk_rows,
            source_fetch_batch_size=source_fetch_batch_size,
        )
        if source_rows:
            stamp_payload_lineage(source_rows, lineage_source_id)
            backfilled_count = apply_upsert_ops(
                dst_cur=dst_cur,
                catalog=catalog,
                raw_schema=raw_schema,
                meta=mirror,
                upsert_rows=source_rows,
                target_insert_batch_size=target_insert_batch_size,
                cast_to_null_counter=cast_to_null_counter,
            )
        LOGGER.info(
            "[INFO] incremental backfill: table=%s requested=%s fetched=%s upserted=%s",
            table.name,
            len(missing_pk_rows),
            len(source_rows) if missing_pk_rows else 0,
            backfilled_count,
        )
        backfilled_key_set = {
            tuple(item.get(column) for column in mirror.primary_key)
            for item in missing_pk_rows
        }
        replay_delete_ops: list[dict[str, Any]] = []
        replay_upsert_ops: list[dict[str, Any]] = []
        replay_update_ops: list[tuple[dict[str, Any], dict[str, Any]]] = []
        for key in backfilled_key_set:
            operation_data = final_state.get(key)
            if operation_data is None:
                continue
            operation, pk_values, payload = operation_data
            if operation == "delete":
                replay_delete_ops.append(pk_values)
            elif operation == "upsert":
                replay_upsert_ops.append(payload)
            elif operation == "update":
                replay_update_ops.append((pk_values, payload))
        replay_mirror_payloads = [
            *replay_upsert_ops,
            *[payload for _pk_values, payload in replay_update_ops],
        ]
        stamp_payload_lineage(replay_mirror_payloads, lineage_source_id)
        replay_deleted_count = apply_delete_ops(
            dst_cur=dst_cur,
            catalog=catalog,
            raw_schema=raw_schema,
            meta=mirror,
            delete_keys=replay_delete_ops,
            target_insert_batch_size=target_insert_batch_size,
            cast_to_null_counter=cast_to_null_counter,
        )
        replay_upserted_count = apply_upsert_ops(
            dst_cur=dst_cur,
            catalog=catalog,
            raw_schema=raw_schema,
            meta=mirror,
            upsert_rows=replay_upsert_ops,
            target_insert_batch_size=target_insert_batch_size,
            cast_to_null_counter=cast_to_null_counter,
        )
        replay_updated_count = apply_update_ops(
            dst_cur=dst_cur,
            catalog=catalog,
            raw_schema=raw_schema,
            meta=mirror,
            update_rows=replay_update_ops,
            target_insert_batch_size=target_insert_batch_size,
            cast_to_null_counter=cast_to_null_counter,
            lineage_refresh_source=lineage_source_id,
        )
        replayed_count = replay_deleted_count + replay_upserted_count + replay_updated_count
        LOGGER.info(
            "[INFO] incremental replay after backfill: table=%s keys=%s replayed=%s",
            table.name,
            len(backfilled_key_set),
            replayed_count,
        )
    invalid_rows = write_invalid_incremental_events(
        dst_cur=dst_cur,
        catalog=catalog,
        raw_schema=raw_schema,
        table=table,
        rows=invalid_event_rows,
        target_insert_batch_size=target_insert_batch_size,
        lineage_source_id=lineage_source_id,
    )

    stats.rows_state_upserted = (
        upserted_count + updated_count + deleted_count + backfilled_count + replayed_count
    )
    stats.rows_backfilled = backfilled_count
    stats.rows_replayed = replayed_count
    stats.rows_invalid = invalid_rows
    stats.rows_cast_to_null = cast_to_null_counter[0]
    LOGGER.info(
        "[INFO] source incremental processing done: table=%s rows_read=%s elapsed_sec=%.2f",
        table.name,
        stats.rows_read,
        time_module.monotonic() - stream_started_at,
    )
    return stats


# ---------- Полный снимок таблицы (режим full) ----------


def load_full_snapshot(
    src_cur,
    dst_cur,
    catalog: str,
    raw_schema: str,
    table: TableConfig,
    snapshot_day: date,
    snapshot_table: str,
    source_fetch_batch_size: int,
    target_insert_batch_size: int,
    lineage_source_id: str,
) -> int:
    """Загружает полный снимок (full snapshot) таблицы в *__snapshot.

    Алгоритм:
    1. Читает все строки из источника: source_pkey = concat_ws('-', pk_cols),
       payload = row_to_json(t).
    2. Удаляет существующие данные за snapshot_day (DELETE WHERE snapshot_day = ...).
    3. Вставляет строки батчами через insert_values_batches.

    Не использует MERGE — каждый день пишет новую партицию snapshot_day.

    Args:
        src_cur: Курсор PostgreSQL.
        dst_cur: Курсор Trino.
        catalog: Каталог Iceberg.
        raw_schema: Схема RAW.
        table: Конфигурация таблицы (должен иметь primary_key).
        snapshot_day: Дата снимка.
        snapshot_table: Имя целевой таблицы snapshot.
        source_fetch_batch_size: Размер батча чтения.
        target_insert_batch_size: Размер батча записи.
        lineage_source_id: Идентификатор источника.

    Returns:
        Число загруженных строк.

    Raises:
        ValueError: Если у таблицы не задан primary_key.
    """
    source_snapshot_started_at = time_module.monotonic()
    source_schema, source_table = split_table_name(table.name)
    q_schema = quote_ident(source_schema)
    q_table = quote_ident(source_table)
    snapshot_ref = quote_table(catalog, raw_schema, snapshot_table)

    if not table.primary_key:
        raise ValueError(f"Table {table.name} in full mode requires primary_key list")

    pk_expr = ", ".join(
        [f"COALESCE(t.{quote_ident(column)}::text, '')" for column in table.primary_key]
    )

    LOGGER.info(
        "[INFO] source snapshot fetch start: table=%s batch_size=%s",
        table.name,
        source_fetch_batch_size,
    )
    src_cur.execute(
        f"""
        SELECT
            concat_ws('-', {pk_expr}) AS source_pkey,
            row_to_json(t) AS payload
        FROM {q_schema}.{q_table} t
        """
    )

    execute_trino_write_with_retry(
        dst_cur=dst_cur,
        sql=f"DELETE FROM {snapshot_ref} WHERE snapshot_day = {sql_date(snapshot_day)}",
        op="delete_snapshot_day",
        target_ref=snapshot_ref,
    )

    loaded_rows = 0
    source_batches = 0
    extracted_at = datetime.now(timezone.utc)
    while True:
        rows = src_cur.fetchmany(source_fetch_batch_size)
        if not rows:
            break
        source_batches += 1
        loaded_rows += len(rows)
        LOGGER.info(
            "[INFO] source snapshot fetch batch: table=%s batch=%s rows=%s rows_total=%s",
            table.name,
            source_batches,
            len(rows),
            loaded_rows,
        )
        prepared_rows = [
            (
                snapshot_day,
                source_pkey_value,
                payload_value,
                extracted_at,
                lineage_source_id,
                extracted_at,
            )
            for source_pkey_value, payload_value in rows
        ]
        insert_values_batches(
            dst_cur=dst_cur,
            table_ref=snapshot_ref,
            rows=prepared_rows,
            batch_size=min(target_insert_batch_size, max(1, MAX_TRINO_ROWS_PER_WRITE)),
            row_renderer=lambda item: (
                f"({sql_date(item[0])}, {sql_string(item[1])}, "
                f"{sql_json_text(item[2])}, {sql_timestamptz(item[3])}, "
                f"{sql_string(item[4])}, {sql_timestamptz(item[5])})"
            ),
        )
    LOGGER.info(
        "[INFO] source snapshot fetch done: table=%s batches=%s rows_total=%s elapsed_sec=%.2f",
        table.name,
        source_batches,
        loaded_rows,
        time_module.monotonic() - source_snapshot_started_at,
    )
    return loaded_rows


# ---------- Суточное окно (--day + --tz) и MERGE в raw_load_manifest ----------


def build_day_window(day_string: str, tz_name: str) -> tuple[date, datetime, datetime]:
    """Вычисляет UTC-границы календарного дня с учётом часового пояса.

    Например, для ``--day 2024-06-02 --tz Europe/Moscow``:
    локальные 2024-06-02 00:00:00 MSK → 2024-06-01 21:00:00 UTC,
    локальные 2024-06-03 00:00:00 MSK → 2024-06-02 21:00:00 UTC.

    Args:
        day_string: Дата в формате YYYY-MM-DD.
        tz_name: Имя часового пояса (например ``Europe/Moscow``).

    Returns:
        Кортеж (day_value: date, day_start_utc: datetime, day_end_utc: datetime).
    """
    day_value = date.fromisoformat(day_string)
    tz = ZoneInfo(tz_name)
    local_start = datetime.combine(day_value, time.min, tzinfo=tz)
    local_end = local_start + timedelta(days=1)
    return day_value, local_start.astimezone(timezone.utc), local_end.astimezone(timezone.utc)


def write_manifest(
    dst_cur,
    catalog: str,
    raw_schema: str,
    day_value: date,
    tz_name: str,
    day_start_utc: datetime,
    day_end_utc: datetime,
    table_configs: list[TableConfig],
    stats: dict[str, TableStats],
    started_at: datetime,
    finished_at: datetime,
) -> None:
    """Записывает статистику загрузки в ``raw_load_manifest`` через MERGE.

    Для каждой таблицы формирует MERGE с деталями (rows_read, events_inserted,
    state_upserted, backfilled, replayed, invalid, cast_to_null).
    Использует execute_trino_write_with_iceberg_retry для обработки конфликтов
    коммита (манифест — общая таблица для многих параллельных задач).

    Args:
        dst_cur: Курсор Trino.
        catalog: Каталог Iceberg.
        raw_schema: Схема RAW.
        day_value: Дата загрузки.
        tz_name: Часовой пояс.
        day_start_utc: Начало окна UTC.
        day_end_utc: Конец окна UTC.
        table_configs: Список конфигураций таблиц.
        stats: Словарь table_name → TableStats.
        started_at: Время начала загрузки.
        finished_at: Время окончания загрузки.
    """
    manifest_ref = quote_table(catalog, raw_schema, "raw_load_manifest")
    for table in table_configs:
        table_stats = stats[table.name]
        details = {
            "table": table.name,
            "mode": table.mode,
            "target_catalog": catalog,
            "target_schema": raw_schema,
            "rows_backfilled": table_stats.rows_backfilled,
            "rows_replayed": table_stats.rows_replayed,
            "rows_cast_to_null": table_stats.rows_cast_to_null,
        }
        merge_sql = f"""
            MERGE INTO {manifest_ref} t
            USING (
                SELECT
                    {sql_date(day_value)} AS load_day,
                    {sql_string(table.name)} AS source_table,
                    {sql_string(table.mode)} AS mode,
                    {sql_string(tz_name)} AS tz_name,
                    {sql_timestamptz(day_start_utc)} AS window_start_utc,
                    {sql_timestamptz(day_end_utc)} AS window_end_utc,
                    {sql_bigint(table_stats.rows_read)} AS rows_read,
                    {sql_bigint(table_stats.rows_events_inserted)} AS rows_events_inserted,
                    {sql_bigint(table_stats.rows_state_upserted)} AS rows_state_upserted,
                    {sql_bigint(table_stats.rows_invalid)} AS rows_invalid,
                    {sql_bigint(table_stats.rows_snapshot_loaded)} AS rows_snapshot_loaded,
                    {sql_string("success")} AS status,
                    {sql_json_text(details)} AS details,
                    {sql_timestamptz(started_at)} AS started_at,
                    {sql_timestamptz(finished_at)} AS finished_at
            ) s
            ON t.load_day = s.load_day
               AND t.source_table = s.source_table
               AND t.mode = s.mode
            WHEN MATCHED THEN
                UPDATE SET
                    tz_name = s.tz_name,
                    window_start_utc = s.window_start_utc,
                    window_end_utc = s.window_end_utc,
                    rows_read = s.rows_read,
                    rows_events_inserted = s.rows_events_inserted,
                    rows_state_upserted = s.rows_state_upserted,
                    rows_invalid = s.rows_invalid,
                    rows_snapshot_loaded = s.rows_snapshot_loaded,
                    status = s.status,
                    details = s.details,
                    started_at = s.started_at,
                    finished_at = s.finished_at
            WHEN NOT MATCHED THEN
                INSERT (
                    load_day, source_table, mode, tz_name,
                    window_start_utc, window_end_utc,
                    rows_read, rows_events_inserted, rows_state_upserted, rows_invalid, rows_snapshot_loaded,
                    status, details, started_at, finished_at
                )
                VALUES (
                    s.load_day, s.source_table, s.mode, s.tz_name,
                    s.window_start_utc, s.window_end_utc,
                    s.rows_read, s.rows_events_inserted, s.rows_state_upserted, s.rows_invalid, s.rows_snapshot_loaded,
                    s.status, s.details, s.started_at, s.finished_at
                )
            """
        execute_trino_write_with_iceberg_retry(
            dst_cur, merge_sql, target_ref=manifest_ref, op="merge_raw_manifest"
        )


def main() -> None:
    """Подключиться к источнику и (если не dry-run) к Trino, прогнать таблицы за ``--day`` (все или ``--table``)."""
    args = parse_args()
    setup_logging(args.log_level)
    config = load_config(args.config)
    day_value, day_start, day_end = build_day_window(args.day, args.tz)
    source_cfg = config["source"]
    raw_lineage_id = raw_load_source_id_from_config(source_cfg)
    source_default_schema = str(source_cfg.get("default_schema", "public"))
    source_log_schema = str(source_cfg.get("log_schema", source_default_schema))
    source_log_table = str(source_cfg.get("log_table", "global_log"))
    table_configs_all = parse_table_configs(config["tables"], default_source_schema=source_default_schema)
    table_configs = filter_table_configs_by_arg(
        table_configs_all,
        args.table,
        source_default_schema,
    )
    load_cfg = config.get("load", {})
    fallback_batch_size = int(load_cfg.get("batch_size", 1000))
    source_fetch_batch_size = int(load_cfg.get("source_fetch_batch_size", fallback_batch_size))
    target_insert_batch_size = int(load_cfg.get("target_insert_batch_size", fallback_batch_size))
    if source_fetch_batch_size <= 0:
        raise ValueError("load.source_fetch_batch_size must be greater than 0")
    if target_insert_batch_size <= 0:
        raise ValueError("load.target_insert_batch_size must be greater than 0")
    if MAX_TRINO_ROWS_PER_WRITE <= 0:
        raise ValueError("RAW_LOADER_MAX_TRINO_ROWS_PER_WRITE must be greater than 0")
    target_cfg = config.get("target", {})
    raw_schema = str(target_cfg.get("default_schema", "raw"))
    started_at = datetime.now(timezone.utc)

    src_dsn = build_pg_dsn(source_cfg)
    trino_target: TrinoTargetConfig | None = None
    if not args.dry_run:
        trino_target = build_trino_target_config(target_cfg, fallback_schema=raw_schema)
        raw_schema = trino_target.schema

    LOGGER.info(
        f"[INFO] day={day_value.isoformat()} tz={args.tz} "
        f"window_utc=[{day_start.isoformat()}..{day_end.isoformat()}) "
        f"source_fetch_batch_size={source_fetch_batch_size} "
        f"target_insert_batch_size={target_insert_batch_size} "
        f"source_log={source_log_schema}.{source_log_table} "
        f"table_filter={'ALL' if not args.table else args.table} "
        f"dry_run={args.dry_run}"
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
                incremental_filter_tables = sorted(
                    {
                        alias
                        for table in incremental_tables
                        for alias in (
                            table.name,
                            split_table_name(table.name)[1],
                        )
                    }
                )
                if incremental_filter_tables:
                    count_sql = f"""
                        SELECT count(*)
                        FROM {quote_ident(source_log_schema)}.{quote_ident(source_log_table)}
                        WHERE stamp >= %s
                          AND stamp < %s
                          AND "table" = ANY(%s::text[])
                    """
                    src_cur.execute(count_sql, (day_start, day_end, incremental_filter_tables))
                    inc_count = src_cur.fetchone()[0]
                else:
                    inc_count = 0
                LOGGER.info(f"[DRY-RUN] incremental global_log rows={inc_count}")

                for table in table_configs:
                    if table.mode == "full":
                        source_schema, source_table = split_table_name(table.name)
                        src_cur.execute(
                            f"SELECT count(*) FROM {quote_ident(source_schema)}.{quote_ident(source_table)}"
                        )
                        full_count = src_cur.fetchone()[0]
                        LOGGER.info(f"[DRY-RUN] full table={table.name} rows={full_count}")
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
                    incremental_names = {t.name for t in incremental_list}

                    for incremental_table in incremental_list:
                        LOGGER.info("[INFO] incremental bulk start: table=%s", incremental_table.name)
                        table_stats = load_incremental_table_streaming(
                            src_cur=src_cur,
                            dst_cur=dst_cur,
                            catalog=trino_target.catalog,
                            raw_schema=raw_schema,
                            table=incremental_table,
                            day_start=day_start,
                            day_end=day_end,
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
                            snapshot_day=day_value,
                            snapshot_table=snapshot_table,
                            source_fetch_batch_size=source_fetch_batch_size,
                            target_insert_batch_size=target_insert_batch_size,
                            lineage_source_id=raw_lineage_id,
                        )
                        stats[table.name].rows_read = count
                        stats[table.name].rows_snapshot_loaded = count
                        LOGGER.info(
                            "[INFO] full snapshot loaded: table=%s rows=%s",
                            table.name,
                            count,
                        )

                    finished_at = datetime.now(timezone.utc)
                    write_manifest(
                        dst_cur=dst_cur,
                        catalog=trino_target.catalog,
                        raw_schema=raw_schema,
                        day_value=day_value,
                        tz_name=args.tz,
                        day_start_utc=day_start,
                        day_end_utc=day_end,
                        table_configs=table_configs,
                        stats=stats,
                        started_at=started_at,
                        finished_at=finished_at,
                    )
                    commit_if_supported(dst_conn, context=f"day={day_value.isoformat()}")

                    incremental_rows_processed = sum(
                        item.rows_read for name, item in stats.items() if name in incremental_names
                    )
                    full_rows_loaded = sum(item.rows_snapshot_loaded for item in stats.values())
                    backfilled_rows = sum(item.rows_backfilled for item in stats.values())
                    replayed_rows = sum(item.rows_replayed for item in stats.values())
                    invalid_rows = sum(item.rows_invalid for item in stats.values())
                    cast_to_null_rows = sum(item.rows_cast_to_null for item in stats.values())

                    LOGGER.info(f"[INFO] incremental_rows_processed={incremental_rows_processed}")
                    LOGGER.info(f"[INFO] full_rows_loaded={full_rows_loaded}")
                    LOGGER.info(f"[INFO] backfilled_rows={backfilled_rows}")
                    LOGGER.info(f"[INFO] replayed_rows={replayed_rows}")
                    LOGGER.info(f"[INFO] invalid_rows={invalid_rows}")
                    LOGGER.info(f"[INFO] cast_to_null_rows={cast_to_null_rows}")
                    LOGGER.info(f"[INFO] target_catalog={trino_target.catalog}")
                    LOGGER.info(f"[INFO] target_schema={raw_schema}")


if __name__ == "__main__":
    main()
