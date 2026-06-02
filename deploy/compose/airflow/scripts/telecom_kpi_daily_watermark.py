"""Назначение календарного «дня данных» для DAG telecom_kpi_daily и курсор в raw_load_service_state.

При автоматическом режиме: ``last_fully_loaded_day + 1`` по строке (daily_loader_name, tz_name).
Первая инициализация без строки — от ``bootstrap_last_loaded_day + 1`` из конфига.

Пакетная загрузка ``load_raw_period_batch.py`` использует отдельный ``loader_name`` и не затрагивается.
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any

from load_raw_day import (
    build_trino_target_config,
    commit_if_supported,
    connect_trino,
    load_config,
    quote_schema,
)
from load_raw_period_batch import (
    bootstrap_last_loaded_day,
    ensure_service_table,
    read_last_loaded_day,
    service_table_name,
    write_last_loaded_day,
)

LOGGER = logging.getLogger(__name__)


def daily_loader_name(load_cfg: dict[str, Any]) -> str:
    """Имя загрузчика для водяного знака ежедневного KPI DAG.

    Берётся из ``load.batch_service.daily_loader_name`` конфигурации.
    По умолчанию ``"telecom_kpi_daily"``.

    Args:
        load_cfg: Секция ``load`` из raw_load_config.json.

    Returns:
        Строка — ключ в ``raw_load_service_state``.
    """
    batch = load_cfg.get("batch_service", {})
    raw = batch.get("daily_loader_name")
    if raw is None:
        return "telecom_kpi_daily"
    s = str(raw).strip()
    return s if s else "telecom_kpi_daily"


def resolve_data_day(*, config_path: str, tz_name: str, process_date_override: str | None) -> str:
    """Определяет календарный день данных для KPI DAG.

    Алгоритм (приоритет):
    1. ``process_date_override`` непустой → возвращает его (проверка через ``date.fromisoformat``).
    2. Чтение ``last_fully_loaded_day`` из ``raw_load_service_state`` → ``last + 1 день``.
    3. Если записи нет → ``bootstrap_last_loaded_day + 1`` из конфига.
    4. Если и bootstrap нет → ``ValueError``.

    Args:
        config_path: Путь к raw_load_config.json.
        tz_name: Имя таймзоны (например ``"Europe/Moscow"``).
        process_date_override: Явная дата из конфигурации Airflow (conf/Params).

    Returns:
        Строка ``YYYY-MM-DD`` — календарный день для задач пайплайна.

    Raises:
        ValueError: Если не задан ни курсор, ни bootstrap.
    """
    if process_date_override is not None:
        trimmed = process_date_override.strip()
        if trimmed.lower() in {"", "none", "null"}:
            trimmed = ""
        if trimmed:
            _ = date.fromisoformat(trimmed)
            LOGGER.info("[INFO] kpi daily data day from override: %s", trimmed)
            return trimmed

    cfg = load_config(config_path)
    load_cfg = cfg.get("load", {})
    target_cfg = cfg.get("target", {})
    raw_schema_fallback = str(target_cfg.get("default_schema", "raw"))
    tt = build_trino_target_config(target_cfg, fallback_schema=raw_schema_fallback)
    raw_schema = tt.schema
    state_table = service_table_name(load_cfg)
    loader_key = daily_loader_name(load_cfg)

    with connect_trino(tt) as conn:
        with conn.cursor() as cur:
            cur.execute(tt.verify_connection_sql)
            _ = cur.fetchone()
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {quote_schema(tt.catalog, raw_schema)}")
            ref = ensure_service_table(
                dst_cur=cur,
                catalog=tt.catalog,
                raw_schema=raw_schema,
                state_table=state_table,
            )
            commit_if_supported(conn, context="kpi_daily_ensure_service_table")
            last = read_last_loaded_day(
                dst_cur=cur,
                table_ref=ref,
                configured_loader_name=loader_key,
                tz_name=tz_name,
            )
            if last is None:
                boot = bootstrap_last_loaded_day(load_cfg)
                if boot is None:
                    raise ValueError(
                        "Не задан последний успешный день в Iceberg "
                        "(raw_load_service_state) и отсутствует "
                        "load.batch_service.bootstrap_last_loaded_day в конфиге; "
                        "укажите param/conf process_date или bootstrap."
                    )
                resolved = boot + timedelta(days=1)
                LOGGER.info(
                    "[INFO] kpi daily data day from bootstrap: bootstrap=%s resolved=%s",
                    boot.isoformat(),
                    resolved.isoformat(),
                )
            else:
                resolved = last + timedelta(days=1)
                LOGGER.info(
                    "[INFO] kpi daily data day from service table: last=%s resolved=%s",
                    last.isoformat(),
                    resolved.isoformat(),
                )

    return resolved.isoformat()


def advance_daily_watermark(
    *,
    config_path: str,
    tz_name: str,
    loaded_day: date,
) -> None:
    """Продвигает водяной знак в ``raw_load_service_state`` после успешного прогона KPI DAG.

    Записывает ``loaded_day`` как ``last_fully_loaded_day`` для связки
    (``daily_loader_name``, ``tz_name``). Вызывается только при успехе всего DAG
    (``TriggerRule.ALL_SUCCESS``).

    Args:
        config_path: Путь к raw_load_config.json.
        tz_name: Имя таймзоны.
        loaded_day: Успешно загруженный календарный день.
    """
    cfg = load_config(config_path)
    load_cfg = cfg.get("load", {})
    target_cfg = cfg.get("target", {})
    raw_schema_fallback = str(target_cfg.get("default_schema", "raw"))
    tt = build_trino_target_config(target_cfg, fallback_schema=raw_schema_fallback)
    raw_schema = tt.schema
    state_table = service_table_name(load_cfg)
    loader_key = daily_loader_name(load_cfg)

    with connect_trino(tt) as conn:
        with conn.cursor() as cur:
            cur.execute(tt.verify_connection_sql)
            _ = cur.fetchone()
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {quote_schema(tt.catalog, raw_schema)}")
            ref = ensure_service_table(
                dst_cur=cur,
                catalog=tt.catalog,
                raw_schema=raw_schema,
                state_table=state_table,
            )
            write_last_loaded_day(
                dst_cur=cur,
                table_ref=ref,
                configured_loader_name=loader_key,
                tz_name=tz_name,
                day_value=loaded_day,
            )
            commit_if_supported(conn, context="kpi_daily_advance_watermark")
    LOGGER.info(
        "[INFO] kpi daily watermark advanced: loader=%s tz=%s day=%s",
        loader_key,
        tz_name,
        loaded_day.isoformat(),
    )
