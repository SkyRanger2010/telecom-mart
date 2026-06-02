"""
Общие вещи для скриптов слоя DQ (data quality).

Сводки за календарный день кладём в Iceberg, схема из config → pipeline.validate_schema
(обычно `validate`), таблица `dq_daily_summary`. Две фазы:
  raw_manifest — по сырому манифесту после load_raw_day;
  dds_load_log   — по журналу загрузок DDS после dds_jobs.

Одна запись на пару (process_date, phase): перед вставкой строка с тем же ключом удаляется.
"""
from __future__ import annotations

import json
from datetime import date, datetime, timezone
from typing import Any

from load_raw_day import (
    quote_schema,
    quote_table,
    sql_date,
    sql_string,
    sql_timestamptz,
)


def dq_summary_phase_raw() -> str:
    """Значение колонки phase для сводки по raw_load_manifest."""
    return "raw_manifest"


def dq_summary_phase_dds() -> str:
    """Значение колонки phase для сводки по dds_table_load_log."""
    return "dds_load_log"


def ensure_dq_daily_summary_table(dst_cur, catalog: str, validate_schema: str) -> str:
    """Создаёт схему и таблицу при необходимости; возвращает полное имя таблицы."""
    summary_ref = quote_table(catalog, validate_schema, "dq_daily_summary")
    dst_cur.execute(f"CREATE SCHEMA IF NOT EXISTS {quote_schema(catalog, validate_schema)}")
    dst_cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {summary_ref} (
            process_date DATE NOT NULL,
            phase VARCHAR NOT NULL,
            evaluated_at TIMESTAMP(6) WITH TIME ZONE NOT NULL,
            aggregates_json VARCHAR,
            summary_line VARCHAR NOT NULL
        )
        """
    )
    return summary_ref


def write_dq_daily_summary(
    dst_cur,
    *,
    summary_ref: str,
    process_date: date,
    phase: str,
    aggregates: dict[str, Any],
    summary_line: str,
) -> None:
    """DELETE+INSERT одной строки сводки (идемпотентный перезапуск задачи Airflow)."""
    evaluated = datetime.now(timezone.utc)
    dst_cur.execute(
        f"""
        DELETE FROM {summary_ref}
        WHERE process_date = {sql_date(process_date)}
          AND phase = {sql_string(phase)}
        """
    )
    dst_cur.execute(
        f"""
        INSERT INTO {summary_ref} (
            process_date, phase, evaluated_at, aggregates_json, summary_line
        )
        VALUES (
            {sql_date(process_date)},
            {sql_string(phase)},
            {sql_timestamptz(evaluated)},
            {sql_string(json.dumps(aggregates, ensure_ascii=False))},
            {sql_string(summary_line)}
        )
        """
    )


def commit_trino_if_supported(conn) -> None:
    """У части Trino-клиентов есть commit(); у части — нет. Не падаем ни в каком случае."""
    fn = getattr(conn, "commit", None)
    if callable(fn):
        fn()
