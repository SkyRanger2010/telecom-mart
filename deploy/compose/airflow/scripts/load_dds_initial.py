"""Пакетная первичная сборка DDS: все таблицы из конфига (обёртка над dds_table_runner).

Назначение
----------
Скрипт выполняет начальную (initial) загрузку ВСЕХ таблиц, перечисленных
в ``raw_load_config.json``, из RAW-слоя в DDS.

Алгоритм:
  1. Чтение конфига — список таблиц, параметры подключения, DDS-настройки.
  2. Последовательный вызов ``load_one_table()`` для каждой строки конфига
     (из ``dds_table_runner``).
  3. Для каждой таблицы принудительно ``dds_full_rebuild = True`` —
     всегда полное пересоздание, без дельты.
  4. После каждой таблицы: обновление манифеста ``dds_initial_manifest``
     и служебного состояния ``dds_load_service_state``.
  5. Итоговый лог с суммарным количеством строк.

Отличие от ежедневных dds_jobs
------------------------------
- **initial**: все таблицы за один прогон, ``dds_full_rebuild = True``,
  пишет манифест и служебное состояние.
- **dds_jobs**: одна таблица за прогон (задача Airflow), инкрементальная
  дельта через ``*__events``, не трогает манифест, не пишет состояние
  (передаются ``--skip-manifest``, без ``--write-service-state``).

Параметры CLI
-------------
* ``--config`` — путь к JSON-конфигу
  (по умолчанию ``/opt/airflow/scripts/raw_load_config.json``).
* ``--cutoff-day`` — дата исторического среза ``YYYY-MM-DD``
  (по умолчанию ``2023-12-31``).
* ``--log-level`` — уровень логирования
  (по умолчанию ``INFO``).
* ``--dry-run`` — только подсчёт строк, без записи.
* ``--skip-validate-sql`` — пропустить ``dds_validate/*.sql``.
* ``--skip-clean-sql`` — пропустить ``dds_clean/*.sql``.
"""
from __future__ import annotations

import argparse
import logging
import os
from argparse import Namespace
from datetime import date

from dds_table_runner import (
    ensure_dds_service_table,
    ensure_manifest_table,
    load_config,
    load_one_table,
    setup_logging,
    write_dds_service_state,
    write_manifest_rows,
)
from load_raw_day import build_trino_target_config, connect_trino, parse_table_configs

LOGGER = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """Разобрать аргументы командной строки для пакетной initial-загрузки.

    Returns:
        Namespace с аргументами: config, cutoff_day, log_level, dry_run,
        skip_validate_sql, skip_clean_sql.
    """
    parser = argparse.ArgumentParser(
        description="Build initial DDS snapshot for all configured tables",
    )
    parser.add_argument(
        "--config",
        default="/opt/airflow/scripts/raw_load_config.json",
        help="Path to JSON config",
    )
    parser.add_argument(
        "--cutoff-day",
        default="2023-12-31",
        help="Historical cutoff day in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--log-level",
        default=os.getenv("RAW_LOADER_LOG_LEVEL", "INFO"),
        help="Logging level",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only row counts",
    )
    parser.add_argument(
        "--skip-validate-sql",
        action="store_true",
        help="Не выполнять dds_validate/*.sql перед загрузкой каждой таблицы",
    )
    parser.add_argument(
        "--skip-clean-sql",
        action="store_true",
        help="Не выполнять dds_clean/*.sql",
    )
    return parser.parse_args()


def dds_schema_name(cfg: dict) -> str:
    """Имя схемы DDS из конфига, по умолчанию ``dds``.

    Args:
        cfg: Полный словарь конфигурации.

    Returns:
        Имя схемы (строка).
    """
    return str(cfg.get("dds", {}).get("schema", "dds"))


def dds_service_table_name(cfg: dict) -> str:
    """Имя служебной таблицы состояния загрузки DDS.

    Args:
        cfg: Полный словарь конфигурации.

    Returns:
        Имя таблицы, по умолчанию ``dds_load_service_state``.
    """
    return str(cfg.get("dds", {}).get("state_table", "dds_load_service_state"))


def dds_loader_name(cfg: dict) -> str:
    """Имя загрузчика для подписи в служебной таблице.

    Args:
        cfg: Полный словарь конфигурации.

    Returns:
        Имя загрузчика, по умолчанию ``dds_initial_loader``.
    """
    return str(cfg.get("dds", {}).get("loader_name", "dds_initial_loader"))


def main() -> None:
    """Главная точка входа: пакетная загрузка всех таблиц конфига в DDS.

    Для каждой таблицы:
      - Создаётся Namespace с ``dds_full_rebuild = True`` (полное пересоздание).
      - Вызывается ``load_one_table()``, результат добавляется в ``built_stats``.
      - Обновляется манифест и служебное состояние.
    """
    args = parse_args()
    setup_logging(args.log_level)
    cutoff_day = date.fromisoformat(args.cutoff_day)

    config = load_config(args.config)
    source_cfg = config["source"]
    source_default_schema = str(source_cfg.get("default_schema", "public"))
    target_cfg = config["target"]
    trino_target = build_trino_target_config(
        target_cfg, fallback_schema=str(target_cfg.get("default_schema", "raw"))
    )
    dds_schema = dds_schema_name(config)

    table_configs = parse_table_configs(config["tables"], default_source_schema=source_default_schema)
    built_stats = []  # Аккумулируем статистику по всем таблицам
    LOGGER.info(
        "[INFO] dds initial (all tables) start: cutoff_day=%s tables=%s",
        cutoff_day.isoformat(),
        len(table_configs),
    )

    for table in table_configs:
        # Формируем Namespace с параметрами для load_one_table
        ns = Namespace(
            table=table.name,
            config=args.config,
            cutoff_day=args.cutoff_day,
            log_level=args.log_level,
            dry_run=args.dry_run,
            skip_pre_count=False,
            write_service_state=False,       # состояние пишем отдельно ниже
            skip_manifest=True,              # манифест ведём сами
            skip_load_log=False,
            skip_validate_sql=args.skip_validate_sql,
            skip_clean_sql=args.skip_clean_sql,
            dds_full_rebuild=True,           # всегда полное пересоздание для initial
            dds_legacy_flat=False,
            dds_day_tz="Europe/Moscow",
        )
        built_stats.append(load_one_table(args=ns))

        # После каждой таблицы обновляем манифест и состояние
        with connect_trino(trino_target) as dst_conn:
            with dst_conn.cursor() as dst_cur:
                dst_cur.execute(trino_target.verify_connection_sql)
                _ = dst_cur.fetchone()
                manifest_ref = ensure_manifest_table(dst_cur, trino_target.catalog, dds_schema)
                write_manifest_rows(dst_cur, manifest_ref, cutoff_day, built_stats)
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
                LOGGER.info(
                    "[INFO] aggregate manifest + service state written for cutoff=%s",
                    cutoff_day.isoformat(),
                )
    LOGGER.info(
        "[INFO] dds initial done: total_rows=%s",
        sum(s.rows for s in built_stats),
    )


if __name__ == "__main__":
    main()
