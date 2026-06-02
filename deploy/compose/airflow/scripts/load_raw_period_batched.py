"""Загрузка RAW-слоя за длительный период путём разбиения на подпериоды.

Этот скрипт — **внешний цикл** батчинга. Он НЕ загружает данные сам,
а запускает ``load_raw_period_batch.py`` как subprocess для каждого
подпериода (чанка) заданной длины.

Отличие от ``load_raw_period_batch.py``:
- Тот загружает один непрерывный отрезок дат за один вызов Trino.
- Этот разбивает длинный период на чанки по ``--batch-days`` дней и
  последовательно вызывает batch-загрузчик для каждого чанка.

Это позволяет:
- Загружать произвольно длинные периоды без риска OOM.
- Перезапускать отдельные чанки при сбоях (в сочетании с ретраями).
- Равномернее распределять нагрузку на Trino.

Параметры:
  --start-day YYYY-MM-DD — начало периода (включительно).
  --end-day YYYY-MM-DD — конец периода (включительно).
  --batch-days N — размер чанка в днях (по умолчанию из конфига или 7).
  --batch-retries N — число ретраев для упавшего чанка.
  --retry-delay-sec N — пауза между ретраями в секундах.
"""
from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from load_raw_day import load_config, setup_logging

LOGGER = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """Разбирает аргументы командной строки для батчированной загрузки периода.

    Returns:
        argparse.Namespace с атрибутами config, start_day, end_day,
        batch_days, tz, log_level, dry_run, batch_retries, retry_delay_sec.
    """
    parser = argparse.ArgumentParser(
        description="Load raw layer for period by running batch loader per chunk",
    )
    parser.add_argument(
        "--config",
        default="/opt/airflow/scripts/raw_load_config.json",
        help="Path to JSON config",
    )
    parser.add_argument(
        "--start-day",
        required=True,
        help="Start day in YYYY-MM-DD format (inclusive)",
    )
    parser.add_argument(
        "--end-day",
        required=True,
        help="End day in YYYY-MM-DD format (inclusive)",
    )
    parser.add_argument(
        "--batch-days",
        type=int,
        default=None,
        help="Batch size in days (if omitted, resolve from config load.period_batch_days or fallback 7)",
    )
    parser.add_argument(
        "--tz",
        default="UTC",
        help="Timezone passed to batch loader (example: Europe/Moscow)",
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
    parser.add_argument(
        "--batch-retries",
        type=int,
        default=None,
        help="How many retries for one failed batch (if omitted, resolve from config load.period_batch_retries or fallback 3)",
    )
    parser.add_argument(
        "--retry-delay-sec",
        type=int,
        default=None,
        help="Pause in seconds before retrying failed batch (if omitted, resolve from config load.period_retry_delay_sec or fallback 30)",
    )
    return parser.parse_args()


def resolve_batch_days(args_batch_days: int | None, load_cfg: dict[str, Any]) -> int:
    """Определяет размер чанка: аргумент → конфиг → 7 (fallback).

    Args:
        args_batch_days: Значение --batch-days или None.
        load_cfg: Секция ``load`` конфига.

    Returns:
        Размер чанка в днях (>0).

    Raises:
        ValueError: Если значение <= 0.
    """
    if args_batch_days is not None:
        batch_days = args_batch_days
    else:
        batch_days = int(load_cfg.get("period_batch_days", 7))
    if batch_days <= 0:
        raise ValueError("Batch size must be greater than 0")
    return batch_days


def resolve_batch_retries(args_batch_retries: int | None, load_cfg: dict[str, Any]) -> int:
    """Определяет число ретраев для чанка: аргумент → конфиг → 3 (fallback).

    Args:
        args_batch_retries: Значение --batch-retries или None.
        load_cfg: Секция ``load`` конфига.

    Returns:
        Число ретраев (>=0).

    Raises:
        ValueError: Если значение < 0.
    """
    if args_batch_retries is not None:
        retries = args_batch_retries
    else:
        retries = int(load_cfg.get("period_batch_retries", 3))
    if retries < 0:
        raise ValueError("Retry count must be >= 0")
    return retries


def resolve_retry_delay(args_retry_delay_sec: int | None, load_cfg: dict[str, Any]) -> int:
    """Определяет паузу между ретраями: аргумент → конфиг → 30 (fallback).

    Args:
        args_retry_delay_sec: Значение --retry-delay-sec или None.
        load_cfg: Секция ``load`` конфига.

    Returns:
        Пауза в секундах (>=0).

    Raises:
        ValueError: Если значение < 0.
    """
    if args_retry_delay_sec is not None:
        delay_sec = args_retry_delay_sec
    else:
        delay_sec = int(load_cfg.get("period_retry_delay_sec", 30))
    if delay_sec < 0:
        raise ValueError("Retry delay must be >= 0")
    return delay_sec


def iter_batches(start_day: date, end_day: date, batch_days: int):
    """Генератор: разбивает диапазон дат на чанки по batch_days дней.

    Args:
        start_day: Начало диапазона (включительно).
        end_day: Конец диапазона (включительно).
        batch_days: Размер чанка в днях.

    Yields:
        Кортежи (batch_start, batch_end) — обе границы включительно.
    """
    current_start = start_day
    while current_start <= end_day:
        current_end = min(current_start + timedelta(days=batch_days - 1), end_day)
        yield current_start, current_end
        current_start = current_end + timedelta(days=1)


def run_batch_loader(
    script_path: Path,
    config_path: str,
    batch_start: date,
    batch_end: date,
    days_count: int,
    tz_name: str,
    log_level: str,
    dry_run: bool,
    max_retries: int,
    retry_delay_sec: int,
) -> None:
    """Запускает load_raw_period_batch.py как subprocess для одного чанка.

    При неудаче повторяет до max_retries раз с паузой retry_delay_sec.

    Args:
        script_path: Путь к load_raw_period_batch.py.
        config_path: Путь к JSON-конфигу.
        batch_start: Начало чанка.
        batch_end: Конец чанка.
        days_count: Число дней в чанке.
        tz_name: Часовой пояс.
        log_level: Уровень логирования.
        dry_run: Режим dry-run.
        max_retries: Максимальное число ретраев (0 = без ретраев).
        retry_delay_sec: Пауза между ретраями.

    Raises:
        subprocess.CalledProcessError: Если все попытки исчерпаны.
    """
    cmd = [
        sys.executable,
        str(script_path),
        "--config",
        config_path,
        "--day",
        batch_start.isoformat(),
        "--days-count",
        str(days_count),
        "--tz",
        tz_name,
        "--log-level",
        log_level,
    ]
    if dry_run:
        cmd.append("--dry-run")
    total_attempts = max_retries + 1
    for attempt in range(1, total_attempts + 1):
        try:
            subprocess.run(cmd, check=True)
            if attempt > 1:
                LOGGER.info(
                    "[INFO] batch recovered after retry: range=[%s..%s] attempt=%s/%s",
                    batch_start.isoformat(),
                    batch_end.isoformat(),
                    attempt,
                    total_attempts,
                )
            return
        except subprocess.CalledProcessError:
            if attempt >= total_attempts:
                LOGGER.error(
                    "[ERROR] batch failed with no retries left: range=[%s..%s] attempts=%s",
                    batch_start.isoformat(),
                    batch_end.isoformat(),
                    total_attempts,
                )
                raise
            LOGGER.warning(
                "[WARN] batch failed: range=[%s..%s] attempt=%s/%s retry_in_sec=%s",
                batch_start.isoformat(),
                batch_end.isoformat(),
                attempt,
                total_attempts,
                retry_delay_sec,
            )
            if retry_delay_sec > 0:
                time.sleep(retry_delay_sec)


def main() -> None:
    """Точка входа: разбивает период на чанки и последовательно загружает каждый.

    Алгоритм:
    1. Парсинг аргументов и конфига.
    2. Валидация диапазона дат.
    3. Расчёт числа чанков.
    4. Для каждого чанка — вызов run_batch_loader с ретраями.
    5. Логирование общего прогресса.
    """
    args = parse_args()
    setup_logging(args.log_level)

    config = load_config(args.config)
    load_cfg = config.get("load", {})

    start_day = date.fromisoformat(args.start_day)
    end_day = date.fromisoformat(args.end_day)
    if start_day > end_day:
        raise ValueError("--start-day must be <= --end-day")

    batch_days = resolve_batch_days(args.batch_days, load_cfg)
    batch_retries = resolve_batch_retries(args.batch_retries, load_cfg)
    retry_delay_sec = resolve_retry_delay(args.retry_delay_sec, load_cfg)
    total_days = (end_day - start_day).days + 1
    total_batches = (total_days + batch_days - 1) // batch_days

    batch_loader_path = Path(__file__).with_name("load_raw_period_batch.py")
    if not batch_loader_path.exists():
        raise FileNotFoundError(f"Batch loader not found: {batch_loader_path}")

    LOGGER.info(
        "[INFO] batched period load start: requested=[%s..%s] total_days=%s batch_days=%s total_batches=%s tz=%s dry_run=%s",
        start_day.isoformat(),
        end_day.isoformat(),
        total_days,
        batch_days,
        total_batches,
        args.tz,
        args.dry_run,
    )
    LOGGER.info(
        "[INFO] batch retry policy: retries_per_batch=%s retry_delay_sec=%s",
        batch_retries,
        retry_delay_sec,
    )

    for index, (batch_start, batch_end) in enumerate(
        iter_batches(start_day=start_day, end_day=end_day, batch_days=batch_days),
        start=1,
    ):
        days_count = (batch_end - batch_start).days + 1
        LOGGER.info(
            "[INFO] run batch %s/%s: range=[%s..%s] days_count=%s",
            index,
            total_batches,
            batch_start.isoformat(),
            batch_end.isoformat(),
            days_count,
        )
        run_batch_loader(
            script_path=batch_loader_path,
            config_path=args.config,
            batch_start=batch_start,
            batch_end=batch_end,
            days_count=days_count,
            tz_name=args.tz,
            log_level=args.log_level,
            dry_run=args.dry_run,
            max_retries=batch_retries,
            retry_delay_sec=retry_delay_sec,
        )

    LOGGER.info("[INFO] batched period load finished")


if __name__ == "__main__":
    main()
