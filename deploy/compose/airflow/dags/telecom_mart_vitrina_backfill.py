"""Пересчёт **одной** MART-витрины (Iceberg / Trino) за диапазон календарных дней.

RAW, DDS и водяной знак ``telecom_kpi_daily`` не затрагиваются. Для всех витрин подряд за период
используйте ``telecom_mart_backfill``.

**Триггер** (пример)::

    airflow dags trigger telecom_mart_vitrina_backfill -c '{
      "vitrina": "revenue",
      "start_date": "2024-06-01",
      "end_date": "2024-06-30",
      "sync_clickhouse": true,
      "clickhouse_full_table": false
    }'

Параметры (``conf`` / **Params** DAG, приоритет у ``conf``):

- ``vitrina`` — ключ витрины (``revenue``, ``kpi_revenue_daily``, ``ab0``, …); см. ``mart_runner_common.normalize_vitrina_key``.
- ``start_date``, ``end_date`` — inclusive, ``YYYY-MM-DD``.
- ``ensure_all_ddl`` (bool) — на первом дне: ``--ensure-all-ddl`` (все KPI-таблицы в mart).
- ``sync_clickhouse`` (bool) — после каждого дня MART (или один раз в конце) обновить ClickHouse serving.
- ``clickhouse_full_table`` (bool) — при ``sync_clickhouse``: один ``--full-table`` после всех дней MART;
  иначе ``--report-date`` за каждый день. Для ``dim_tariff`` всегда полная выгрузка.

Окружение (если нет ``conf``/params): ``TELECOM_MART_VITRINA``, ``TELECOM_MART_VITRINA_START``,
``TELECOM_MART_VITRINA_END``.

**Зависимости данных** (MART не пересчитывает другие витрины автоматически):

- ``ab0`` / ``ab30`` / ``ab90`` — за те же дни нужны ``inflow`` и ``outflow``.
- ``revenue_active_*`` — в mart должна быть актуальная ``kpi_arpu_daily`` (колонка ``active_clients``).
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
from datetime import date, timedelta
from typing import Any

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Param

from telecom_mart_job_scripts import MART_VITRINA_KEYS

LOGGER = logging.getLogger(__name__)

RAW_CONFIG = "/opt/airflow/scripts/raw_load_config.json"
SCRIPTS_DIR = "/opt/airflow/scripts"
RAW_TZ = "Europe/Moscow"
START = pendulum.datetime(2024, 1, 1, tz=RAW_TZ)


def _daterange_inclusive(start: date, end: date) -> list[date]:
    """Список календарных дней от ``start`` до ``end`` включительно.

    Args:
        start: Первый день (включительно).
        end: Последний день (включительно).

    Returns:
        Список дат в порядке возрастания.

    Raises:
        ValueError: Если ``start > end``.
    """
    if start > end:
        raise ValueError(f"start_date ({start}) позже end_date ({end})")
    out: list[date] = []
    cur = start
    while cur <= end:
        out.append(cur)
        cur += timedelta(days=1)
    return out


def _truthy(val: Any) -> bool:
    """Интерпретация значения как булева: None → False, "1"/"true"/"yes"/"on" → True.

    Args:
        val: Значение из conf/params.

    Returns:
        Булева интерпретация.
    """
    if val is None:
        return False
    if isinstance(val, bool):
        return val
    s = str(val).strip().lower()
    return s in {"1", "true", "yes", "on"}


def _run_subprocess(cmd: list[str], *, label: str) -> None:
    """Запуск подпроцесса с проверкой кода возврата.

    Args:
        cmd: Команда и аргументы.
        label: Метка для логирования и сообщения об ошибке.

    Raises:
        RuntimeError: Если код возврата ≠ 0.
    """
    LOGGER.info("[INFO] %s: %s", label, " ".join(cmd))
    proc = subprocess.run(cmd, check=False, capture_output=True, text=True)
    if proc.returncode != 0:
        err = (proc.stderr or "").strip() or (proc.stdout or "").strip() or "(no output)"
        raise RuntimeError(f"{label} failed rc={proc.returncode}: {err[:8000]}")


def _resolve_context(context: dict) -> tuple[str, date, date, bool, bool, bool]:
    """Извлекает параметры витрины и диапазон дат из контекста задачи.

    Приоритет: Configuration JSON → Params → переменные окружения.

    Args:
        context: Контекст Airflow.

    Returns:
        Кортеж ``(vitrina, start_date, end_date, ensure_all_ddl, sync_clickhouse, clickhouse_full_table)``.
    """
    dag_run = context.get("dag_run")
    conf = (dag_run.conf if dag_run else None) or {}
    if not isinstance(conf, dict):
        conf = {}
    params_obj = context.get("params") or {}

    def _pick(*keys: str) -> str | None:
        """Возвращает первое непустое значение из conf или params по цепочке ключей.

        Приоритет: Configuration JSON → Params DAG.

        Args:
            *keys: Имена ключей для поиска (например ``"start_date"``, ``"from_date"``).

        Returns:
            Строковое значение или None.
        """
        for k in keys:
            v = conf.get(k)
            if v is not None and str(v).strip() != "":
                return str(v).strip()
            v = params_obj.get(k)
            if v is not None and str(v).strip() != "":
                return str(v).strip()
        return None

    vitrina_s = _pick("vitrina")
    if not vitrina_s:
        vitrina_s = os.getenv("TELECOM_MART_VITRINA", "").strip()
    if not vitrina_s:
        raise ValueError(
            "Укажите vitrina в conf/params (например revenue, ab0, receipts) "
            "или TELECOM_MART_VITRINA в окружении."
        )

    start_s = _pick("start_date", "from_date")
    end_s = _pick("end_date", "to_date")
    if not start_s or not end_s:
        start_s = start_s or os.getenv("TELECOM_MART_VITRINA_START", "").strip()
        end_s = end_s or os.getenv("TELECOM_MART_VITRINA_END", "").strip()
    if not start_s or not end_s:
        raise ValueError(
            "Укажите диапазон: conf/params start_date и end_date (YYYY-MM-DD), "
            "или TELECOM_MART_VITRINA_START / TELECOM_MART_VITRINA_END."
        )

    ensure_all_ddl = _truthy(conf.get("ensure_all_ddl", params_obj.get("ensure_all_ddl", False)))
    sync_clickhouse = _truthy(conf.get("sync_clickhouse", params_obj.get("sync_clickhouse", False)))
    ch_full = _truthy(conf.get("clickhouse_full_table", params_obj.get("clickhouse_full_table", False)))

    return (
        vitrina_s,
        date.fromisoformat(start_s),
        date.fromisoformat(end_s),
        ensure_all_ddl,
        sync_clickhouse,
        ch_full,
    )


def run_mart_vitrina_backfill(**context: Any) -> None:
    """Пересчёт одной MART-витрины за диапазон дат с опциональной синхронизацией ClickHouse.

    Для каждого дня:
    1. Запускает ``mart_vitrina_runner.py`` с ``--vitrina`` и ``--report-date``.
    2. При ``sync_clickhouse=True`` и ``clickhouse_full_table=False`` —
       инкрементальная репликация в CH через ``mart_serving_clickhouse.py --report-date``.
    После всех дней — при ``clickhouse_full_table=True`` или ``dim_tariff`` —
    одна полная выгрузка ``--full-table``.

    Args:
        context: Контекст задачи Airflow.
    """
    vitrina_raw, start_d, end_d, ensure_all_ddl, sync_clickhouse, clickhouse_full_table = _resolve_context(
        context
    )
    days = _daterange_inclusive(start_d, end_d)

    if SCRIPTS_DIR not in sys.path:
        sys.path.insert(0, SCRIPTS_DIR)
    from mart_runner_common import (  # noqa: PLC0415
        MART_TARGET_TABLE_BY_VITRINA_KEY,
        normalize_vitrina_key,
    )

    vitrina_key = normalize_vitrina_key(vitrina_raw)
    serving_table = MART_TARGET_TABLE_BY_VITRINA_KEY[vitrina_key]
    # Режимы синхронизации ClickHouse: per-day (инкремент) или один full-table (dim_tariff всегда full).
    ch_per_day = sync_clickhouse and not clickhouse_full_table and vitrina_key != "dim_tariff"
    ch_full_once = sync_clickhouse and (clickhouse_full_table or vitrina_key == "dim_tariff")

    LOGGER.info(
        "[INFO] mart_vitrina_backfill: vitrina=%s (%s) days=%s..%s count=%s "
        "ensure_all_ddl=%s sync_ch=%s ch_per_day=%s ch_full=%s",
        vitrina_raw,
        vitrina_key,
        start_d.isoformat(),
        end_d.isoformat(),
        len(days),
        ensure_all_ddl,
        sync_clickhouse,
        ch_per_day,
        ch_full_once,
    )

    runner = f"{SCRIPTS_DIR}/mart_vitrina_runner.py"
    serving = f"{SCRIPTS_DIR}/mart_serving_clickhouse.py"

    for idx, d in enumerate(days):
        mart_cmd: list[str] = [
            "python3",
            runner,
            "--config",
            RAW_CONFIG,
            "--vitrina",
            vitrina_key,
            "--report-date",
            d.isoformat(),
        ]
        if ensure_all_ddl and idx == 0:
            mart_cmd.append("--ensure-all-ddl")
        _run_subprocess(mart_cmd, label=f"mart:{vitrina_key}:{d.isoformat()}")

        if ch_per_day:
            ch_cmd = [
                "python3",
                serving,
                "--config",
                RAW_CONFIG,
                "--table",
                serving_table,
                "--report-date",
                d.isoformat(),
            ]
            _run_subprocess(ch_cmd, label=f"serving:{serving_table}:{d.isoformat()}")

    if ch_full_once:
        ch_cmd = [
            "python3",
            serving,
            "--config",
            RAW_CONFIG,
            "--table",
            serving_table,
            "--full-table",
        ]
        _run_subprocess(ch_cmd, label=f"serving:{serving_table}:full")

    LOGGER.info(
        "[INFO] mart_vitrina_backfill: finished vitrina=%s days=%s",
        vitrina_key,
        len(days),
    )


_DOC_MD = __doc__ or ""

with DAG(
    dag_id="telecom_mart_vitrina_backfill",
    description="Пересчёт одной MART-витрины за диапазон дат (опц. ClickHouse)",
    doc_md=_DOC_MD,
    schedule=None,
    start_date=START,
    catchup=False,
    max_active_runs=1,
    tags=["telecom-mart", "mart", "kpi", "backfill", "vitrina"],
    params={
        "vitrina": Param(
            default="revenue",
            type="string",
            enum=list(MART_VITRINA_KEYS),
            title="Витрина (ключ CLI mart_vitrina_runner)",
        ),
        "start_date": Param(
            default=None,
            type=["null", "string"],
            title="Первый день inclusive (YYYY-MM-DD)",
        ),
        "end_date": Param(
            default=None,
            type=["null", "string"],
            title="Последний день inclusive (YYYY-MM-DD)",
        ),
        "ensure_all_ddl": Param(
            default=False,
            type="boolean",
            title="Первый день: --ensure-all-ddl",
        ),
        "sync_clickhouse": Param(
            default=False,
            type="boolean",
            title="Обновить ClickHouse serving после MART",
        ),
        "clickhouse_full_table": Param(
            default=False,
            type="boolean",
            title="CH: одна полная выгрузка (--full-table) вместо по дням",
        ),
    },
    default_args={
        "depends_on_past": False,
        "retries": 0,
    },
) as dag:
    PythonOperator(
        task_id="mart_vitrina_backfill",
        python_callable=run_mart_vitrina_backfill,
    )
