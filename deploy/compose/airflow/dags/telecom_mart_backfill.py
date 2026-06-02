"""Пересчёт только слоя **MART** (Iceberg через Trino) за диапазон календарных дней.

RAW, DDS, DQ и водяной знак ``telecom_kpi_daily`` **не вызываются** — предполагается, что DDS
уже заполнен за нужный период. Для каждого дня inclusive последовательно выполняются все
``mart_jobs`` (``gen_dim_tariff``, ``gen_kpi_*.py``) в порядке ``telecom_mart_job_scripts.MART_JOB_SCRIPTS``.

**ClickHouse serving** этим DAG не обновляется; после MART при необходимости запустите
``telecom_mart_clickhouse_daily``, DAG **``telecom_mart_vitrina_backfill``** (одна витрина + период)
или ``mart_serving_clickhouse.py`` с ``--full-table``.

Источник диапазона (приоритет сверху вниз):

1. **Configuration JSON** при триггере::

       {"start_date": "2024-01-01", "end_date": "2024-01-31"}

2. **Params** DAG (непустые строки ``start_date`` / ``end_date``).

3. Переменные окружения на воркере::

       TELECOM_MART_BACKFILL_START=2024-01-01
       TELECOM_MART_BACKFILL_END=2024-01-31

Дополнительные ключи в ``conf`` / **Params** (опционально):

- ``ensure_all_ddl`` (bool) — на первом дне к первому скрипту добавить ``--ensure-all-ddl``
  (создание всех KPI-таблиц в mart, если контур новый).
"""

from __future__ import annotations

import logging
import os
import subprocess
from datetime import date, timedelta
from typing import Any

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Param

from telecom_mart_job_scripts import MART_JOB_SCRIPTS

LOGGER = logging.getLogger(__name__)

RAW_CONFIG = "/opt/airflow/scripts/raw_load_config.json"
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


def _resolve_range_and_flags(context: dict) -> tuple[date, date, bool]:
    """Извлекает диапазон дат и флаг ``ensure_all_ddl`` из контекста задачи.

    Args:
        context: Контекст Airflow (dag_run, params).

    Returns:
        Кортеж ``(start_date, end_date, ensure_all_ddl)``.
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

    start_s = _pick("start_date", "from_date")
    end_s = _pick("end_date", "to_date")
    if not start_s or not end_s:
        start_s = start_s or os.getenv("TELECOM_MART_BACKFILL_START", "").strip()
        end_s = end_s or os.getenv("TELECOM_MART_BACKFILL_END", "").strip()
    if not start_s or not end_s:
        raise ValueError(
            "Укажите диапазон: conf/params start_date и end_date (YYYY-MM-DD), "
            "или TELECOM_MART_BACKFILL_START / TELECOM_MART_BACKFILL_END в окружении."
        )

    start_d = date.fromisoformat(start_s)
    end_d = date.fromisoformat(end_s)

    ensure_raw = conf.get("ensure_all_ddl", params_obj.get("ensure_all_ddl", False))
    ensure = _truthy(ensure_raw)

    return start_d, end_d, ensure


def run_mart_layer_backfill(**context: Any) -> None:
    """Последовательный пересчёт всех MART-витрин за каждый день диапазона.

    Для каждого дня запускает скрипты из ``MART_JOB_SCRIPTS`` в заданном порядке.
    При ``ensure_all_ddl=True`` первому скрипту первого дня добавляется ``--ensure-all-ddl``.
    Не вызывает RAW/DDS/DQ — предполагается, что DDS уже заполнен.

    Args:
        context: Контекст задачи Airflow.
    """
    start_d, end_d, ensure_all_ddl = _resolve_range_and_flags(context)
    days = _daterange_inclusive(start_d, end_d)

    LOGGER.info(
        "[INFO] mart_backfill: days %s..%s count=%s ensure_all_ddl=%s jobs=%s",
        start_d.isoformat(),
        end_d.isoformat(),
        len(days),
        ensure_all_ddl,
        len(MART_JOB_SCRIPTS),
    )

    for d in days:
        for idx, script in enumerate(MART_JOB_SCRIPTS):
            cmd: list[str] = [
                "python3",
                f"/opt/airflow/scripts/mart_jobs/{script}",
                "--config",
                RAW_CONFIG,
                "--report-date",
                d.isoformat(),
            ]
            # --ensure-all-ddl только для первого скрипта первого дня (создание таблиц при холодном старте).
            if ensure_all_ddl and d == start_d and idx == 0:
                cmd.append("--ensure-all-ddl")

            LOGGER.info("[INFO] mart_backfill: %s", " ".join(cmd))
            proc = subprocess.run(
                cmd,
                check=False,
                capture_output=True,
                text=True,
            )
            if proc.returncode != 0:
                err = (proc.stderr or "").strip() or (proc.stdout or "").strip() or "(no output)"
                raise RuntimeError(
                    f"MART backfill failed: report_date={d.isoformat()} script={script} "
                    f"rc={proc.returncode}: {err[:8000]}"
                )

    LOGGER.info("[INFO] mart_backfill: finished %s calendar days", len(days))


_DOC_MD = __doc__ or ""

with DAG(
    dag_id="telecom_mart_backfill",
    description="Пересчёт MART (gen_kpi_*) за диапазон дат без RAW/DDS",
    doc_md=_DOC_MD,
    schedule=None,
    start_date=START,
    catchup=False,
    max_active_runs=1,
    tags=["telecom-mart", "mart", "kpi", "backfill"],
    params={
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
            title="Первый прогон: --ensure-all-ddl на первом скрипте первого дня",
        ),
    },
    default_args={
        "depends_on_past": False,
        "retries": 0,
    },
) as dag:
    PythonOperator(
        task_id="mart_layer_sequential_backfill",
        python_callable=run_mart_layer_backfill,
    )
