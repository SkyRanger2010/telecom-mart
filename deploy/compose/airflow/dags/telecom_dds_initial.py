"""Первичная сборка DDS: все таблицы из конфига (``load_dds_initial.py``).

Назначение: однократное заполнение слоя DDS историческими данными из RAW
до заданной даты отсечения (cutoff).

DAG **без расписания** (``schedule=None``) — запуск только вручную или триггером
после того, как в RAW уже прогружена история до cutoff
(см. ``bootstrap_last_loaded_day`` в raw_load_config.json).

При триггере из UI можно передать JSON конфигурации DagRun:
``{"cutoff_day": "2023-12-31"}``. Если ключ не указан — используется
``DDS_INITIAL_CUTOFF`` (константа модуля).

После завершения этого DAG можно запускать ежедневный ``telecom_kpi_daily``.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

RAW_CONFIG = "/opt/airflow/scripts/raw_load_config.json"
RAW_TZ = "Europe/Moscow"
# Дата отсечения по умолчанию: все данные RAW до этой даты будут загружены в DDS.
DDS_INITIAL_CUTOFF = "2023-12-31"

START = pendulum.datetime(2024, 1, 1, tz=RAW_TZ)

with DAG(
    dag_id="telecom_dds_initial",
    description="Разовая/ручная первичная загрузка всех объектов DDS (cutoff as-of)",
    schedule=None,
    start_date=START,
    catchup=False,
    max_active_runs=1,
    tags=["telecom-mart", "dds", "bootstrap", "initial"],
    params={"cutoff_day": DDS_INITIAL_CUTOFF},
    default_args={
        "depends_on_past": False,
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
    },
    doc_md=__doc__,
) as dag:
    # cutoff передаётся через переменную окружения, а не через аргумент CLI.
    # Это корректнее для BashOperator (Airflow 3) и безопаснее, чем .get на не-словарном conf.
    dds_initial_load = BashOperator(
        task_id="dds_initial_load",
        bash_command=(
            "python3 /opt/airflow/scripts/load_dds_initial.py "
            f"--config {RAW_CONFIG} "
            '--cutoff-day "${DDS_INITIAL_CUTOFF_DAY}"'
        ),
        env={
            # Jinja2: извлекает cutoff_day из dag_run.conf (если словарь),
            # иначе fallback на params.cutoff_day (константа DAG).
            "DDS_INITIAL_CUTOFF_DAY": (
                "{% set _c = dag_run.conf if dag_run.conf is mapping else {} %}"
                "{{ _c.get('cutoff_day', params.cutoff_day) }}"
            ),
        },
    )
