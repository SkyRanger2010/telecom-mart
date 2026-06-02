"""Ежедневное копирование KPI-витрин из Iceberg MART (Trino) в ClickHouse.

Для каждой таблицы: **полная** выгрузка из Iceberg (без фильтра по дню загрузки RAW/DDS):
``TRUNCATE`` целевой таблицы в ClickHouse, затем потоковый ``INSERT`` всех строк.

Скрипт: ``scripts/mart_serving_clickhouse.py`` с флагом ``--full-table``. Если в Trino
таблица пуста, задача **не** очищает ClickHouse (см. ``--allow-empty-source`` для принудительного
``TRUNCATE`` и пустой витрины).

Инкремент «только за один календарный день» остаётся в CLI через ``--report-date`` (без этого DAG).
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

RAW_CONFIG = "/opt/airflow/scripts/raw_load_config.json"
RAW_TZ = "Europe/Moscow"
START = pendulum.datetime(2024, 1, 1, tz=RAW_TZ)

SERVING_TABLE_NAMES: tuple[str, ...] = (
    "kpi_ab0_daily",
    "kpi_ab30_daily",
    "kpi_ab90_daily",
    "kpi_arpu_daily",
    "kpi_revenue_daily",
    "kpi_receipts_daily",
    "kpi_revenue_active_month",
    "kpi_revenue_active_week",
    "kpi_revenue_active_quarter",
    "kpi_revenue_active_year",
    "kpi_inflow_daily",
    "kpi_outflow_daily",
    "dim_tariff",
    "dim_segment",
)

_DOC_MD = __doc__ or ""

with DAG(
    dag_id="telecom_mart_clickhouse_daily",
    description="MART (Iceberg/Trino) → ClickHouse serving, полные таблицы, ежедневно",
    doc_md=_DOC_MD,
    schedule="30 7 * * *",
    start_date=START,
    catchup=False,
    max_active_runs=1,
    tags=["telecom-mart", "mart", "clickhouse", "serving"],
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
    },
) as dag:
    for name in SERVING_TABLE_NAMES:
        BashOperator(
            task_id=f"serving__{name}",
            bash_command=(
                "python3 /opt/airflow/scripts/mart_serving_clickhouse.py "
                f"--config {RAW_CONFIG} "
                "--full-table "
                f"--table {name}"
            ),
        )
