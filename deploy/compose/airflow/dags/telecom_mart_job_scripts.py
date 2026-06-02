"""Список CLI-скриптов слоя MART для Airflow (без побочных эффектов при импорте).

Порядок важен: ``gen_dim_tariff`` → приток/отток → АБ0/30/90 (АБ из inflow/outflow);
в ``telecom_kpi_daily`` для АБ заданы зависимости от inflow/outflow; в backfill — порядок списка.
``gen_kpi_arpu_daily`` перед ``gen_kpi_revenue_*`` (выручка из ARPU).
Синхронизируйте с ``mart_runner_common.VITRINA_KEYS`` / ``mart_jobs`` при добавлении витрин.
"""

from __future__ import annotations

MART_JOB_SCRIPTS: tuple[str, ...] = (
    "gen_dim_segment.py",
    "gen_dim_tariff.py",
    "gen_kpi_inflow_daily.py",
    "gen_kpi_outflow_daily.py",
    "gen_kpi_ab0_daily.py",
    "gen_kpi_arpu_daily.py",
    "gen_kpi_revenue_daily.py",
    "gen_kpi_receipts_daily.py",
    "gen_kpi_revenue_active_month.py",
    "gen_kpi_revenue_active_week.py",
    "gen_kpi_revenue_active_quarter.py",
    "gen_kpi_revenue_active_year.py",
)

# Зависимости DAG: АБ-витрины читают kpi_inflow_daily / kpi_outflow_daily за тот же день (и лаг оттока).
MART_AB_JOB_SCRIPTS: tuple[str, ...] = (
    "gen_kpi_ab0_daily.py",
)
MART_FLOW_JOB_SCRIPTS: tuple[str, ...] = (
    "gen_kpi_inflow_daily.py",
    "gen_kpi_outflow_daily.py",
)

# Ключи для DAG ``telecom_mart_vitrina_backfill`` (--vitrina / conf vitrina). Синхронизировать с mart_runner_common.VITRINA_KEYS.
MART_VITRINA_KEYS: tuple[str, ...] = (
    "dim_segment",
    "dim_tariff",
    "inflow",
    "outflow",
    "ab0",
    "ab30",
    "ab90",
    "arpu",
    "revenue",
    "receipts",
    "revenue_active_month",
    "revenue_active_week",
    "revenue_active_quarter",
    "revenue_active_year",
)
