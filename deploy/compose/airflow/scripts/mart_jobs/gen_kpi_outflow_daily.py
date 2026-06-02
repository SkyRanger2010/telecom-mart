"""Точка входа Airflow: витрина kpi_outflow_daily — отток абонентов.

Считает уникальных абонентов, договоры и заказы, прекратившие действие в отчётную дату.
Условия: expire_time = report_date ИЛИ статус изменился на EXPIRED/DISABLED.
Фильтр: expenditure=false, is_draft=false.

Гранулярность: день × сегмент × вид услуги × тариф × тип клиента.
Используется для рекуррентного расчёта АБ0/АБ30/АБ90.

CLI: --config raw_load_config.json --report-date YYYY-MM-DD
"""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from mart_vitrina_runner import main

if __name__ == "__main__":
    main(implicit_default_vitrina="outflow")
