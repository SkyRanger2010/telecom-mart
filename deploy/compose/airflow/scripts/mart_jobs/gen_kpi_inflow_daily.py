"""Точка входа Airflow: витрина kpi_inflow_daily — приток новых абонентов.

Считает уникальных абонентов, договоры и заказы, ВПЕРВЫЕ активированные
в отчётную дату (first_client_activation = report_date).

Фильтр: activated = report_date, expenditure=false, is_draft=false.
Гранулярность: день × сегмент × вид услуги × тариф × тип клиента.

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
    main(implicit_default_vitrina="inflow")
