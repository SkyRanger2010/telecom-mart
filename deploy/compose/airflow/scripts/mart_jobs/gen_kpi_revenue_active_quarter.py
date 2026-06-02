"""Точка входа Airflow: витрина kpi_revenue_active_quarter — выручка за квартал.

Агрегирует kpi_revenue_daily за календарный квартал (Q1: янв-мар, Q2: апр-июн, …).
period_end = последний день квартала (только в эти даты запускается).

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
    main(implicit_default_vitrina="revenue_active_quarter")
