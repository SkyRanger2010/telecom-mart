"""Точка входа Airflow: витрина kpi_revenue_active_year — выручка за календарный год.

Агрегирует kpi_revenue_daily за период с 1 января по 31 декабря.
period_end = 31 декабря (только в эту дату запускается).

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
    main(implicit_default_vitrina="revenue_active_year")
