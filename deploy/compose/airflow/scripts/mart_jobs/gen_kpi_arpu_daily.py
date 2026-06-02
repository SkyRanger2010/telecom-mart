"""Точка входа Airflow: витрина kpi_arpu_daily — средняя выручка на абонента.

Формула: arpu_daily = total_revenue / active_clients (АБ0 на дату).
arpu_monthly = arpu_daily × дней в месяце report_date.

Окно выручки:
- Если report_date = последний день месяца → с 1-го числа этого месяца.
- Иначе → скользящее окно длиной N дней назад (N = дней в месяце).

⚠ ARPU не суммируется при агрегации по периоду!
Правильно: SUM(total_revenue) / SUM(active_clients), а не SUM(arpu_daily).

Зависит от: kpi_revenue_daily + kpi_ab0_daily.

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
    main(implicit_default_vitrina="arpu")
