"""Точка входа Airflow: витрина kpi_ab90_daily — активные за последние 90 дней.

АБ90 считает уникальных абонентов, у которых заказ был активен хотя бы один день
в окне [report_date−89, report_date]. Учитываются статусы ENABLED и EXPIRED.

Расчёт рекуррентный с 90-го дня витрины:
  АБ90(D) = АБ90(D−1) + new_clients(D) − churned_clients(D−89).
До накопления 90 дней — честный COUNT DISTINCT снимок окна.

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
    main(implicit_default_vitrina="ab90")
