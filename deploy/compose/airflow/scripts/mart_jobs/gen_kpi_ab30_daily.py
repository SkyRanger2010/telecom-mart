"""Точка входа Airflow: витрина kpi_ab30_daily — активные за последние 30 дней.

АБ30 считает уникальных абонентов, у которых заказ был активен хотя бы один день
в окне [report_date−29, report_date]. Учитываются статусы ENABLED и EXPIRED
(заказ мог истечь внутри окна, но был активен в его пределах).

Расчёт рекуррентный с 30-го дня витрины:
  АБ30(D) = АБ30(D−1) + new_clients(D) − churned_clients(D−29).
До накопления 30 дней — честный COUNT DISTINCT снимок окна.

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
    main(implicit_default_vitrina="ab30")
