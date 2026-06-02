"""Точка входа Airflow: витрина kpi_ab90_daily — активные за последние 90 дней.

Прямой COUNT(DISTINCT subscriber_id) — уникальные абоненты, у которых
за последние 90 дней (включая отчётную дату) был хотя бы один день
с активным заказом. Учитываются статусы ENABLED и EXPIRED.

Окно: [report_date − 89, report_date] включительно.
НЕ использует рекуррентную формулу — честный снимок окна.

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
