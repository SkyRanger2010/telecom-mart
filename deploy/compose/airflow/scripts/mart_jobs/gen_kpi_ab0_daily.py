"""Точка входа Airflow: витрины kpi_ab0/ab30/ab90_daily — все три за один проход.

При вызове gen_kpi_ab0_daily.py (vitrina_key=ab0) считаются сразу три витрины:
- AB0: уникальные клиенты с активным заказом на отчётную дату
- AB30: уникальные клиенты с хотя бы одним днём активности в [D−29, D]
- AB90: то же для окна [D−89, D]

Все три — прямые COUNT(DISTINCT subscriber_id) из DDS.
gen_kpi_ab30 и gen_kpi_ab90 — no-op (уже посчитаны в ab0).

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
    main(implicit_default_vitrina="ab0")
