"""Точка входа Airflow: витрина kpi_revenue_daily — дневная выручка.

Считает сумму начислений (credits) из dds_ledgers, нормализованную по дням:
каждая проводка биллинга делится на число дней в её интервале (daily_share).
Например, месячный тариф 3000₽ в феврале (28 дней) → 3000/28 ≈ 107₽/день.

Фильтр: credits > 0, заказ ENABLED и активен на отчётную дату.
Также считает paying_owners — число активных платящих абонентов.

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
    main(implicit_default_vitrina="revenue")
