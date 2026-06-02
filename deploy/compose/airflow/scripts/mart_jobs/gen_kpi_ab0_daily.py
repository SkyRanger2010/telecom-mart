"""Точка входа Airflow: витрина kpi_ab0_daily — активные абоненты на дату.

Прямой COUNT(DISTINCT subscriber_id) — все уникальные клиенты, у которых
на отчётную дату есть хотя бы один активный заказ:
- в статусе ENABLED,
- активирован (activated IS NOT NULL),
- срок не истёк (expire_time IS NULL или >= report_date),
- не расходный (expenditure=false), не черновик.

НЕ использует рекуррентную формулу (в отличие от АБ30/АБ90).
Это «честный» снимок активной базы на конец дня, включающий и старых,
и новых абонентов.

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
