"""Точка входа Airflow: витрина kpi_receipts_daily — кассовые поступления.

В отличие от revenue, считает сумму credits БЕЗ нормализации по дневной доле.
Берётся напрямую из dds_ledgers.period — это «кассовый» показатель:
сколько фактически начислено в периоде проводки.

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
    main(implicit_default_vitrina="receipts")
