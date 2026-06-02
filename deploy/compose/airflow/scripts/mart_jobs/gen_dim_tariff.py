"""Точка входа Airflow: справочник dim_tariff — тарифы из DDS.

Извлекает tariff_id → tariff_title из dds_tariffs_snapshot.
Используется всеми KPI-витринами для подписи тарифов (колонка tariff_title).
Всегда полная выгрузка (не зависит от отчётной даты).

CLI: --config raw_load_config.json
"""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from mart_vitrina_runner import main

if __name__ == "__main__":
    main(implicit_default_vitrina="dim_tariff")
