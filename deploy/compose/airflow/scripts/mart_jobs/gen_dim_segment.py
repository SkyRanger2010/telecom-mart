"""Точка входа Airflow: справочник dim_segment — направления деятельности.

Извлекает иерархию сегментов из dds_segments_snapshot (JSON payload).
Содержит segment_id, parent_id, title — используется дашбордами
для фильтрации и группировки KPI по направлениям бизнеса.
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
    main(implicit_default_vitrina="dim_segment")
