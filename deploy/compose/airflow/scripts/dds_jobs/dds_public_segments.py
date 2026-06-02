"""DDS: public.segments → dds_segments_snapshot (full).

Назначение
----------
Тонкая обёртка над ``dds_table_runner.cli_for_table`` для таблицы
``public.segments`` — справочник клиентских сегментов.

Режим загрузки: **full** (snapshot)
- Источник: RAW-снимок ``raw.public__segments__snapshot``.
- Полное пересоздание из последнего снимка на ``cutoff_day``.

Целевая таблица DDS: ``iceberg.dds.dds_segments_snapshot``.
При SCD2: ``dds_segments_snapshot_scd2`` + VIEW.

Бизнес-смысл
------------
Сегменты — маркетинговые группы клиентов (B2B/B2C, крупный/средний/малый бизнес,
география и т.п.). Справочник для:
- фильтрации витрин KPI по сегментам
- таргетированных маркетинговых кампаний
- анализа выручки и оттока в разрезе сегментов
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dds_table_runner import cli_for_table

if __name__ == "__main__":
    cli_for_table("public.segments")
