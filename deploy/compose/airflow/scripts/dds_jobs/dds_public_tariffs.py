"""DDS: public.tariffs → dds_tariffs_snapshot (full).

Назначение
----------
Тонкая обёртка над ``dds_table_runner.cli_for_table`` для таблицы
``public.tariffs`` — справочник тарифов.

Режим загрузки: **full** (snapshot)
- Источник: RAW-снимок ``raw.public__tariffs__snapshot``.
- Полное пересоздание из последнего снимка на ``cutoff_day``.

Целевая таблица DDS: ``iceberg.dds.dds_tariffs_snapshot``.
При SCD2: ``dds_tariffs_snapshot_scd2`` + VIEW.

Бизнес-смысл
------------
Тарифы — основные продукты компании: наименование, тип (интернет/ТВ/телефония),
скорость, объём трафика и т.п. Центральный справочник для:
- витрин KPI по продуктам (gen_dim_tariff)
- анализа выручки в разрезе тарифов
- продуктовой аналитики и A/B-тестирования
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dds_table_runner import cli_for_table

if __name__ == "__main__":
    cli_for_table("public.tariffs")
