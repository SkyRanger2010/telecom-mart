"""DDS: public.tariff2pack → dds_tariff2pack_snapshot (full).

Назначение
----------
Тонкая обёртка над ``dds_table_runner.cli_for_table`` для таблицы
``public.tariff2pack`` — связка «тариф → пакет» (many-to-many).

Режим загрузки: **full** (snapshot)
- Источник: RAW-снимок ``raw.public__tariff2pack__snapshot``.
- Полное пересоздание из последнего снимка на ``cutoff_day``.

Целевая таблица DDS: ``iceberg.dds.dds_tariff2pack_snapshot``.
При SCD2: ``dds_tariff2pack_snapshot_scd2`` + VIEW.

Бизнес-смысл
------------
Связка тарифов с пакетами: определяет, какие тарифы входят в состав пакетных
предложений. Используется для анализа пакетных продаж и построения
продуктовой иерархии.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dds_table_runner import cli_for_table

if __name__ == "__main__":
    cli_for_table("public.tariff2pack")
