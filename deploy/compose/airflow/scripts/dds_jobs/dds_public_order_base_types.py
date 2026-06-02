"""DDS: public.order_base_types → dds_order_base_types_snapshot (full).

Назначение
----------
Тонкая обёртка над ``dds_table_runner.cli_for_table`` для таблицы
``public.order_base_types`` — справочник видов услуг (человекочитаемые группы).

Режим загрузки: **full** (snapshot)
- Источник: RAW-снимок ``raw.public__order_base_types__snapshot``.
- Полное пересоздание целевой таблицы из последнего снимка на ``cutoff_day``.

Целевая таблица DDS: ``iceberg.dds.dds_order_base_types_snapshot``.
При SCD2: ``dds_order_base_types_snapshot_scd2`` + VIEW.

Бизнес-смысл
------------
Справочник группировки услуг: интернет, телефония, ТВ, хостинг, аренда каналов и т.п.
Используется для категоризации заказов (orders) в витринах и отчётах.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dds_table_runner import cli_for_table

if __name__ == "__main__":
    cli_for_table("public.order_base_types")
