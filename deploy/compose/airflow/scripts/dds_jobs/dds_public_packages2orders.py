"""DDS: public.packages2orders → dds_packages2orders (incremental).

Назначение
----------
Тонкая обёртка над ``dds_table_runner.cli_for_table`` для таблицы
``public.packages2orders`` — связка «пакет → заказы» (many-to-many).

Режим загрузки: **incremental**
- Источник: RAW-зеркало ``raw.packages2orders``.
- Ежедневная дельта из ``raw.public__packages2orders__events``.

Целевая таблица DDS: ``iceberg.dds.dds_packages2orders``.
При SCD2: ``dds_packages2orders_scd2`` + VIEW.

Бизнес-смысл
------------
Связующая таблица между пакетами и заказами: один пакет может включать
несколько заказов (услуг), один заказ может входить в несколько пакетов.
Используется для построения иерархии продуктов и анализа пакетных продаж.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dds_table_runner import cli_for_table

if __name__ == "__main__":
    cli_for_table("public.packages2orders")
