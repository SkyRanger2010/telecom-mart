"""DDS: public.orders → dds_orders (incremental).

Назначение
----------
Тонкая обёртка над ``dds_table_runner.cli_for_table`` для таблицы
``public.orders`` — заказы (подключённые услуги) абонентов.

Режим загрузки: **incremental**
- Источник: RAW-зеркало ``raw.orders``.
- Ежедневная дельта из ``raw.public__orders__events``.
- MERGE затронутых строк + DELETE по ``op=DELETE``.

Целевая таблица DDS: ``iceberg.dds.dds_orders``.
При SCD2: ``dds_orders_scd2`` + VIEW.

Бизнес-смысл
------------
Заказы — центральная сущность биллинга: активные услуги на абоненте.
Каждый заказ привязан к договору (agreement) и тарифу/пакету.
Основа для витрин KPI:
- revenue (выручка по услугам)
- ab0/ab30/ab90 (активная база)
- inflow/outflow (подключения/отключения)
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dds_table_runner import cli_for_table

if __name__ == "__main__":
    cli_for_table("public.orders")
