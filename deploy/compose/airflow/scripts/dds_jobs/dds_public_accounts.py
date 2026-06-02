"""DDS: public.accounts → dds_accounts (incremental).

Назначение
----------
Тонкая обёртка над ``dds_table_runner.cli_for_table`` для таблицы
``public.accounts`` — лицевые счета абонентов.

Режим загрузки: **incremental**
- Источник: RAW-зеркало ``raw.accounts``.
- Ежедневная дельта из ``raw.public__accounts__events``.
- MERGE затронутых строк + DELETE по ``op=DELETE``.

Целевая таблица DDS: ``iceberg.dds.dds_accounts``.
При SCD2: физическая ``dds_accounts_scd2`` + VIEW ``dds_accounts``.

Бизнес-смысл
------------
Лицевые счета — основной объект биллинга: баланс, статус, тип счёта.
Связь с клиентами через ``client_id``, с договорами через ``agreement_id``.
Основа для витрин KPI по балансам и оборотам.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dds_table_runner import cli_for_table

if __name__ == "__main__":
    cli_for_table("public.accounts")
