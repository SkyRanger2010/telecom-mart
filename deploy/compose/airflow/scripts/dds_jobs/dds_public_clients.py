"""DDS: public.clients → dds_clients (incremental).

Назначение
----------
Тонкая обёртка над ``dds_table_runner.cli_for_table`` для таблицы
``public.clients`` — клиенты (контрагенты) биллинга.

Режим загрузки: **incremental**
- Источник: RAW-зеркало ``raw.clients``.
- Ежедневная дельта из ``raw.public__clients__events``.
- MERGE затронутых строк + DELETE по ``op=DELETE``.

Целевая таблица DDS: ``iceberg.dds.dds_clients``.
При SCD2: ``dds_clients_scd2`` + VIEW.

Бизнес-смысл
------------
Клиенты — физические и юридические лица, которым оказываются услуги.
Основа для:
- клиентской аналитики и RFM-сегментации
- витрин KPI по клиентской базе
- трекинга жизненного цикла (onboarding → active → churn)
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dds_table_runner import cli_for_table

if __name__ == "__main__":
    cli_for_table("public.clients")
