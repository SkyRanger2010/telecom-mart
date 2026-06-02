"""DDS: public.ledgers → dds_ledgers (incremental).

Назначение
----------
Тонкая обёртка над ``dds_table_runner.cli_for_table`` для таблицы
``public.ledgers`` — регистры учёта (бухгалтерские проводки).

Режим загрузки: **incremental**
- Источник: RAW-зеркало ``raw.ledgers``.
- Ежедневная дельта из ``raw.public__ledgers__events``.

Целевая таблица DDS: ``iceberg.dds.dds_ledgers``.
При SCD2: ``dds_ledgers_scd2`` + VIEW.

Бизнес-смысл
------------
Регистры бухгалтерского учёта — агрегированные проводки по счетам,
периодам и статьям. Источник для финансовой отчётности и сверки
с account_transactions.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dds_table_runner import cli_for_table

if __name__ == "__main__":
    cli_for_table("public.ledgers")
