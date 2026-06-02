"""DDS: public.accounts_history → dds_accounts_history (incremental).

Назначение
----------
Тонкая обёртка над ``dds_table_runner.cli_for_table`` для таблицы
``public.accounts_history`` — история изменений лицевых счетов.

Режим загрузки: **incremental**
- Источник: RAW-зеркало ``raw.accounts_history``.
- Ежедневная дельта из ``raw.public__accounts_history__events``.

Целевая таблица DDS: ``iceberg.dds.dds_accounts_history``.
При SCD2: ``dds_accounts_history_scd2`` + VIEW.

Бизнес-смысл
------------
Лог изменений по лицевым счетам — кто, когда и что менял.
Используется для аудита биллинговых операций и восстановления хронологии.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dds_table_runner import cli_for_table

if __name__ == "__main__":
    cli_for_table("public.accounts_history")
