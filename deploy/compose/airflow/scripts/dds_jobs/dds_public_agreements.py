"""DDS: public.agreements → dds_agreements (incremental).

Назначение
----------
Тонкая обёртка над ``dds_table_runner.cli_for_table`` для таблицы
``public.agreements`` — договоры с абонентами.

Режим загрузки: **incremental**
- Источник: RAW-зеркало ``raw.agreements``.
- Ежедневная дельта из ``raw.public__agreements__events``.

Целевая таблица DDS: ``iceberg.dds.dds_agreements``.
При SCD2: ``dds_agreements_scd2`` + VIEW.

Бизнес-смысл
------------
Договор — юридическое основание для оказания услуг.
Связывает клиента, тариф, лицевой счёт. Ключевая сущность для:
- сегментации клиентов
- оттока (churn) — дата закрытия договора
- подключения/отключения услуг
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dds_table_runner import cli_for_table

if __name__ == "__main__":
    cli_for_table("public.agreements")
