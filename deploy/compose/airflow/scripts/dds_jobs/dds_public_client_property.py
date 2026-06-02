"""DDS: public.client_property → dds_client_property_snapshot (full).

Назначение
----------
Тонкая обёртка над ``dds_table_runner.cli_for_table`` для таблицы
``public.client_property`` — дополнительные атрибуты клиентов.

Режим загрузки: **full** (snapshot)
- Источник: RAW-снимок ``raw.public__client_property__snapshot``.
- Полное пересоздание целевой таблицы из последнего снимка на ``cutoff_day``.
- Выборка: ``row_number()`` по ``(source_pkey, snapshot_day DESC, extracted_at DESC)``,
  ``WHERE rn = 1``.

Целевая таблица DDS: ``iceberg.dds.dds_client_property_snapshot``.
При SCD2: ``dds_client_property_snapshot_scd2`` + VIEW.

Бизнес-смысл
------------
Расширенные свойства клиентов (JSON/KV), не входящие в основную таблицу clients.
Используется для сегментации и таргетирования: тип клиента, отрасль, признаки VIP и т.п.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dds_table_runner import cli_for_table

if __name__ == "__main__":
    cli_for_table("public.client_property")
