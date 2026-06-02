"""DDS: public.tariff_sales → dds_tariff_sales_snapshot (full).

Назначение
----------
Тонкая обёртка над ``dds_table_runner.cli_for_table`` для таблицы
``public.tariff_sales`` — цены/стоимости тарифов (исторические).

Режим загрузки: **full** (snapshot)
- Источник: RAW-снимок ``raw.public__tariff_sales__snapshot``.
- Полное пересоздание из последнего снимка на ``cutoff_day``.

Целевая таблица DDS: ``iceberg.dds.dds_tariff_sales_snapshot``.
При SCD2: ``dds_tariff_sales_snapshot_scd2`` + VIEW.

Бизнес-смысл
------------
Ценовые справочники тарифов — стоимость для разных категорий клиентов,
периодов действия и валют. Источник для расчёта выручки (revenue)
и анализа ценовой политики.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dds_table_runner import cli_for_table

if __name__ == "__main__":
    cli_for_table("public.tariff_sales")
