"""DDS: public.tariff2option → dds_tariff2option_snapshot (full).

Назначение
----------
Тонкая обёртка над ``dds_table_runner.cli_for_table`` для таблицы
``public.tariff2option`` — связка «тариф → опции» (many-to-many).

Режим загрузки: **full** (snapshot)
- Источник: RAW-снимок ``raw.public__tariff2option__snapshot``.
- Полное пересоздание из последнего снимка на ``cutoff_day``.

Целевая таблица DDS: ``iceberg.dds.dds_tariff2option_snapshot``.
При SCD2: ``dds_tariff2option_snapshot_scd2`` + VIEW.

Бизнес-смысл
------------
Связка тарифов с дополнительными опциями (например, тариф «Базовый» +
опция «Статический IP»). Справочник для витрин продуктовой аналитики
и расчёта стоимости услуг.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dds_table_runner import cli_for_table

if __name__ == "__main__":
    cli_for_table("public.tariff2option")
