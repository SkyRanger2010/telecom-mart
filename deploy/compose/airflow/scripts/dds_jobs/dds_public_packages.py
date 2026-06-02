"""DDS: public.packages → dds_packages_snapshot (full).

Назначение
----------
Тонкая обёртка над ``dds_table_runner.cli_for_table`` для таблицы
``public.packages`` — справочник пакетов услуг.

Режим загрузки: **full** (snapshot)
- Источник: RAW-снимок ``raw.public__packages__snapshot``.
- Полное пересоздание из последнего снимка на ``cutoff_day``.

Целевая таблица DDS: ``iceberg.dds.dds_packages_snapshot``.
При SCD2: ``dds_packages_snapshot_scd2`` + VIEW.

Бизнес-смысл
------------
Пакеты — коммерческие предложения, группирующие несколько тарифов/услуг
в одно предложение (например «Интернет + ТВ»). Справочник для витрин
продаж и анализа продуктового портфеля.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dds_table_runner import cli_for_table

if __name__ == "__main__":
    cli_for_table("public.packages")
