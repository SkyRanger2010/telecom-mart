"""DDS: public.account_transactions → dds_account_transactions (incremental).

Назначение
----------
Тонкая обёртка над ``dds_table_runner.cli_for_table`` для таблицы
``public.account_transactions`` — проводки по лицевым счетам.

Режим загрузки: **incremental**
- Источник: RAW-зеркало ``raw.account_transactions`` (Postgres state).
- Ежедневная дельта: MERGE + DELETE из ``raw.public__account_transactions__events``
  по ``changed_at`` в суточном окне.
- При первом запуске или ``--dds-full-rebuild``: полное пересоздание из зеркала.

Целевая таблица DDS: ``iceberg.dds.dds_account_transactions``.
При включённом SCD2: ``iceberg.dds.dds_account_transactions_scd2``
+ VIEW ``iceberg.dds.dds_account_transactions`` (``is_current = TRUE``).

Бизнес-смысл
------------
Финансовые транзакции по счетам абонентов — пополнения, списания, начисления.
Ключевой источник для витрин KPI: revenue, receipts, ARPU.

Использование
-------------
В Airflow DAG::

    load_dds_account_transactions = PythonOperator(
        task_id="dds_account_transactions",
        python_callable="dds_jobs.dds_public_account_transactions.main",  # или cli_for_table
    )
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dds_table_runner import cli_for_table

if __name__ == "__main__":
    cli_for_table("public.account_transactions")
