Файлы Trino SQL (один файл на исходную таблицу, точка заменена на два подчёркивания):
  airflow/scripts/dds_validate/public__orders.sql
  airflow/scripts/dds_clean/public__orders.sql

Несколько операторов — разделить точкой с запятой. Строки, начинающиеся с -- после split игнорируются только если вся строка — комментарий (простые проверки).

Результат DQ пишется в dds.dds_table_load_log (dq_status/dq_note). При отсутствии sidecar-SQL статус dq_status около OK.
