Отдельная группа задач «оценка качества / объёмов» после загрузок.

dq_common.py — общая таблица сводок dq_daily_summary и commit для Trino-драйвера.

Скрипты:
  dq_summarize_raw_manifest.py — после RAW: читает iceberg.<raw_schema>.raw_load_manifest,
    пишет iceberg.<validate_schema>.dq_daily_summary (phase=raw_manifest), в лог — резюме
    и построчно по таблицам (rows_read, events, snapshot, invalid, status).
  dq_summarize_dds_load_log.py — после DDS: агрегирует iceberg.<dds_schema>.dds_table_load_log,
    та же dq_daily_summary (phase=dds_load_log), детальный лог по dq_status/dq_note.

Конфиг схем: raw_load_config.json → pipeline.catalog, raw_schema, validate_schema, dds_schema.

Дополнительные SQL-проверки можно вешать отдельными задачами в том же TaskGroup или вызывать
после сводки; эталон ручного SQL — ../validate_raw_daily.sql (параметризовать дату).
