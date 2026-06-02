-- =============================================================================
-- VALIDATE — проверки партии RAW за календарный день процесса.
-- =============================================================================
--
-- Назначение:
--   Верификация загрузки RAW-слоя за один день: сверка объёмов между манифестом
--   (raw_load_manifest) и фактическими данными в таблицах событий.
--
-- Когда запускать:
--   После дневной загрузки RAW (load_raw_day.py / load_raw_period_batch.py),
--   ДО обновления DDS / SCD2.
--
-- Именование таблиц:
--   События: schema__table__events (например public__orders__events).
--   Снимки:  schema__table__snapshot.
--
-- Настройка:
--   Подставьте process_date и границы окна так же, как в load_raw (--tz / UTC).
--   Для автоматизации — запускайте из Airflow с параметризацией даты.
--
-- Автоматические резюме:
--   dq_jobs/dq_summarize_raw_manifest.py — агрегация манифеста за период.
--   dq_jobs/dq_summarize_dds_load_log.py — агрегация лога DDS-загрузок.
--   → iceberg.validate.dq_daily_summary (+ логи задач Airflow).
-- =============================================================================

-- Схема для объектов валидации (если ещё не создана).
CREATE SCHEMA IF NOT EXISTS iceberg.validate;

-- ---------------------------------------------------------------------------
-- CTE 1: proc — дата проверяемой партии.
-- ЗАМЕНИТЕ дату на актуальную при каждом запуске.
-- ---------------------------------------------------------------------------
WITH proc AS (
    SELECT DATE '2024-06-02' AS process_date -- <<< дата партии
),

-- ---------------------------------------------------------------------------
-- CTE 2: bounds — UTC-границы окна загрузки.
-- Должны совпадать с параметрами --day и --tz при вызове load_raw.
-- Здесь: 2024-06-02 в UTC (полночь→полночь).
-- ---------------------------------------------------------------------------
bounds AS (
    SELECT
        process_date,
        FROM_ISO8601_TIMESTAMP('2024-06-02T00:00:00Z') AS day_start_utc,
        FROM_ISO8601_TIMESTAMP('2024-06-03T00:00:00Z') AS day_end_exclusive_utc
    FROM proc
),

-- ---------------------------------------------------------------------------
-- CTE 3: manifest_check — читает манифест за process_date.
-- Извлекает source_table и rows_read для последующей сверки.
-- ---------------------------------------------------------------------------
manifest_check AS (
    SELECT m.source_table,
           CAST(m.rows_read AS BIGINT) AS manifest_rows_read
    FROM iceberg.raw.raw_load_manifest m
    CROSS JOIN bounds b
    WHERE m.load_day = b.process_date
),

-- ---------------------------------------------------------------------------
-- CTE 4: orders_events_daily — фактические события orders за окно.
-- Считает строки в public__orders__events, попавшие в UTC-окно дня.
-- Добавьте аналогичные CTE для других таблиц при необходимости.
-- ---------------------------------------------------------------------------
orders_events_daily AS (
    SELECT count(*) AS cnt
    FROM iceberg.raw.public__orders__events e
    CROSS JOIN bounds b
    WHERE e.changed_at >= b.day_start_utc
      AND e.changed_at < b.day_end_exclusive_utc
)

-- ===========================================================================
-- Финальный SELECT: два ряда с метриками.
-- ===========================================================================

-- Метрика 1: число строк в манифесте для public.orders.
-- Сравните с фактическим числом событий (следующая метрика).
-- Значительное расхождение → потеря событий или двойная загрузка.
SELECT 'raw_manifest_rows' AS metric,
       CAST(SUM(manifest_rows_read) AS DOUBLE) AS value
FROM manifest_check WHERE source_table = 'public.orders'

UNION ALL

-- Метрика 2: фактическое число событий orders в окне дня.
-- Должно быть близко к manifest_rows_read (допустимо небольшое расхождение
-- из-за дедупликации по log_id при MERGE в events).
SELECT 'orders_events_in_window' AS metric,
       CAST((SELECT cnt FROM orders_events_daily) AS DOUBLE) AS value;
