-- =============================================================================
-- DDS — шаблон применения изменений за день со SCD2 по бизнес-ключу (Type 2).
-- =============================================================================
--
-- Вход: iceberg.raw.<schema>__<table>__events
--   Колонки: new_values (JSON), op (INSERT/UPDATE/DELETE), changed_at, source_table,
--   source_pkey, ...
--
-- Выход: iceberg.dds.<entity>_scd2
--   Колонки: bk_<key> (бизнес-ключ), surrogate_key, attr_hash,
--   valid_from, valid_to, is_current, payload_json, row_op, lineage_load_day.
--
-- Порядок в дневном DAG:
--   1) LOAD RAW (за сутки) — заполнение raw.*__events.
--   2) VALIDATE (validate_raw_daily.sql) — проверка свежих событий.
--   3) STAGING — развернуть события за окно [day_start, day_end) в строки:
--      id, attrs..., op, changed_at.
--   4) MERGE SCD2:
--      для UPDATE — закрыть предыдущую is_current-версию
--        (valid_to = day_end - 1μs, is_current = FALSE),
--        вставить новую (valid_from = day_end, is_current = TRUE);
--      для INSERT — вставить первую версию;
--      для DELETE — закрыть текущую версию без вставки новой.
--
-- Ниже — каркас Iceberg DDL; mapping JSON зависит от полей в global_log.
-- Конкретные реализации SCD2 для каждой таблицы генерируются в dds_scd2.py
-- на основе конфига и структуры Postgres-источника.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Схема DDS (если ещё не создана)
-- ---------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS iceberg.dds;

-- ---------------------------------------------------------------------------
-- CTE 1: Физическая SCD2-таблица (пример для заказов)
-- ---------------------------------------------------------------------------
-- bk_order_id      — бизнес-ключ (PK источника).
-- surrogate_key     — суррогатный ключ строки SCD2 (уникальный ID версии).
-- attr_hash         — хеш атрибутов (VARBINARY xxhash64), для сравнения версий.
-- valid_from        — начало действия версии (UTC).
-- valid_to          — конец действия версии (NULL = открыта).
-- is_current        — TRUE для актуальной версии.
-- payload_json      — полный JSON-слепок строки для отладки/аудита.
-- row_op            — метка операции (BOOTSTRAP / UPSERT / CLOSE).
-- lineage_load_day  — дата загрузки (для инкрементальной выборки).
--
-- Партиционирование: bucket(bk_order_id, 32) для равномерного распределения.
CREATE TABLE IF NOT EXISTS iceberg.dds.orders_scd2 (
    bk_order_id BIGINT,
    surrogate_key BIGINT,
    attr_hash VARBINARY,
    valid_from TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    valid_to TIMESTAMP(6) WITH TIME ZONE,
    is_current BOOLEAN NOT NULL,
    payload_json VARCHAR,
    row_op VARCHAR,
    lineage_load_day DATE
)
WITH (
    partitioning = ARRAY['bucket(bk_order_id, 32)']
);

-- ---------------------------------------------------------------------------
-- CTE 2: Staging — разворот событий из *__events в строки
-- ---------------------------------------------------------------------------
-- Из JSON-поля new_values извлекаются бизнес-атрибуты через json_extract_scalar.
-- Фильтр по суточному окну: changed_at ∈ [day_start, day_end).
-- Пример для таблицы orders (поля зависят от конкретной схемы источника):
--
-- INSERT INTO iceberg.dds.orders_scd2_stage ...
-- SELECT
--   CAST(json_extract_scalar(
--     CAST(JSON_PARSE(COALESCE(e.new_values, '{}')) AS JSON), '$.id'
--   ) AS BIGINT) AS bk_order_id,
--   e.changed_at,
--   upper(e.op) AS op,
--   e.new_values AS payload_json,
--   ...
-- FROM iceberg.raw.public__orders__events e
-- WHERE e.changed_at >= :day_start
--   AND e.changed_at < :day_end;

-- ---------------------------------------------------------------------------
-- CTE 3: Логическое VIEW — текущее зеркало для витрин и KPI
-- ---------------------------------------------------------------------------
-- Совместимость с KPI: после ночного MERGE поддерживается текущее зеркало
-- dds_orders из последней версии SCD2 (is_current = TRUE).
-- Альтернативно: продолжать обновление плоского зеркала отдельным шагом
-- и использовать SCD2 как вторичный журнал аудита.
--
-- CREATE OR REPLACE VIEW iceberg.dds.orders AS
-- SELECT bk_order_id, ... business columns ...
-- FROM iceberg.dds.orders_scd2
-- WHERE is_current = TRUE;

-- ---------------------------------------------------------------------------
-- CTE 4: Реконструкция на дату (point-in-time)
-- ---------------------------------------------------------------------------
-- Для аналитики «как было на дату D» можно использовать valid_from/valid_to:
--
-- SELECT ...
-- FROM iceberg.dds.orders_scd2
-- WHERE valid_from <= :point_in_time
--   AND (valid_to IS NULL OR valid_to > :point_in_time);
