-- Таблица результата дашборда «по запросу» (имя фиксировано: dashboard_query_stub).
-- Пересоздаётся пайплайном: llm-api → sqlguard (materialize_sql) → dashbot → SELECT для UI.

CREATE TABLE IF NOT EXISTS serving.dashboard_query_stub (
    report_date Date,
    segment_label String,
    metric_value Float64,
    metric_kind String
) ENGINE = MergeTree()
ORDER BY (report_date, segment_label);
