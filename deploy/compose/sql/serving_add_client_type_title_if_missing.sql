-- Подпись типа клиента (из mart: join client_property.title); Nullable(String) как в mart_serving_clickhouse.
-- База по умолчанию serving.

ALTER TABLE serving.kpi_ab0_daily ADD COLUMN IF NOT EXISTS client_type_title Nullable(String);
ALTER TABLE serving.kpi_ab30_daily ADD COLUMN IF NOT EXISTS client_type_title Nullable(String);
ALTER TABLE serving.kpi_ab90_daily ADD COLUMN IF NOT EXISTS client_type_title Nullable(String);
ALTER TABLE serving.kpi_inflow_daily ADD COLUMN IF NOT EXISTS client_type_title Nullable(String);
ALTER TABLE serving.kpi_outflow_daily ADD COLUMN IF NOT EXISTS client_type_title Nullable(String);
