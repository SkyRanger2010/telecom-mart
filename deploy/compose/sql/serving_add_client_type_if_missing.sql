-- Тип клиента (person / ip / org / unknown) для дашборда «Абонентская база».
-- Согласовано с mart_serving_clickhouse / vitrina.serving_flow_columns (Nullable(String)).
-- База по умолчанию serving; при другой БД замените префикс.

ALTER TABLE serving.kpi_ab0_daily ADD COLUMN IF NOT EXISTS client_type Nullable(String);
ALTER TABLE serving.kpi_ab30_daily ADD COLUMN IF NOT EXISTS client_type Nullable(String);
ALTER TABLE serving.kpi_ab90_daily ADD COLUMN IF NOT EXISTS client_type Nullable(String);
ALTER TABLE serving.kpi_inflow_daily ADD COLUMN IF NOT EXISTS client_type Nullable(String);
ALTER TABLE serving.kpi_outflow_daily ADD COLUMN IF NOT EXISTS client_type Nullable(String);
