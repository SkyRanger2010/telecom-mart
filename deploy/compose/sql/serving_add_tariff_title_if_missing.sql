-- Название тарифа в serving: как в mart_serving_clickhouse (KPI — Nullable(String), dim_tariff — String).
-- База по умолчанию serving; при другой БД замените префикс.

ALTER TABLE serving.kpi_ab0_daily ADD COLUMN IF NOT EXISTS tariff_title Nullable(String);
ALTER TABLE serving.kpi_ab30_daily ADD COLUMN IF NOT EXISTS tariff_title Nullable(String);
ALTER TABLE serving.kpi_ab90_daily ADD COLUMN IF NOT EXISTS tariff_title Nullable(String);
ALTER TABLE serving.kpi_revenue_daily ADD COLUMN IF NOT EXISTS tariff_title Nullable(String);
ALTER TABLE serving.kpi_arpu_daily ADD COLUMN IF NOT EXISTS tariff_title Nullable(String);
ALTER TABLE serving.kpi_inflow_daily ADD COLUMN IF NOT EXISTS tariff_title Nullable(String);
ALTER TABLE serving.kpi_outflow_daily ADD COLUMN IF NOT EXISTS tariff_title Nullable(String);
ALTER TABLE serving.kpi_revenue_active_month ADD COLUMN IF NOT EXISTS tariff_title Nullable(String);
ALTER TABLE serving.kpi_revenue_active_week ADD COLUMN IF NOT EXISTS tariff_title Nullable(String);
ALTER TABLE serving.kpi_revenue_active_quarter ADD COLUMN IF NOT EXISTS tariff_title Nullable(String);
ALTER TABLE serving.kpi_revenue_active_year ADD COLUMN IF NOT EXISTS tariff_title Nullable(String);

-- Справочник: в DDL serving не Nullable; пустая строка для уже существующих строк.
ALTER TABLE serving.dim_tariff ADD COLUMN IF NOT EXISTS tariff_title String DEFAULT '';
