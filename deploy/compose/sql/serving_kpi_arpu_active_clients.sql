-- kpi_arpu_daily: в Iceberg/MART колонка называется active_clients; в старом serving могло остаться paying_owners.
-- Выполнить, если в system.columns есть paying_owners и нет active_clients.

ALTER TABLE serving.kpi_arpu_daily RENAME COLUMN paying_owners TO active_clients;
