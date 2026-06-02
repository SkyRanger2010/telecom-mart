-- MART KPI MVP — эталонные определения для Trino над Iceberg.
-- Зависимости: схема dds заполнена (load_dds_initial.py + загрузки RAW по списку raw_load_config.json).
-- При другом каталоге замените префикс iceberg на значение TRINO_CATALOG.

-- Физические витрины на день: mart_vitrina_runner.py + mart_jobs/gen_kpi_*_daily.py (--report-date).
-- -----------------------------------------------------------------------------
-- Инвентарь метрик (дашборд MVP)
--
-- AB0      — активные абоненты на дату среза (ENABLED, интервал услуги не истёк).
-- AB30     — по каждому report_date уникальные клиенты (clients.id через договор) с подпиской, пересекающей календарные 30 дней до среза включительно (kpi_ab30_daily).
-- AB90     — то же для 90 календарных дней включительно (kpi_ab90_daily).
-- ARPU     — выручка / |абоненты в знаменателе|, зерно: день | месяц | квартал.
-- Выручка  — сумма начислений за период.
-- Приток   — по дню активации заказа: новые клиенты, новые договоры, новые заказы (см. v_inflow_daily_by_dims).
-- Отток    — по календарному дню expire_time: уход клиентов, завершившиеся договоры, завершившиеся заказы (см. v_outflow_daily_by_dims).
--
-- Общие разрезы: segment_id, base_type (вид услуги), tariff_id.
-- Подписи сегментов/тарифов: из iceberg.dds.dds_segments_snapshot / iceberg.dds.dds_tariffs_snapshot (JSON payload).
-- -----------------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS iceberg.mart;

-- Продуктовые заказы, присоединённые к договору: HALK orders.owner_id = agreements.id;
-- абонент — agreements.client_id. Исключаем expenditure (расходные) и draft (черновики).
-- Это базовое представление для всех витрин AB, revenue, inflow/outflow.
CREATE OR REPLACE VIEW iceberg.mart.v_subscriber_orders AS
SELECT
    o.id AS order_id,
    o.owner_id AS agreement_id,
    CAST(a.client_id AS bigint) AS subscriber_id,
    o.segment_id,
    o.base_type,
    o.tariff_id,
    o.status,
    o.activated,
    o.expire_time,
    o.suspended,
    o.expenditure,
    o.is_draft
FROM iceberg.dds.dds_orders o
INNER JOIN iceberg.dds.dds_agreements a
    ON CAST(a.id AS bigint) = o.owner_id
WHERE o.expenditure = false
  AND (o.is_draft = false OR o.is_draft IS NULL);

-- -----------------------------------------------------------------------------
-- AB0: уникальные клиенты (subscriber_id) с активным продуктовым заказом на текущую дату.
-- Условия: status='ENABLED', activated IS NOT NULL, expire_time не истекло.
-- GROUPING SETS даёт сразу все уровни агрегации (все комбинации разрезов + total).
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW iceberg.mart.v_ab0_by_dims AS
SELECT
    segment_id,
    base_type AS service_kind,
    tariff_id,
    COUNT(DISTINCT subscriber_id) AS ab0
FROM iceberg.mart.v_subscriber_orders
WHERE status = 'ENABLED'
  AND activated IS NOT NULL
  AND (expire_time IS NULL OR CAST(expire_time AS date) >= CURRENT_DATE)
GROUP BY GROUPING SETS (
    (segment_id, base_type, tariff_id),
    (segment_id, base_type),
    (segment_id, tariff_id),
    (base_type, tariff_id),
    (segment_id),
    (base_type),
    (tariff_id),
    ()
);

-- -----------------------------------------------------------------------------
-- Выручка по заказу из ledgers (MVP): положительные credits за календарный день period.
-- Без нормализации на длительность интервала проводки — сырая сумма за день.
-- ARPU знаменатель см. kpi_arpu_daily (активные клиенты AB0 на дату × MTD выручка по дням).
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW iceberg.mart.v_revenue_daily_by_order AS
SELECT
    CAST(l.period AS date) AS revenue_day,
    l.order_id,
    SUM(l.credits) AS revenue_amount
FROM iceberg.dds.dds_ledgers l
WHERE l.period IS NOT NULL
  AND l.credits IS NOT NULL
  AND l.credits > 0
GROUP BY 1, 2;

CREATE OR REPLACE VIEW iceberg.mart.v_revenue_daily_by_dims AS
SELECT
    r.revenue_day,
    o.segment_id,
    o.base_type AS service_kind,
    o.tariff_id,
    SUM(r.revenue_amount) AS total_revenue,
    COUNT(DISTINCT o.subscriber_id) AS owners_with_revenue
FROM iceberg.mart.v_revenue_daily_by_order r
INNER JOIN iceberg.mart.v_subscriber_orders o
    ON o.order_id = r.order_id
GROUP BY GROUPING SETS (
    (r.revenue_day, o.segment_id, o.base_type, o.tariff_id),
    (r.revenue_day, o.segment_id, o.base_type),
    (r.revenue_day, o.segment_id),
    (r.revenue_day, o.base_type),
    (r.revenue_day, o.tariff_id),
    (r.revenue_day)
);

-- -----------------------------------------------------------------------------
-- Иллюстрация окна AB30/AB90 (якорь — CURRENT_DATE для ad-hoc;
-- в проде временной ряд — kpi_ab30_daily и kpi_ab90_daily из mart_vitrina_runner по report_date).
-- Учитываются заказы статусом ENABLED или EXPIRED; DISABLED исключены.
-- Ограничение: без истории статусов в DDS пересуждение календаря по activated/expire.
--
-- AB30: все заказы, чей activated <= CURRENT_DATE и expire_time >= CURRENT_DATE - 29 дней.
-- AB90: то же, окно 90 дней.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW iceberg.mart.v_ab_activity_overlay_mvp AS
SELECT
    30 AS window_days,
    o.segment_id,
    o.base_type AS service_kind,
    o.tariff_id,
    COUNT(DISTINCT o.subscriber_id) AS active_subscribers
FROM iceberg.mart.v_subscriber_orders o
WHERE o.activated IS NOT NULL
  AND CAST(o.activated AS date) <= CURRENT_DATE
  AND (o.expire_time IS NULL OR CAST(o.expire_time AS date) >= date_add('day', -29, CAST(CURRENT_DATE AS date)))
  AND o.status IN ('ENABLED', 'EXPIRED')
GROUP BY GROUPING SETS (
    (o.segment_id, o.base_type, o.tariff_id),
    (o.segment_id, o.base_type),
    (o.segment_id, o.tariff_id),
    (o.base_type, o.tariff_id),
    (o.segment_id),
    (o.base_type),
    (o.tariff_id),
    ()
)
UNION ALL
SELECT
    90 AS window_days,
    o.segment_id,
    o.base_type AS service_kind,
    o.tariff_id,
    COUNT(DISTINCT o.subscriber_id) AS active_subscribers
FROM iceberg.mart.v_subscriber_orders o
WHERE o.activated IS NOT NULL
  AND CAST(o.activated AS date) <= CURRENT_DATE
  AND (o.expire_time IS NULL OR CAST(o.expire_time AS date) >= date_add('day', -89, CAST(CURRENT_DATE AS date)))
  AND o.status IN ('ENABLED', 'EXPIRED')
GROUP BY GROUPING SETS (
    (o.segment_id, o.base_type, o.tariff_id),
    (o.segment_id, o.base_type),
    (o.segment_id, o.tariff_id),
    (o.base_type, o.tariff_id),
    (o.segment_id),
    (o.base_type),
    (o.tariff_id),
    ()
);

-- -----------------------------------------------------------------------------
-- ARPU (MTD месяца на report_date): выручка ledgers, нормированная на календарный день заказа;
-- активные клиенты AB0 на дату; без разреза бизнес-центра.
-- Источник: физическая таблица kpi_arpu_daily (генерируется mart_vitrina_runner).
-- Поля: arpu_daily = дневной ARPU, arpu_monthly = arpu_daily × дней в месяце.
CREATE OR REPLACE VIEW iceberg.mart.v_arpu_daily_by_dims AS
SELECT
    report_date AS revenue_day,
    month_start,
    segment_id,
    service_kind,
    tariff_id,
    total_revenue,
    active_clients AS active_subscribers,
    arpu_daily AS arpu
FROM iceberg.mart.kpi_arpu_daily;

-- -----------------------------------------------------------------------------
-- Приток (календарный день активации заказа):
--   new_orders — все активации продуктовых заказов в этот день;
--   new_clients — agreements.client_id, у которых это глобальный первый день активации
--     (MIN(activated) по subscriber_id);
--   new_agreements — договор (orders.owner_id = agreements.id), у которого это первый
--     день активации любого заказа (MIN(activated) по agreement_id).
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW iceberg.mart.v_inflow_daily_by_dims AS
SELECT
    b.activation_day,
    b.segment_id,
    b.service_kind,
    b.tariff_id,
    COUNT(DISTINCT CASE
        WHEN b.client_first_day = b.activation_day THEN b.subscriber_id
    END) AS new_clients,
    COUNT(DISTINCT CASE
        WHEN b.agreement_first_day = b.activation_day THEN b.agreement_id
    END) AS new_agreements,
    COUNT(*) AS new_orders
FROM (
    SELECT
        CAST(o.activated AS date) AS activation_day,
        o.subscriber_id,
        o.agreement_id,
        o.segment_id,
        o.base_type AS service_kind,
        o.tariff_id,
        fc.first_activation_day AS client_first_day,
        fa.first_activation_day AS agreement_first_day
    FROM iceberg.mart.v_subscriber_orders o
    INNER JOIN (
        SELECT subscriber_id, MIN(CAST(activated AS date)) AS first_activation_day
        FROM iceberg.mart.v_subscriber_orders
        WHERE activated IS NOT NULL
        GROUP BY subscriber_id
    ) fc ON fc.subscriber_id = o.subscriber_id
    INNER JOIN (
        SELECT agreement_id, MIN(CAST(activated AS date)) AS first_activation_day
        FROM iceberg.mart.v_subscriber_orders
        WHERE activated IS NOT NULL
        GROUP BY agreement_id
    ) fa ON fa.agreement_id = o.agreement_id
    WHERE o.activated IS NOT NULL
) b
GROUP BY GROUPING SETS (
    (b.activation_day, b.segment_id, b.service_kind, b.tariff_id),
    (b.activation_day, b.segment_id, b.service_kind),
    (b.activation_day, b.segment_id),
    (b.activation_day, b.service_kind),
    (b.activation_day, b.tariff_id),
    (b.activation_day)
);

-- -----------------------------------------------------------------------------
-- Отток (календарный день CAST(expire_time AS date), только заказы с expire_time):
--   completed_orders — заказы с этим днём окончания;
--   completed_agreements — договор, у которого MAX(expire) по всем его заказам = этот день
--     (все подписки договора завершены);
--   churned_clients — клиент, у которого MAX(expire) по всем его заказам = этот день
--     (клиент полностью ушёл, нет ни одной активной подписки).
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW iceberg.mart.v_outflow_daily_by_dims AS
SELECT
    x.outflow_day,
    x.segment_id,
    x.service_kind,
    x.tariff_id,
    COUNT(DISTINCT CASE
        WHEN x.client_last_end_day = x.outflow_day THEN x.subscriber_id
    END) AS churned_clients,
    COUNT(DISTINCT CASE
        WHEN x.agreement_last_end_day = x.outflow_day THEN x.agreement_id
    END) AS completed_agreements,
    COUNT(*) AS completed_orders
FROM (
    SELECT
        CAST(o.expire_time AS date) AS outflow_day,
        o.subscriber_id,
        o.agreement_id,
        o.segment_id,
        o.base_type AS service_kind,
        o.tariff_id,
        ale.last_end_day AS agreement_last_end_day,
        cle.last_end_day AS client_last_end_day
    FROM iceberg.mart.v_subscriber_orders o
    LEFT JOIN (
        SELECT agreement_id, MAX(CAST(expire_time AS date)) AS last_end_day
        FROM iceberg.mart.v_subscriber_orders
        WHERE expire_time IS NOT NULL
        GROUP BY agreement_id
    ) ale ON ale.agreement_id = o.agreement_id
    LEFT JOIN (
        SELECT subscriber_id, MAX(CAST(expire_time AS date)) AS last_end_day
        FROM iceberg.mart.v_subscriber_orders
        WHERE expire_time IS NOT NULL
        GROUP BY subscriber_id
    ) cle ON cle.subscriber_id = o.subscriber_id
    WHERE o.expire_time IS NOT NULL
) x
GROUP BY GROUPING SETS (
    (x.outflow_day, x.segment_id, x.service_kind, x.tariff_id),
    (x.outflow_day, x.segment_id, x.service_kind),
    (x.outflow_day, x.segment_id),
    (x.outflow_day, x.service_kind),
    (x.outflow_day, x.tariff_id),
    (x.outflow_day)
);
