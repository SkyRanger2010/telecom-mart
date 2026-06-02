-- -----------------------------------------------------------------------------
-- Монолитный SQL-эталон всех KPI-витрин за один день (2024-06-02).
-- Рекомендуемый режим эксплуатации: по одному скрипту на витрину —
t-- ``mart_vitrina_runner.py`` и ``mart_jobs/gen_kpi_*_daily.py``,
-- параметр ``--report-date`` (совпадает с ds DAG).
--
-- Порядок DAG: RAW -> validate_raw_daily.sql -> DDS -> этот файл.
-- Спецификация KPI: VKR_SCOPE_MVP.md §5.5.
-- DIAG: представления iceberg.mart.v_* см. mart_kpi_mvp.sql
-- -----------------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS iceberg.mart;

CREATE TABLE IF NOT EXISTS iceberg.mart.kpi_ab0_daily (
    report_date DATE NOT NULL,
    segment_id BIGINT,
    service_kind VARCHAR,
    tariff_id BIGINT,
    active_subscribers BIGINT NOT NULL,
    refreshed_at TIMESTAMP(6) WITH TIME ZONE NOT NULL
)
WITH (partitioning = ARRAY['day(report_date)']);

CREATE TABLE IF NOT EXISTS iceberg.mart.kpi_ab30_daily (
    report_date DATE NOT NULL,
    segment_id BIGINT,
    service_kind VARCHAR,
    tariff_id BIGINT,
    active_subscribers BIGINT NOT NULL,
    refreshed_at TIMESTAMP(6) WITH TIME ZONE NOT NULL
)
WITH (partitioning = ARRAY['day(report_date)']);

CREATE TABLE IF NOT EXISTS iceberg.mart.kpi_ab90_daily (
    report_date DATE NOT NULL,
    segment_id BIGINT,
    service_kind VARCHAR,
    tariff_id BIGINT,
    active_subscribers BIGINT NOT NULL,
    refreshed_at TIMESTAMP(6) WITH TIME ZONE NOT NULL
)
WITH (partitioning = ARRAY['day(report_date)']);

CREATE TABLE IF NOT EXISTS iceberg.mart.kpi_revenue_daily (
    report_date DATE NOT NULL,
    segment_id BIGINT,
    service_kind VARCHAR,
    tariff_id BIGINT,
    total_revenue DOUBLE NOT NULL,
    paying_owners BIGINT NOT NULL,
    refreshed_at TIMESTAMP(6) WITH TIME ZONE NOT NULL
)
WITH (partitioning = ARRAY['day(report_date)']);

CREATE TABLE IF NOT EXISTS iceberg.mart.kpi_arpu_daily (
    report_date DATE NOT NULL,
    month_start DATE NOT NULL,
    segment_id BIGINT,
    service_kind VARCHAR,
    tariff_id BIGINT,
    total_revenue DOUBLE NOT NULL,
    active_clients BIGINT NOT NULL,
    arpu_daily DOUBLE,
    refreshed_at TIMESTAMP(6) WITH TIME ZONE NOT NULL
)
WITH (partitioning = ARRAY['day(report_date)']);

CREATE TABLE IF NOT EXISTS iceberg.mart.kpi_inflow_daily (
    report_date DATE NOT NULL,
    segment_id BIGINT,
    service_kind VARCHAR,
    tariff_id BIGINT,
    new_clients BIGINT NOT NULL,
    new_agreements BIGINT NOT NULL,
    new_orders BIGINT NOT NULL,
    refreshed_at TIMESTAMP(6) WITH TIME ZONE NOT NULL
)
WITH (partitioning = ARRAY['day(report_date)']);

CREATE TABLE IF NOT EXISTS iceberg.mart.kpi_outflow_daily (
    report_date DATE NOT NULL,
    segment_id BIGINT,
    service_kind VARCHAR,
    tariff_id BIGINT,
    churned_clients BIGINT NOT NULL,
    completed_agreements BIGINT NOT NULL,
    completed_orders BIGINT NOT NULL,
    refreshed_at TIMESTAMP(6) WITH TIME ZONE NOT NULL
)
WITH (partitioning = ARRAY['day(report_date)']);

-- =====================================================================
-- <<< CONFIG: синхронно подставить дату ниже во все операторы блока >>> 
-- =====================================================================

-- AB0: активные заказчики на дату среза.
-- Условие: status='ENABLED', activated IS NOT NULL, expire_time >= process_date.
-- Подзапрос sub_ord фильтрует expenditure=false, is_draft=false.
DELETE FROM iceberg.mart.kpi_ab0_daily WHERE report_date = DATE '2024-06-02';

INSERT INTO iceberg.mart.kpi_ab0_daily (
    report_date,
    segment_id,
    service_kind,
    tariff_id,
    active_subscribers,
    refreshed_at
)
WITH
    cfg AS (SELECT DATE '2024-06-02' AS process_date),
    sub_ord AS (
        SELECT
            o.*,
            CAST(a.client_id AS BIGINT) AS subscriber_id
        FROM iceberg.dds.dds_orders o
        INNER JOIN iceberg.dds.dds_agreements a
            ON CAST(a.id AS BIGINT) = o.owner_id
        WHERE COALESCE(o.expenditure, false) = false
          AND COALESCE(o.is_draft, false) = false
    )
SELECT
    c.process_date,
    o.segment_id,
    o.base_type,
    o.tariff_id,
    COUNT(DISTINCT o.subscriber_id),
    current_timestamp
FROM cfg c
INNER JOIN sub_ord o ON true
WHERE o.status = 'ENABLED'
  AND o.activated IS NOT NULL
  AND (o.expire_time IS NULL OR CAST(o.expire_time AS DATE) >= c.process_date)
GROUP BY c.process_date, o.segment_id, o.base_type, o.tariff_id;

-- AB30/AB90: окно 30/90 календарных дней — activated в прошлом,
-- expire_time >= process_date - (29 | 89) дней.
-- Статус: ENABLED или EXPIRED (DISABLED исключены).
DELETE FROM iceberg.mart.kpi_ab30_daily WHERE report_date = DATE '2024-06-02';

INSERT INTO iceberg.mart.kpi_ab30_daily (
    report_date,
    segment_id,
    service_kind,
    tariff_id,
    active_subscribers,
    refreshed_at
)
WITH
    cfg AS (SELECT DATE '2024-06-02' AS process_date),
    sub_ord AS (
        SELECT
            o.*,
            CAST(a.client_id AS BIGINT) AS subscriber_id
        FROM iceberg.dds.dds_orders o
        INNER JOIN iceberg.dds.dds_agreements a
            ON CAST(a.id AS BIGINT) = o.owner_id
        WHERE COALESCE(o.expenditure, false) = false
          AND COALESCE(o.is_draft, false) = false
    )
SELECT
    c.process_date,
    o.segment_id,
    o.base_type,
    o.tariff_id,
    COUNT(DISTINCT o.subscriber_id),
    current_timestamp
FROM cfg c
INNER JOIN sub_ord o ON true
WHERE o.activated IS NOT NULL
  AND CAST(o.activated AS DATE) <= CAST(c.process_date AS DATE)
  AND (o.expire_time IS NULL OR CAST(o.expire_time AS DATE) >= date_add('day', -29, CAST(c.process_date AS DATE)))
  AND o.status IN ('ENABLED', 'EXPIRED')
GROUP BY c.process_date, o.segment_id, o.base_type, o.tariff_id;

DELETE FROM iceberg.mart.kpi_ab90_daily WHERE report_date = DATE '2024-06-02';

INSERT INTO iceberg.mart.kpi_ab90_daily (
    report_date,
    segment_id,
    service_kind,
    tariff_id,
    active_subscribers,
    refreshed_at
)
WITH
    cfg AS (SELECT DATE '2024-06-02' AS process_date),
    sub_ord AS (
        SELECT
            o.*,
            CAST(a.client_id AS BIGINT) AS subscriber_id
        FROM iceberg.dds.dds_orders o
        INNER JOIN iceberg.dds.dds_agreements a
            ON CAST(a.id AS BIGINT) = o.owner_id
        WHERE COALESCE(o.expenditure, false) = false
          AND COALESCE(o.is_draft, false) = false
    )
SELECT
    c.process_date,
    o.segment_id,
    o.base_type,
    o.tariff_id,
    COUNT(DISTINCT o.subscriber_id),
    current_timestamp
FROM cfg c
INNER JOIN sub_ord o ON true
WHERE o.activated IS NOT NULL
  AND CAST(o.activated AS DATE) <= CAST(c.process_date AS DATE)
  AND (o.expire_time IS NULL OR CAST(o.expire_time AS DATE) >= date_add('day', -89, CAST(c.process_date AS DATE)))
  AND o.status IN ('ENABLED', 'EXPIRED')
GROUP BY c.process_date, o.segment_id, o.base_type, o.tariff_id;

-- ARPU: MTD-выручка к отчётной дате, делённая на активных клиентов.
-- rev CTE: нормализация credits по типу периода (MONTHLY / YEARLY / DAILY / ONETIME).
-- actives: AB0 на дату среза.
-- mtd_rev: накопленная нормализованная выручка с начала месяца окна.
DELETE FROM iceberg.mart.kpi_arpu_daily WHERE report_date = DATE '2024-06-02';

INSERT INTO iceberg.mart.kpi_arpu_daily (
    report_date,
    month_start,
    segment_id,
    service_kind,
    tariff_id,
    total_revenue,
    active_clients,
    arpu_daily,
    refreshed_at
)
WITH
    cfg AS (SELECT DATE '2024-06-02' AS process_date),
    mb AS (
        SELECT c.process_date,
               CAST(date_trunc('month', c.process_date) AS DATE) AS month_start
        FROM cfg c
    ),
    sub_ord AS (
        SELECT
            o.*,
            CAST(a.client_id AS BIGINT) AS subscriber_id
        FROM iceberg.dds.dds_orders o
        INNER JOIN iceberg.dds.dds_agreements a
            ON CAST(a.id AS BIGINT) = o.owner_id
        WHERE COALESCE(o.expenditure, false) = false
          AND COALESCE(o.is_draft, false) = false
    ),
    rev AS (
        SELECT CAST(l.period AS DATE) AS revenue_day,
               l.order_id,
               CAST(SUM((CASE CAST(l.period_type AS VARCHAR)
                   WHEN 'MONTHLY' THEN CAST(l.credits AS DOUBLE) / CAST(date_diff(
                       'day',
                       date_trunc('month', CAST(l.period AS DATE)),
                       date_add('month', 1, date_trunc('month', CAST(l.period AS DATE)))
                   ) AS DOUBLE)
                   WHEN 'YEARLY' THEN CAST(l.credits AS DOUBLE) / CAST(date_diff(
                       'day',
                       date_trunc('year', CAST(l.period AS DATE)),
                       date_add('year', 1, date_trunc('year', CAST(l.period AS DATE)))
                   ) AS DOUBLE)
                   WHEN 'DAILY' THEN CAST(l.credits AS DOUBLE)
                   WHEN 'ONETIME' THEN CAST(l.credits AS DOUBLE)
                   ELSE CAST(l.credits AS DOUBLE)
               END)) AS DOUBLE) AS revenue_amount
        FROM iceberg.dds.dds_ledgers l
        WHERE l.period IS NOT NULL
          AND l.credits IS NOT NULL
          AND l.credits > 0
        GROUP BY 1, 2
    ),
    actives AS (
        SELECT b.process_date AS report_date,
               b.month_start,
               o.segment_id,
               o.base_type AS service_kind,
               o.tariff_id,
               COUNT(DISTINCT o.subscriber_id) AS active_clients
        FROM mb b
        INNER JOIN sub_ord o ON true
        WHERE o.status = 'ENABLED'
          AND o.activated IS NOT NULL
          AND (o.expire_time IS NULL OR CAST(o.expire_time AS DATE) >= CAST(b.process_date AS DATE))
        GROUP BY b.process_date, b.month_start, o.segment_id, o.base_type, o.tariff_id
    ),
    mtd_rev AS (
        SELECT b.process_date AS report_date,
               b.month_start,
               o.segment_id,
               o.base_type AS service_kind,
               o.tariff_id,
               CAST(SUM(r.revenue_amount) AS DOUBLE) AS total_revenue
        FROM mb b
        INNER JOIN rev r
            ON r.revenue_day >= b.month_start
           AND r.revenue_day <= b.process_date
        INNER JOIN sub_ord o ON o.id = r.order_id
        WHERE o.status = 'ENABLED'
          AND o.activated IS NOT NULL
          AND (o.expire_time IS NULL OR CAST(o.expire_time AS DATE) >= CAST(b.process_date AS DATE))
        GROUP BY b.process_date, b.month_start, o.segment_id, o.base_type, o.tariff_id
    )
SELECT
    a.report_date,
    a.month_start,
    a.segment_id,
    a.service_kind,
    a.tariff_id,
    ROUND(COALESCE(m.total_revenue, CAST(0 AS DOUBLE)), 2),
    a.active_clients,
    CASE
        WHEN a.active_clients > 0
        THEN ROUND(
            COALESCE(m.total_revenue, CAST(0 AS DOUBLE)) / CAST(a.active_clients AS DOUBLE),
            2
        )
        ELSE CAST(NULL AS DOUBLE)
    END,
    current_timestamp
FROM actives a
LEFT JOIN mtd_rev m
    ON a.report_date = m.report_date
   AND a.month_start IS NOT DISTINCT FROM m.month_start
   AND a.segment_id IS NOT DISTINCT FROM m.segment_id
   AND a.service_kind IS NOT DISTINCT FROM m.service_kind
   AND a.tariff_id IS NOT DISTINCT FROM m.tariff_id;

-- kpi_revenue_daily: та же методика, что kpi_arpu_daily (DDS rev + активные заказы на дату),
-- сумма нормализованных кредитов за revenue_day = report_date. Реализация: mart_runner_common.statements_for_vitrina (ветка revenue).
/*
DELETE / INSERT для kpi_revenue_daily раньше строились из kpi_arpu_daily (прирост MTD); актуальный SQL генерируется в Python — см. mart_runner_common.py.
*/
-- Приток: заказы с activated = report_date.
-- first_client_activation: MIN(activated) по subscriber_id.
-- first_agreement_activation: MIN(activated) по agreement_id (owner_id).
-- new_clients: те, у кого first_activation_day = report_date.
-- new_agreements: те, у кого first_activation_day = report_date.
-- new_orders: все заказы с activated = report_date.
DELETE FROM iceberg.mart.kpi_inflow_daily WHERE report_date = DATE '2024-06-02';

INSERT INTO iceberg.mart.kpi_inflow_daily (
    report_date,
    segment_id,
    service_kind,
    tariff_id,
    new_clients,
    new_agreements,
    new_orders,
    refreshed_at
)
WITH
    cfg AS (SELECT DATE '2024-06-02' AS process_date),
    sub_ord AS (
        SELECT
            o.*,
            CAST(a.client_id AS BIGINT) AS subscriber_id
        FROM iceberg.dds.dds_orders o
        INNER JOIN iceberg.dds.dds_agreements a
            ON CAST(a.id AS BIGINT) = o.owner_id
        WHERE COALESCE(o.expenditure, false) = false
          AND COALESCE(o.is_draft, false) = false
    ),
    first_client_activation AS (
        SELECT subscriber_id,
               MIN(CAST(activated AS DATE)) AS first_activation_day
        FROM sub_ord
        WHERE activated IS NOT NULL
        GROUP BY subscriber_id
    ),
    first_agreement_activation AS (
        SELECT owner_id AS agreement_id,
               MIN(CAST(activated AS DATE)) AS first_activation_day
        FROM sub_ord
        WHERE activated IS NOT NULL
        GROUP BY owner_id
    )
SELECT c.process_date,
       o.segment_id,
       o.base_type,
       o.tariff_id,
       COUNT(DISTINCT CASE
           WHEN fca.first_activation_day = c.process_date
           THEN o.subscriber_id
       END),
       COUNT(DISTINCT CASE
           WHEN faa.first_activation_day = c.process_date
           THEN o.owner_id
       END),
       COUNT(*),
       current_timestamp
FROM cfg c
INNER JOIN sub_ord o ON CAST(o.activated AS DATE) = c.process_date
INNER JOIN first_client_activation fca ON fca.subscriber_id = o.subscriber_id
INNER JOIN first_agreement_activation faa ON faa.agreement_id = o.owner_id
GROUP BY c.process_date, o.segment_id, o.base_type, o.tariff_id;

-- Отток: заказы с expire_time = report_date.
-- agreement_last_end: MAX(expire_time) по agreement_id.
-- client_last_end: MAX(expire_time) по subscriber_id.
-- churned_clients: те, у кого last_service_end_day = report_date (все подписки завершены).
-- completed_agreements: те, у кого last_service_end_day = report_date.
-- completed_orders: все заказы с expire_time = report_date.
DELETE FROM iceberg.mart.kpi_outflow_daily WHERE report_date = DATE '2024-06-02';

INSERT INTO iceberg.mart.kpi_outflow_daily (
    report_date,
    segment_id,
    service_kind,
    tariff_id,
    churned_clients,
    completed_agreements,
    completed_orders,
    refreshed_at
)
WITH
    cfg AS (SELECT DATE '2024-06-02' AS process_date),
    sub_ord AS (
        SELECT
            o.*,
            CAST(a.client_id AS BIGINT) AS subscriber_id
        FROM iceberg.dds.dds_orders o
        INNER JOIN iceberg.dds.dds_agreements a
            ON CAST(a.id AS BIGINT) = o.owner_id
        WHERE COALESCE(o.expenditure, false) = false
          AND COALESCE(o.is_draft, false) = false
    ),
    agreement_last_end AS (
        SELECT owner_id AS agreement_id,
               MAX(CAST(expire_time AS DATE)) AS last_service_end_day
        FROM sub_ord
        WHERE expire_time IS NOT NULL
        GROUP BY owner_id
    ),
    client_last_end AS (
        SELECT subscriber_id,
               MAX(CAST(expire_time AS DATE)) AS last_service_end_day
        FROM sub_ord
        WHERE expire_time IS NOT NULL
        GROUP BY subscriber_id
    )
SELECT c.process_date,
       o.segment_id,
       o.base_type,
       o.tariff_id,
       COUNT(DISTINCT CASE
           WHEN cle.last_service_end_day = c.process_date
           THEN o.subscriber_id
       END),
       COUNT(DISTINCT CASE
           WHEN ale.last_service_end_day = c.process_date
           THEN o.owner_id
       END),
       COUNT(*),
       current_timestamp
FROM cfg c
INNER JOIN sub_ord o
    ON o.expire_time IS NOT NULL
   AND CAST(o.expire_time AS DATE) = c.process_date
LEFT JOIN agreement_last_end ale ON ale.agreement_id = o.owner_id
LEFT JOIN client_last_end cle ON cle.subscriber_id = o.subscriber_id
GROUP BY c.process_date, o.segment_id, o.base_type, o.tariff_id;

-- -----------------------------------------------------------------------------
-- Дополнения (не блокирующие MVP):
-- * ARPU месяца/квартала уже отражены в режиме «MTD месяца на каждый.report_date» (kpi_arpu_daily).
-- -----------------------------------------------------------------------------
