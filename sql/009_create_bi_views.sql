-- Миграция 009
-- Представления Gold для Power BI

-- 1. Предметная аналитика аэропортов

CREATE OR REPLACE VIEW gold.airport_daily_bi AS
SELECT
    g.airport_id,
    a.airport_code,
    a.airport_name,
    a.city,
    a.state_code,

    g.flight_date,
    TO_CHAR(g.flight_date, 'YYYY') AS flight_year,
    TO_CHAR(g.flight_date, 'YYYY-MM') AS flight_month,

    g.total_flights,
    g.completed_flights,
    g.cancelled_flights,
    g.diverted_flights,
    g.delayed_flights,
    g.delay_share,
    g.avg_dep_delay,

    g.avg_temperature,
    g.total_precipitation,
    g.avg_wind_speed,
    g.max_wind_gust,

    g.updated_at

FROM gold.airport_daily g
JOIN silver.dim_airport a
    ON g.airport_id = a.airport_id;


-- 2. Операционная аналитика загрузок

CREATE OR REPLACE VIEW gold.pipeline_runs_bi AS
SELECT
    load_id,
    source,
    object_name,
    period_from,
    period_to,

    started_at,
    started_at::date AS started_date,
    finished_at,

    EXTRACT(
        EPOCH FROM (finished_at - started_at)
    )::DOUBLE PRECISION AS duration_seconds,

    COALESCE(rows_received, 0) AS rows_received,
    COALESCE(rows_inserted, 0) AS rows_inserted,
    COALESCE(rows_skipped, 0) AS rows_skipped,
    COALESCE(rows_rejected, 0) AS rows_rejected,

    status,
    error_message

FROM meta.load_log;