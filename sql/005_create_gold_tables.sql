CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.airport_daily (
    airport_id INTEGER NOT NULL
        REFERENCES silver.dim_airport(airport_id),

    flight_date DATE NOT NULL,

    total_flights INTEGER NOT NULL,
    completed_flights INTEGER NOT NULL,
    cancelled_flights INTEGER NOT NULL,
    diverted_flights INTEGER NOT NULL,
    delayed_flights INTEGER NOT NULL,

    delay_share DOUBLE PRECISION,

    avg_dep_delay DOUBLE PRECISION,

    avg_temperature DOUBLE PRECISION,
    total_precipitation DOUBLE PRECISION,
    avg_wind_speed DOUBLE PRECISION,
    max_wind_gust DOUBLE PRECISION,

    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (airport_id, flight_date)
);


WITH flight_daily AS (
    SELECT
        origin_airport_id AS airport_id,
        flight_date,

        COUNT(*) AS total_flights,

        COUNT(*) FILTER (
            WHERE is_completed
        ) AS completed_flights,

        COUNT(*) FILTER (
            WHERE cancelled
        ) AS cancelled_flights,

        COUNT(*) FILTER (
            WHERE diverted
        ) AS diverted_flights,

        COUNT(*) FILTER (
            WHERE is_delayed_15
        ) AS delayed_flights,

        AVG(dep_delay) FILTER (
            WHERE is_completed
        ) AS avg_dep_delay

    FROM silver.flights

    GROUP BY
        origin_airport_id,
        flight_date
),

weather_daily AS (
    SELECT
        airport_id,
        weather_time::date AS weather_date,

        AVG(temperature_2m) AS avg_temperature,
        SUM(precipitation) AS total_precipitation,
        AVG(wind_speed_10m) AS avg_wind_speed,
        MAX(wind_gusts_10m) AS max_wind_gust

    FROM silver.weather_hourly

    GROUP BY
        airport_id,
        weather_time::date
)

INSERT INTO gold.airport_daily (
    airport_id,
    flight_date,
    total_flights,
    completed_flights,
    cancelled_flights,
    diverted_flights,
    delayed_flights,
    delay_share,
    avg_dep_delay,
    avg_temperature,
    total_precipitation,
    avg_wind_speed,
    max_wind_gust
)

SELECT
    f.airport_id,
    f.flight_date,

    f.total_flights,
    f.completed_flights,
    f.cancelled_flights,
    f.diverted_flights,
    f.delayed_flights,

    CASE
        WHEN f.completed_flights > 0
        THEN
            100.0 * f.delayed_flights
            / f.completed_flights
        ELSE 0
    END AS delay_share,

    f.avg_dep_delay,

    w.avg_temperature,
    w.total_precipitation,
    w.avg_wind_speed,
    w.max_wind_gust

FROM flight_daily f

LEFT JOIN weather_daily w
    ON w.airport_id = f.airport_id
    AND w.weather_date = f.flight_date

ON CONFLICT (airport_id, flight_date)
DO UPDATE SET
    total_flights = EXCLUDED.total_flights,
    completed_flights = EXCLUDED.completed_flights,
    cancelled_flights = EXCLUDED.cancelled_flights,
    diverted_flights = EXCLUDED.diverted_flights,
    delayed_flights = EXCLUDED.delayed_flights,
    delay_share = EXCLUDED.delay_share,
    avg_dep_delay = EXCLUDED.avg_dep_delay,
    avg_temperature = EXCLUDED.avg_temperature,
    total_precipitation = EXCLUDED.total_precipitation,
    avg_wind_speed = EXCLUDED.avg_wind_speed,
    max_wind_gust = EXCLUDED.max_wind_gust,
    updated_at = NOW();