-- Bronze -> Silver
-- Очистка, нормализация и разрешение сущностей

-- 1. Заполнение справочника аэропортов
INSERT INTO silver.dim_airport (
    airport_code,
    airport_name,
    city,
    state_code,
    latitude,
    longitude,
    timezone
)
VALUES
(
    'JFK',
    'John F. Kennedy International Airport',
    'New York', 'NY',
    40.6413, -73.7781,
    'America/New_York'
),
(
    'LAX',
    'Los Angeles International Airport',
    'Los Angeles', 'CA',
    33.9416, -118.4085,
    'America/Los_Angeles'
),
(
    'ORD',
    'O''Hare International Airport',
    'Chicago', 'IL',
    41.9742, -87.9073,
    'America/Chicago'
),
(
    'ATL',
    'Hartsfield-Jackson Atlanta International Airport',
    'Atlanta', 'GA',
    33.6407, -84.4277,
    'America/New_York'
),
(
    'DFW',
    'Dallas/Fort Worth International Airport',
    'Dallas', 'TX',
    32.8998, -97.0403,
    'America/Chicago'
)
ON CONFLICT (airport_code) DO NOTHING;

-- 2. Справочник авиакомпаний
INSERT INTO silver.dim_airline (
    airline_code,
    dot_airline_id
)
SELECT DISTINCT
    reporting_airline,
    dot_id_reporting_airline
FROM bronze.bts_flights_raw
WHERE reporting_airline IS NOT NULL
ON CONFLICT (airline_code) DO NOTHING;

-- 3. Рейсы Bronze -> Silver
INSERT INTO silver.flights (
    row_hash,
    flight_date,
    airline_id,
    origin_airport_id,
    dest_airport_code,
    flight_number,
    crs_dep_time,
    dep_time,
    dep_delay,
    dep_delay_minutes,
    cancelled,
    diverted,
    is_completed,
    is_delayed_15
)
SELECT
    b.row_hash,
    b.flight_date,

    a.airline_id,

    ap.airport_id,

    b.dest,

    b.flight_number_reporting_airline,
    b.crs_dep_time,
    b.dep_time,

    b.dep_delay,
    b.dep_delay_minutes,

    COALESCE(b.cancelled, 0) = 1,
    COALESCE(b.diverted, 0) = 1,

    COALESCE(b.cancelled, 0) = 0,

    COALESCE(b.cancelled, 0) = 0
        AND COALESCE(b.dep_delay_minutes, 0) >= 15

FROM bronze.bts_flights_raw b

JOIN silver.dim_airline a
    ON a.airline_code = b.reporting_airline

JOIN silver.dim_airport ap
    ON ap.airport_code = b.origin

WHERE
    b.flight_date IS NOT NULL
    AND b.origin IS NOT NULL
    AND b.dest IS NOT NULL

ON CONFLICT (row_hash) DO NOTHING;

-- 4. Погодные данные Bronze -> Silver
INSERT INTO silver.weather_hourly (
    airport_id,
    weather_time,
    temperature_2m,
    relative_humidity_2m,
    precipitation,
    rain,
    snowfall,
    weather_code,
    pressure_msl,
    cloud_cover,
    wind_speed_10m,
    wind_direction_10m,
    wind_gusts_10m
)
SELECT
    ap.airport_id,

    w.weather_time,

    w.temperature_2m,
    w.relative_humidity_2m,
    w.precipitation,
    w.rain,
    w.snowfall,
    w.weather_code,
    w.pressure_msl,
    w.cloud_cover,
    w.wind_speed_10m,
    w.wind_direction_10m,
    w.wind_gusts_10m

FROM bronze.weather_hourly_raw w

JOIN silver.dim_airport ap
    ON ap.airport_code = w.airport_code

WHERE
    w.weather_time IS NOT NULL

ON CONFLICT (airport_id, weather_time) DO NOTHING;