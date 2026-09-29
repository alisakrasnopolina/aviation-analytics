-- Создадим схему
CREATE SCHEMA IF NOT EXISTS silver;

-- Справочник аэропортов
CREATE TABLE IF NOT EXISTS silver.dim_airport (
    airport_id SERIAL PRIMARY KEY,
    airport_code VARCHAR(3) NOT NULL UNIQUE,
    airport_name TEXT,
    city TEXT,
    state_code VARCHAR(2),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    timezone TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Заполняем JFK
INSERT INTO silver.dim_airport (
    airport_code,
    airport_name,
    city,
    state_code,
    latitude,
    longitude,
    timezone
)
VALUES (
    'JFK',
    'John F. Kennedy International Airport',
    'New York',
    'NY',
    40.6413,
    -73.7781,
    'America/New_York'
)
ON CONFLICT (airport_code) DO NOTHING;

-- Справочник авиакомпаний
CREATE TABLE IF NOT EXISTS silver.dim_airline (
    airline_id SERIAL PRIMARY KEY,
    airline_code VARCHAR(10) NOT NULL UNIQUE,
    dot_airline_id INTEGER,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
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

-- Очищенная таблица рейсов Silver
CREATE TABLE IF NOT EXISTS silver.flights (
    flight_id BIGSERIAL PRIMARY KEY,

    row_hash VARCHAR(64) NOT NULL UNIQUE,

    flight_date DATE NOT NULL,

    airline_id INTEGER NOT NULL
        REFERENCES silver.dim_airline(airline_id),

    origin_airport_id INTEGER NOT NULL
        REFERENCES silver.dim_airport(airport_id),

    dest_airport_code VARCHAR(3) NOT NULL,

    flight_number VARCHAR(20),

    crs_dep_time INTEGER,
    dep_time INTEGER,

    dep_delay DOUBLE PRECISION,
    dep_delay_minutes DOUBLE PRECISION,

    cancelled BOOLEAN NOT NULL,
    diverted BOOLEAN NOT NULL,

    is_completed BOOLEAN NOT NULL,
    is_delayed_15 BOOLEAN NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Заполняем silver.flights
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

-- Создаём Silver для погоды
CREATE TABLE IF NOT EXISTS silver.weather_hourly (
    weather_id BIGSERIAL PRIMARY KEY,

    airport_id INTEGER NOT NULL
        REFERENCES silver.dim_airport(airport_id),

    weather_time TIMESTAMP NOT NULL,

    temperature_2m DOUBLE PRECISION,
    relative_humidity_2m DOUBLE PRECISION,
    precipitation DOUBLE PRECISION,
    rain DOUBLE PRECISION,
    snowfall DOUBLE PRECISION,
    weather_code INTEGER,
    pressure_msl DOUBLE PRECISION,
    cloud_cover DOUBLE PRECISION,
    wind_speed_10m DOUBLE PRECISION,
    wind_direction_10m DOUBLE PRECISION,
    wind_gusts_10m DOUBLE PRECISION,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (airport_id, weather_time)
);

-- Создаём Silver для погоды
CREATE TABLE IF NOT EXISTS silver.weather_hourly (
    weather_id BIGSERIAL PRIMARY KEY,

    airport_id INTEGER NOT NULL
        REFERENCES silver.dim_airport(airport_id),

    weather_time TIMESTAMP NOT NULL,

    temperature_2m DOUBLE PRECISION,
    relative_humidity_2m DOUBLE PRECISION,
    precipitation DOUBLE PRECISION,
    rain DOUBLE PRECISION,
    snowfall DOUBLE PRECISION,
    weather_code INTEGER,
    pressure_msl DOUBLE PRECISION,
    cloud_cover DOUBLE PRECISION,
    wind_speed_10m DOUBLE PRECISION,
    wind_direction_10m DOUBLE PRECISION,
    wind_gusts_10m DOUBLE PRECISION,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (airport_id, weather_time)
);

-- Переносим погоду Bronze → Silver
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