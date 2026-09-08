CREATE TABLE IF NOT EXISTS bronze.bts_flights_raw (
    id BIGSERIAL PRIMARY KEY,

    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    day_of_month INTEGER,
    day_of_week INTEGER,
    flight_date DATE,

    reporting_airline VARCHAR(10),
    dot_id_reporting_airline INTEGER,
    flight_number_reporting_airline VARCHAR(20),

    origin_airport_id INTEGER,
    origin VARCHAR(10),
    origin_city_name VARCHAR(100),
    origin_state VARCHAR(10),

    dest_airport_id INTEGER,
    dest VARCHAR(10),
    dest_city_name VARCHAR(100),
    dest_state VARCHAR(10),

    crs_dep_time INTEGER,
    dep_time INTEGER,
    dep_delay DOUBLE PRECISION,
    dep_delay_minutes DOUBLE PRECISION,
    dep_del15 DOUBLE PRECISION,

    crs_arr_time INTEGER,
    arr_time INTEGER,
    arr_delay DOUBLE PRECISION,
    arr_delay_minutes DOUBLE PRECISION,
    arr_del15 DOUBLE PRECISION,

    cancelled DOUBLE PRECISION,
    cancellation_code VARCHAR(10),
    diverted DOUBLE PRECISION,

    air_time DOUBLE PRECISION,
    distance DOUBLE PRECISION,

    carrier_delay DOUBLE PRECISION,
    weather_delay DOUBLE PRECISION,
    nas_delay DOUBLE PRECISION,
    security_delay DOUBLE PRECISION,
    late_aircraft_delay DOUBLE PRECISION,

    source_file TEXT NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    row_hash VARCHAR(64) NOT NULL UNIQUE
);


CREATE TABLE IF NOT EXISTS bronze.weather_hourly_raw (
    id BIGSERIAL PRIMARY KEY,

    airport_code VARCHAR(10) NOT NULL,
    weather_time TIMESTAMP NOT NULL,

    temperature_2m DOUBLE PRECISION,
    relative_humidity_2m INTEGER,
    precipitation DOUBLE PRECISION,
    rain DOUBLE PRECISION,
    snowfall DOUBLE PRECISION,
    weather_code INTEGER,
    pressure_msl DOUBLE PRECISION,
    cloud_cover INTEGER,
    wind_speed_10m DOUBLE PRECISION,
    wind_direction_10m INTEGER,
    wind_gusts_10m DOUBLE PRECISION,

    source_file TEXT NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (airport_code, weather_time)
);