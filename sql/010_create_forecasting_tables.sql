-- Миграция 010: результаты экспериментов и однодневные прогнозы.
CREATE TABLE IF NOT EXISTS meta.forecast_experiments (
    run_id TEXT PRIMARY KEY,
    source_digest TEXT NOT NULL,
    snapshot_sha256 TEXT NOT NULL,
    manifest JSONB NOT NULL,
    imported_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS gold.forecast_metrics (
    run_id TEXT NOT NULL REFERENCES meta.forecast_experiments(run_id),
    split TEXT NOT NULL CHECK (split IN ('validation','test')),
    target TEXT NOT NULL CHECK (target IN ('total_flights','delay_share')),
    model TEXT NOT NULL CHECK (model IN ('seasonal_naive_7','catboost')),
    airport_code TEXT NOT NULL,
    n INTEGER NOT NULL CHECK (n > 0),
    mae DOUBLE PRECISION NOT NULL CHECK (mae >= 0 AND mae < 'Infinity'::float8),
    rmse DOUBLE PRECISION NOT NULL CHECK (rmse >= 0 AND rmse < 'Infinity'::float8),
    selected_for_use BOOLEAN NOT NULL,
    PRIMARY KEY (run_id,split,target,model,airport_code)
);
CREATE TABLE IF NOT EXISTS gold.forecast_backtest (
    run_id TEXT NOT NULL REFERENCES meta.forecast_experiments(run_id),
    airport_id INTEGER NOT NULL REFERENCES silver.dim_airport(airport_id),
    flight_date DATE NOT NULL,
    history_through DATE NOT NULL,
    split TEXT NOT NULL CHECK (split IN ('validation','test')),
    target TEXT NOT NULL CHECK (target IN ('total_flights','delay_share')),
    model TEXT NOT NULL CHECK (model IN ('seasonal_naive_7','catboost')),
    actual DOUBLE PRECISION NOT NULL CHECK (actual >= 0 AND actual < 'Infinity'::float8),
    prediction DOUBLE PRECISION NOT NULL CHECK (prediction >= 0 AND prediction < 'Infinity'::float8),
    selected_for_use BOOLEAN NOT NULL,
    CHECK (flight_date = history_through + 1),
    CHECK (target <> 'delay_share' OR (actual <= 100 AND prediction <= 100)),
    PRIMARY KEY (run_id,airport_id,flight_date,split,target,model)
);
CREATE TABLE IF NOT EXISTS meta.forecast_batches (
    batch_id TEXT PRIMARY KEY,
    experiment_run_id TEXT NOT NULL REFERENCES meta.forecast_experiments(run_id),
    history_through DATE NOT NULL,
    history_sha256 TEXT NOT NULL,
    model_sha256 TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS gold.airport_forecasts (
    batch_id TEXT NOT NULL REFERENCES meta.forecast_batches(batch_id),
    airport_id INTEGER NOT NULL REFERENCES silver.dim_airport(airport_id),
    flight_date DATE NOT NULL,
    history_through DATE NOT NULL,
    target TEXT NOT NULL CHECK (target IN ('total_flights','delay_share')),
    model TEXT NOT NULL CHECK (model IN ('seasonal_naive_7','catboost')),
    prediction DOUBLE PRECISION NOT NULL CHECK (prediction >= 0 AND prediction < 'Infinity'::float8),
    selected_for_use BOOLEAN NOT NULL,
    CHECK (flight_date = history_through + 1),
    CHECK (target <> 'delay_share' OR prediction <= 100),
    PRIMARY KEY (batch_id,airport_id,flight_date,target,model)
);
CREATE OR REPLACE VIEW gold.forecast_backtest_bi AS
WITH latest AS (
 SELECT run_id FROM meta.forecast_experiments ORDER BY imported_at DESC, run_id DESC LIMIT 1
)
SELECT b.run_id,a.airport_code,b.flight_date,b.history_through,b.split,b.target,
       b.model,b.actual,b.prediction,b.selected_for_use,
       ABS(b.actual-b.prediction) AS absolute_error
FROM gold.forecast_backtest b JOIN latest l USING(run_id)
JOIN silver.dim_airport a USING(airport_id);
CREATE OR REPLACE VIEW gold.forecast_metrics_bi AS
WITH latest AS (
 SELECT run_id FROM meta.forecast_experiments ORDER BY imported_at DESC, run_id DESC LIMIT 1
)
SELECT m.* FROM gold.forecast_metrics m JOIN latest l USING(run_id);
CREATE OR REPLACE VIEW gold.airport_forecasts_bi AS
WITH latest AS (
 SELECT batch_id FROM meta.forecast_batches
 ORDER BY history_through DESC, created_at DESC, batch_id DESC LIMIT 1
)
SELECT b.batch_id,b.experiment_run_id,a.airport_code,f.flight_date,f.history_through,
       f.target,f.model,f.prediction,f.selected_for_use,b.created_at
FROM gold.airport_forecasts f JOIN latest l USING(batch_id)
JOIN meta.forecast_batches b USING(batch_id)
JOIN silver.dim_airport a USING(airport_id);
