-- 011: журнал контроля качества. Существующие данные не изменяются.
CREATE TABLE IF NOT EXISTS meta.quality_runs (
    run_id TEXT PRIMARY KEY,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL CHECK (status IN ('RUNNING','PASS','WARN','FAIL','ERROR')),
    is_demo BOOLEAN NOT NULL DEFAULT FALSE,
    trigger_source TEXT NOT NULL,
    config_snapshot JSONB NOT NULL,
    total_checks INTEGER NOT NULL DEFAULT 0,
    passed_checks INTEGER NOT NULL DEFAULT 0,
    warning_checks INTEGER NOT NULL DEFAULT 0,
    failed_checks INTEGER NOT NULL DEFAULT 0,
    error_checks INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS meta.quality_results (
    run_id TEXT NOT NULL REFERENCES meta.quality_runs(run_id),
    check_id TEXT NOT NULL,
    check_name TEXT NOT NULL,
    object_name TEXT NOT NULL,
    category TEXT NOT NULL,
    severity TEXT NOT NULL CHECK (severity IN ('critical','warning')),
    status TEXT NOT NULL CHECK (status IN ('PASS','WARN','FAIL','ERROR')),
    violations BIGINT CHECK (violations >= 0),
    allowed_violations BIGINT NOT NULL DEFAULT 0 CHECK (allowed_violations >= 0),
    checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    duration_ms DOUBLE PRECISION NOT NULL,
    details TEXT NOT NULL,
    PRIMARY KEY (run_id,check_id)
);
CREATE INDEX IF NOT EXISTS ix_quality_runs_started ON meta.quality_runs(started_at DESC);
CREATE OR REPLACE VIEW gold.quality_runs_bi AS
SELECT * FROM meta.quality_runs WHERE NOT is_demo;
CREATE OR REPLACE VIEW gold.quality_results_bi AS
SELECT r.*,q.started_at AS run_started_at,q.finished_at AS run_finished_at,
       q.status AS run_status,q.trigger_source
FROM meta.quality_results r JOIN meta.quality_runs q USING(run_id)
WHERE NOT q.is_demo;
CREATE OR REPLACE VIEW gold.quality_latest_bi AS
WITH latest AS (
 SELECT run_id FROM meta.quality_runs WHERE NOT is_demo
 ORDER BY started_at DESC,run_id DESC LIMIT 1
)
SELECT r.* FROM gold.quality_results_bi r JOIN latest l USING(run_id);
