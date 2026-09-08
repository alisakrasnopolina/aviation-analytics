CREATE TABLE IF NOT EXISTS meta.load_log (
    load_id BIGSERIAL PRIMARY KEY,

    source VARCHAR(50) NOT NULL,
    object_name VARCHAR(100) NOT NULL,

    started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMPTZ,

    period_from VARCHAR(50),
    period_to VARCHAR(50),

    rows_received INTEGER DEFAULT 0,
    rows_inserted INTEGER DEFAULT 0,
    rows_skipped INTEGER DEFAULT 0,
    rows_rejected INTEGER DEFAULT 0,

    status VARCHAR(20) NOT NULL,

    error_message TEXT
);


CREATE TABLE IF NOT EXISTS meta.source_state (
    source VARCHAR(50) NOT NULL,
    object_name VARCHAR(100) NOT NULL,

    watermark_type VARCHAR(30) NOT NULL,
    watermark_value VARCHAR(100),

    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (source, object_name)
);