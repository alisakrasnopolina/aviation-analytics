-- Операционная витрина запусков загрузки
CREATE OR REPLACE VIEW gold.pipeline_runs AS
SELECT *
FROM meta.load_log;


-- Операционная витрина состояния источников
CREATE OR REPLACE VIEW gold.source_status AS
SELECT *
FROM meta.source_state;