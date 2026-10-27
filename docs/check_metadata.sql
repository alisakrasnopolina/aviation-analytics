-- Выполнять отдельно в DBeaver. Этот файл не помещать в sql/.
-- 1. Миграции 001–012.
SELECT version,filename,applied_at FROM meta.schema_migrations ORDER BY version;
-- 2. Ожидаемые размеры каталога: 24/13/39/3.
SELECT 'datasets' object,COUNT(*) rows FROM meta.dataset_catalog
UNION ALL SELECT 'transformations',COUNT(*) FROM meta.transformation_catalog
UNION ALL SELECT 'lineage_edges',COUNT(*) FROM meta.lineage_edges
UNION ALL SELECT 'metrics',COUNT(*) FROM meta.metric_catalog;
-- 3. Определения показателей.
SELECT metric_id,name,formula,unit,source_fields,limitations FROM meta.metric_catalog ORDER BY metric_id;
-- 4. Связи происхождения данных.
SELECT source_id,target_id,transformation_id,file_path FROM gold.metadata_lineage_bi ORDER BY target_id,source_id;
-- 5. Общие запуски, в том числе ожидаемый demo FAILED.
SELECT run_id,name,status,is_demo,started_at,finished_at,error_type FROM meta.pipeline_runs ORDER BY started_at DESC LIMIT 10;
-- 6. Шаги последнего рабочего запуска.
SELECT step_no,transformation_id,status,duration_ms,code_file,input_datasets,output_datasets
FROM meta.pipeline_steps WHERE run_id=(
 SELECT run_id FROM meta.pipeline_runs WHERE NOT is_demo ORDER BY started_at DESC LIMIT 1
) ORDER BY step_no;
-- 7. Точная связь общего запуска с проверкой качества и прогнозным пакетом.
SELECT s.run_id,s.step_no,s.transformation_id,r.reference_type,r.reference_id
FROM meta.pipeline_steps s JOIN meta.pipeline_references r USING(step_id)
WHERE s.run_id=(SELECT run_id FROM meta.pipeline_runs WHERE NOT is_demo ORDER BY started_at DESC LIMIT 1)
ORDER BY s.step_no,r.reference_type;
-- 8. Проверка связанной записи качества.
SELECT s.run_id,q.run_id AS quality_run_id,q.status,q.total_checks,q.failed_checks
FROM meta.pipeline_steps s JOIN meta.pipeline_references r USING(step_id)
JOIN meta.quality_runs q ON r.reference_type='quality_run' AND r.reference_id=q.run_id
ORDER BY s.started_at DESC LIMIT 5;
-- 9. Связанный пакет и исходный эксперимент модели.
SELECT s.run_id,b.batch_id,b.experiment_run_id,b.history_through,b.history_sha256,b.model_sha256
FROM meta.pipeline_steps s JOIN meta.pipeline_references r USING(step_id)
JOIN meta.forecast_batches b ON r.reference_type='forecast_batch' AND r.reference_id=b.batch_id
ORDER BY s.started_at DESC LIMIT 5;
-- 10. Количество архивных прогнозов: ожидаются прежние 2 пакета и 40 строк.
SELECT COUNT(*) AS batches FROM meta.forecast_batches;
SELECT COUNT(*) AS forecasts FROM gold.airport_forecasts;
