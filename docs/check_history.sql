-- Проверять после установки миграций 012/013 и history hooks.
-- 1. Миграции.
SELECT version,filename,applied_at FROM meta.schema_migrations ORDER BY version;
-- 2. Размеры. Сразу после внедрения: 5480 Gold и 5480 BASELINE-версий.
SELECT (SELECT COUNT(*) FROM gold.airport_daily) AS gold_rows,COUNT(*) AS history_versions,
 COUNT(*) FILTER(WHERE valid_to IS NULL AND operation<>'DELETE') current_live,
 COUNT(*) FILTER(WHERE pipeline_run_id IS NOT NULL) linked_versions
FROM gold.airport_daily_history;
-- 3. Операции в рабочей истории.
SELECT operation,COUNT(*) versions FROM gold.airport_daily_history GROUP BY operation ORDER BY operation;
-- 4. Примеры версий.
SELECT airport_code,flight_date,version_no,operation,total_flights,delay_share,
 valid_from,valid_to,is_current,pipeline_run_id,pipeline_step_id,change_source
FROM gold.airport_daily_history_bi ORDER BY history_id DESC LIMIT 20;
-- 5. Сравнение актуального состояния. Ожидается 0.
SELECT COUNT(*) AS mismatches
FROM gold.airport_daily g FULL JOIN (
 SELECT * FROM gold.airport_daily_history WHERE valid_to IS NULL AND operation<>'DELETE'
) h ON g.airport_id=h.airport_id AND g.flight_date=h.flight_date
WHERE g.airport_id IS NULL OR h.airport_id IS NULL
 OR meta.airport_daily_comparison_payload(to_jsonb(g)) IS DISTINCT FROM meta.airport_daily_comparison_payload(h.snapshot);
-- 6. Привязка реально изменившихся версий к шагам (может быть пусто, если показателей не меняли).
SELECT h.history_id,h.pipeline_run_id,s.transformation_id,h.operation,h.flight_date,h.version_no
FROM gold.airport_daily_history h JOIN meta.pipeline_steps s ON s.step_id=h.pipeline_step_id
ORDER BY h.history_id DESC LIMIT 20;
-- 7. Демонстрационный общий запуск истории должен быть SUCCESS/is_demo=True.
SELECT run_id,name,status,is_demo FROM meta.pipeline_runs WHERE name='history_demo' ORDER BY started_at DESC LIMIT 5;
-- 8. Каталог после history hooks: 25/14/40/3.
SELECT 'datasets' object,COUNT(*) rows FROM meta.dataset_catalog
UNION ALL SELECT 'transformations',COUNT(*) FROM meta.transformation_catalog
UNION ALL SELECT 'edges',COUNT(*) FROM meta.lineage_edges
UNION ALL SELECT 'metrics',COUNT(*) FROM meta.metric_catalog;
-- 9. Прогнозы должны остаться прежними.
SELECT COUNT(*) AS batches FROM meta.forecast_batches;
SELECT COUNT(*) AS forecasts FROM gold.airport_forecasts;
