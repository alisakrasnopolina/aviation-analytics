-- Открыть в DBeaver; выполнять запросы отдельно. Этот файл НЕ класть в sql/.
-- 1. Миграции: ожидаются 001–011.
SELECT version,filename,applied_at FROM meta.schema_migrations ORDER BY version;
-- 2. Последние запуски, включая безопасную демонстрацию.
SELECT run_id,started_at,finished_at,status,is_demo,trigger_source,
       total_checks,passed_checks,warning_checks,failed_checks,error_checks
FROM meta.quality_runs ORDER BY started_at DESC LIMIT 10;
-- 3. Последние рабочие проверки: 15 строк при штатном запуске.
SELECT check_id,check_name,category,severity,status,violations,allowed_violations,details
FROM gold.quality_latest_bi ORDER BY check_id;
-- 4. Проблемы последней рабочей проверки: ожидается пусто, либо обоснованные предупреждения.
SELECT check_id,status,violations,details FROM gold.quality_latest_bi WHERE status<>'PASS';
-- 5. Последняя демонстрация: должны быть FAIL, рабочие данные при этом не менялись.
SELECT r.check_id,r.status,r.violations,r.details
FROM meta.quality_results r
WHERE r.run_id=(SELECT run_id FROM meta.quality_runs WHERE is_demo ORDER BY started_at DESC LIMIT 1)
ORDER BY r.check_id;
-- 6. Gold: ожидаются прежние 5480 строк, по 1096 на аэропорт.
SELECT a.airport_code,COUNT(*) days,MIN(g.flight_date) date_from,MAX(g.flight_date) date_to
FROM gold.airport_daily g JOIN silver.dim_airport a USING(airport_id)
GROUP BY a.airport_code ORDER BY a.airport_code;
-- 7. Архив прогнозов не должен увеличиваться после одинакового predict.
SELECT COUNT(*) batches FROM meta.forecast_batches;
SELECT COUNT(*) forecasts FROM gold.airport_forecasts;
