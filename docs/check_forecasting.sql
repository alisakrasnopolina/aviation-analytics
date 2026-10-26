-- 1. Миграции: после установки ожидаются версии 001–010.
SELECT version,filename,applied_at FROM meta.schema_migrations ORDER BY version;
-- 2. Импортированные эксперименты.
SELECT run_id,imported_at FROM meta.forecast_experiments ORDER BY imported_at DESC;
-- 3. Контроль объёмов первого импорта: 48, 7300, 20.
SELECT 'metrics' AS object,COUNT(*) AS rows FROM gold.forecast_metrics
UNION ALL SELECT 'backtest',COUNT(*) FROM gold.forecast_backtest
UNION ALL SELECT 'forecasts',COUNT(*) FROM gold.airport_forecasts;
-- 4. Сравнение моделей на тесте.
SELECT target,model,airport_code,n,mae,rmse,selected_for_use
FROM gold.forecast_metrics_bi WHERE split='test' ORDER BY target,airport_code,model;
-- 5. Последние выбранные прогнозы: 10 строк.
SELECT airport_code,flight_date,history_through,target,model,prediction
FROM gold.airport_forecasts_bi WHERE selected_for_use ORDER BY target,airport_code;
-- 6. Проверка горизонта: ожидается 0.
SELECT COUNT(*) AS wrong_horizon FROM gold.airport_forecasts
WHERE flight_date <> history_through + 1;
