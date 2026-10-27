-- 013: версии состояния Gold. Начальная версия фиксирует состояние в момент внедрения.
CREATE TABLE IF NOT EXISTS gold.airport_daily_history (
    history_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    airport_id INTEGER NOT NULL REFERENCES silver.dim_airport(airport_id),
    flight_date DATE NOT NULL,
    version_no INTEGER NOT NULL CHECK(version_no>0),
    operation TEXT NOT NULL CHECK(operation IN ('BASELINE','INSERT','UPDATE','DELETE')),
    snapshot JSONB NOT NULL,
    valid_from TIMESTAMPTZ NOT NULL,
    valid_to TIMESTAMPTZ,
    pipeline_run_id TEXT REFERENCES meta.pipeline_runs(run_id),
    pipeline_step_id TEXT REFERENCES meta.pipeline_steps(step_id),
    change_source TEXT NOT NULL,
    CHECK(valid_to IS NULL OR valid_to>valid_from),
    UNIQUE(airport_id,flight_date,version_no)
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_airport_history_current
    ON gold.airport_daily_history(airport_id,flight_date) WHERE valid_to IS NULL;
CREATE INDEX IF NOT EXISTS ix_airport_history_pipeline
    ON gold.airport_daily_history(pipeline_run_id,pipeline_step_id);

CREATE OR REPLACE FUNCTION meta.append_airport_daily_version(
    target_table REGCLASS, p_airport INTEGER, p_date DATE, p_snapshot JSONB, p_operation TEXT
) RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE
    v_version INTEGER;
    v_previous TIMESTAMPTZ;
    v_time TIMESTAMPTZ;
    v_run TEXT:=NULLIF(current_setting('aviation.pipeline_run_id',TRUE),'');
    v_step TEXT:=NULLIF(current_setting('aviation.pipeline_step_id',TRUE),'');
    v_source TEXT:=COALESCE(NULLIF(current_setting('aviation.change_source',TRUE),''),'manual SQL / uninstrumented writer');
BEGIN
    -- Допустимы только рабочая таблица и временная таблица безопасной демонстрации.
    IF target_table <> 'gold.airport_daily_history'::regclass
       AND NOT EXISTS(SELECT 1 FROM pg_class c WHERE c.oid=target_table AND c.relnamespace=pg_my_temp_schema() AND c.relname='airport_daily_history_demo') THEN
        RAISE EXCEPTION 'Unsupported history target';
    END IF;
    IF v_step IS NOT NULL AND NOT EXISTS(SELECT 1 FROM meta.pipeline_steps WHERE step_id=v_step AND run_id=v_run) THEN
        RAISE EXCEPTION 'History step does not belong to the specified pipeline run';
    END IF;
    EXECUTE format('SELECT COALESCE(MAX(version_no),0),MAX(valid_from) FILTER(WHERE valid_to IS NULL) FROM %s WHERE airport_id=$1 AND flight_date=$2',target_table)
      INTO v_version,v_previous USING p_airport,p_date;
    v_time:=clock_timestamp();
    IF v_previous IS NOT NULL THEN v_time:=GREATEST(v_time,v_previous+INTERVAL '1 microsecond'); END IF;
    EXECUTE format('UPDATE %s SET valid_to=$3 WHERE airport_id=$1 AND flight_date=$2 AND valid_to IS NULL',target_table)
      USING p_airport,p_date,v_time;
    EXECUTE format('INSERT INTO %s(airport_id,flight_date,version_no,operation,snapshot,valid_from,pipeline_run_id,pipeline_step_id,change_source) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)',target_table)
      USING p_airport,p_date,v_version+1,p_operation,p_snapshot,v_time,v_run,v_step,v_source;
END;
$$;

-- Убираем только вычислительный шум float: сравнение до 8 десятичных знаков.
-- Снимок сохраняется с исходными значениями; NULL/строки NaN/Infinity не маскируются.
CREATE OR REPLACE FUNCTION meta.airport_daily_comparison_payload(p JSONB)
RETURNS JSONB LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE result JSONB:=p-'updated_at'; field TEXT;
BEGIN
    FOREACH field IN ARRAY ARRAY['delay_share','avg_dep_delay','avg_temperature','total_precipitation','avg_wind_speed','max_wind_gust'] LOOP
        IF jsonb_typeof(result->field)='number' THEN
            result:=jsonb_set(result,ARRAY[field],to_jsonb(round((result->>field)::numeric,8)));
        END IF;
    END LOOP;
    RETURN result;
END;
$$;

CREATE OR REPLACE FUNCTION meta.capture_airport_daily_history()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE target_table REGCLASS:=TG_ARGV[0]::regclass;
BEGIN
    IF TG_OP='UPDATE' THEN
        -- Обновление служебного времени не является новой версией показателей.
        IF meta.airport_daily_comparison_payload(to_jsonb(NEW)) IS NOT DISTINCT FROM meta.airport_daily_comparison_payload(to_jsonb(OLD)) THEN RETURN NEW; END IF;
        IF NEW.airport_id<>OLD.airport_id OR NEW.flight_date<>OLD.flight_date THEN
            PERFORM meta.append_airport_daily_version(target_table,OLD.airport_id,OLD.flight_date,to_jsonb(OLD)-'updated_at','DELETE');
            PERFORM meta.append_airport_daily_version(target_table,NEW.airport_id,NEW.flight_date,to_jsonb(NEW)-'updated_at','INSERT');
        ELSE
            PERFORM meta.append_airport_daily_version(target_table,NEW.airport_id,NEW.flight_date,to_jsonb(NEW)-'updated_at','UPDATE');
        END IF;
        RETURN NEW;
    ELSIF TG_OP='INSERT' THEN
        PERFORM meta.append_airport_daily_version(target_table,NEW.airport_id,NEW.flight_date,to_jsonb(NEW)-'updated_at','INSERT');
        RETURN NEW;
    ELSE
        -- DELETE хранится как текущее состояние «запись удалена» до возможной повторной вставки.
        PERFORM meta.append_airport_daily_version(target_table,OLD.airport_id,OLD.flight_date,to_jsonb(OLD)-'updated_at','DELETE');
        RETURN OLD;
    END IF;
END;
$$;

-- Не допускаем незаметную потерю истории при TRUNCATE; текущий Gold использует upsert.
CREATE OR REPLACE FUNCTION meta.reject_airport_daily_truncate()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'TRUNCATE Gold is disabled after enabling history; use deliberate row changes';
END;
$$;

-- Одна транзакция миграции + блокировка исключают разрыв между baseline и включением триггера.
LOCK TABLE gold.airport_daily IN SHARE ROW EXCLUSIVE MODE;
INSERT INTO gold.airport_daily_history(airport_id,flight_date,version_no,operation,snapshot,valid_from,change_source)
SELECT g.airport_id,g.flight_date,1,'BASELINE',to_jsonb(g)-'updated_at',clock_timestamp(),'migration 013 initial snapshot'
FROM gold.airport_daily g
WHERE NOT EXISTS(SELECT 1 FROM gold.airport_daily_history h WHERE h.airport_id=g.airport_id AND h.flight_date=g.flight_date);
DROP TRIGGER IF EXISTS trg_airport_daily_history ON gold.airport_daily;
CREATE TRIGGER trg_airport_daily_history AFTER INSERT OR UPDATE OR DELETE ON gold.airport_daily
FOR EACH ROW EXECUTE FUNCTION meta.capture_airport_daily_history('gold.airport_daily_history');
DROP TRIGGER IF EXISTS trg_airport_daily_no_truncate ON gold.airport_daily;
CREATE TRIGGER trg_airport_daily_no_truncate BEFORE TRUNCATE ON gold.airport_daily
FOR EACH STATEMENT EXECUTE FUNCTION meta.reject_airport_daily_truncate();

CREATE OR REPLACE VIEW gold.airport_daily_history_bi AS
SELECT h.history_id,h.airport_id,a.airport_code,h.flight_date,h.version_no,h.operation,
       h.valid_from,h.valid_to,(h.valid_to IS NULL) AS is_current,(h.operation='DELETE') AS is_deleted,
       (h.snapshot->>'total_flights')::INTEGER AS total_flights,
       (h.snapshot->>'completed_flights')::INTEGER AS completed_flights,
       (h.snapshot->>'cancelled_flights')::INTEGER AS cancelled_flights,
       (h.snapshot->>'delayed_flights')::INTEGER AS delayed_flights,
       (h.snapshot->>'delay_share')::DOUBLE PRECISION AS delay_share,
       h.snapshot,h.pipeline_run_id,h.pipeline_step_id,h.change_source
FROM gold.airport_daily_history h JOIN silver.dim_airport a USING(airport_id);
CREATE OR REPLACE VIEW gold.airport_daily_history_current_bi AS
SELECT * FROM gold.airport_daily_history_bi WHERE is_current AND NOT is_deleted;
