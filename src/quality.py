"""Контроль качества: python -m src.quality; отчёт: --latest; безопасная демонстрация: --demo."""
from pathlib import Path
from datetime import date, datetime, timezone
import argparse
import calendar
import json
import time
import uuid
import math
import pandas as pd
import yaml
from sqlalchemy import text
from src.database import get_engine

ROOT = Path(__file__).resolve().parents[1]


class QualityGateError(RuntimeError):
    pass


def enforce_quality(summary):
    if summary['status'] in ('FAIL','ERROR','RUNNING'):
        raise QualityGateError(f"Контроль качества {summary['status']}; прогноз заблокирован. run_id={summary['run_id']}")


def settings():
    project=yaml.safe_load((ROOT/'config/project.yaml').read_text(encoding='utf-8-sig'))
    policy=yaml.safe_load((ROOT/'config/quality.yaml').read_text(encoding='utf-8-sig'))
    airports=[a['code'] for a in project['airports']]
    if not airports or len(airports)!=len(set(airports)) or any(not isinstance(a,str) or len(a)!=3 for a in airports):
        raise ValueError('В project.yaml нужны уникальные трёхбуквенные коды аэропортов.')
    bts=project['bts'];weather=project['weather']
    start=date.fromisoformat(str(bts['default_start_month'])+'-01')
    end_month=date.fromisoformat(str(bts['target_end_month'])+'-01')
    end=end_month.replace(day=calendar.monthrange(end_month.year,end_month.month)[1])
    ws=date.fromisoformat(str(weather['default_start_date']));we=date.fromisoformat(str(weather['target_end_date']))
    if start>end or ws>we:raise ValueError('Неверные периоды project.yaml.')
    if not ws<=start<=end<=we:raise ValueError('Погодный период должен покрывать период рейсов.')
    timeout=policy['statement_timeout_seconds']
    if type(timeout) is not int or not 1<=timeout<=3600:raise ValueError('Неверный statement_timeout_seconds.')
    for k in ['share_tolerance','weather_tolerance']:
        value=policy[k]
        if not isinstance(value,(int,float)) or not math.isfinite(value) or value<0:raise ValueError('Неверный допуск: '+k)
    return project,policy,dict(airports=airports,start=start,end=end,weather_start=ws,weather_end=we,
                               share_tolerance=policy['share_tolerance'],weather_tolerance=policy['weather_tolerance'])


# Все relation names заданы кодом, а пользовательские значения передаются параметрами.
def definitions(demo=False):
    g='pg_temp.quality_gold_demo' if demo else 'gold.airport_daily'
    d='silver.dim_airport'
    rows=[]
    def add(key,name,obj,category,sql,detail):rows.append(dict(check_id=key,check_name=name,object_name=obj,category=category,sql=sql,description=detail))
    schema="""WITH expected(table_schema,table_name,column_name,types) AS (VALUES
      ('gold','airport_daily','airport_id',ARRAY['integer']),
      ('gold','airport_daily','flight_date',ARRAY['date']),
      ('gold','airport_daily','total_flights',ARRAY['integer','bigint']),
      ('gold','airport_daily','completed_flights',ARRAY['integer','bigint']),
      ('gold','airport_daily','cancelled_flights',ARRAY['integer','bigint']),
      ('gold','airport_daily','diverted_flights',ARRAY['integer','bigint']),
      ('gold','airport_daily','delayed_flights',ARRAY['integer','bigint']),
      ('gold','airport_daily','delay_share',ARRAY['double precision','numeric','real']),
      ('gold','airport_daily','avg_temperature',ARRAY['double precision','numeric','real']),
      ('gold','airport_daily','total_precipitation',ARRAY['double precision','numeric','real']),
      ('gold','airport_daily','avg_wind_speed',ARRAY['double precision','numeric','real']),
      ('gold','airport_daily','max_wind_gust',ARRAY['double precision','numeric','real']),
      ('bronze','bts_flights_raw','flight_date',ARRAY['date']),
      ('bronze','bts_flights_raw','origin',ARRAY['text','character varying']),
      ('bronze','weather_hourly_raw','weather_time',ARRAY['timestamp without time zone']),
      ('bronze','weather_hourly_raw','airport_code',ARRAY['text','character varying']),
      ('silver','flights','origin_airport_id',ARRAY['integer']),
      ('silver','flights','airline_id',ARRAY['integer']),
      ('silver','weather_hourly','weather_time',ARRAY['timestamp without time zone']),
      ('silver','weather_hourly','airport_id',ARRAY['integer']),
      ('silver','weather_hourly','temperature_2m',ARRAY['double precision','numeric','real']),
      ('silver','weather_hourly','precipitation',ARRAY['double precision','numeric','real']),
      ('silver','weather_hourly','wind_speed_10m',ARRAY['double precision','numeric','real']),
      ('silver','weather_hourly','wind_gusts_10m',ARRAY['double precision','numeric','real']),
      ('silver','dim_airport','airport_id',ARRAY['integer']),
      ('silver','dim_airport','airport_code',ARRAY['text','character varying']),
      ('silver','dim_airline','airline_id',ARRAY['integer']))
      SELECT COUNT(*) FROM expected e LEFT JOIN information_schema.columns c
      ON (c.table_schema,c.table_name,c.column_name)=(e.table_schema,e.table_name,e.column_name)
      WHERE c.column_name IS NULL OR NOT(c.data_type=ANY(e.types))"""
    add('schema_types','Наличие столбцов и типы','Gold/Bronze/Silver','types',schema,'Количество отсутствующих столбцов или несовместимых типов.')
    add('gold_not_empty','Gold не пуст','gold.airport_daily','completeness',f'SELECT CASE WHEN EXISTS(SELECT 1 FROM {g}) THEN 0 ELSE 1 END','Пустая витрина: 1 нарушение.')
    add('gold_unique','Уникальность аэропорт–дата','gold.airport_daily','uniqueness',f'SELECT COALESCE(SUM(n-1),0) FROM (SELECT COUNT(*) n FROM {g} GROUP BY airport_id,flight_date HAVING COUNT(*)>1) s','Число лишних строк с повторяющимся ключом.')
    add('gold_required','Обязательные поля','gold.airport_daily','completeness',f'''SELECT COUNT(*) FROM {g} WHERE airport_id IS NULL OR flight_date IS NULL OR total_flights IS NULL OR completed_flights IS NULL OR cancelled_flights IS NULL OR diverted_flights IS NULL OR delayed_flights IS NULL OR delay_share IS NULL''','Количество строк с пропуском ключа или показателя.')
    add('gold_ranges','Диапазоны показателей','gold.airport_daily','ranges',f'''SELECT COUNT(*) FROM {g} WHERE total_flights<0 OR completed_flights<=0 OR cancelled_flights<0 OR diverted_flights<0 OR delayed_flights<0 OR NOT (delay_share>=0 AND delay_share<=100)''','Неположительный знаменатель, отрицательные количества или доля вне 0–100; NULL проверяется отдельно.')
    add('gold_consistency','Согласованность количеств','gold.airport_daily','consistency',f'''SELECT COUNT(*) FROM {g} WHERE total_flights<>completed_flights+cancelled_flights OR delayed_flights>completed_flights OR diverted_flights>total_flights''','Перенаправленные рейсы могут входить в неотменённые: их не прибавляем повторно.')
    add('delay_formula','Формула доли задержек','gold.airport_daily','consistency',f'''SELECT COUNT(*) FROM {g} WHERE completed_flights>0 AND (delay_share IS NULL OR NOT(ABS(delay_share-100.0*delayed_flights/NULLIF(completed_flights,0))<=:share_tolerance))''','Проверка формулы в процентах с допустимой погрешностью.')
    add('gold_airport_fk','Аэропорты Gold в справочнике','gold.airport_daily','referential',f'''SELECT COUNT(*) FROM {g} g LEFT JOIN {d} a USING(airport_id) WHERE a.airport_id IS NULL''','Количество строк без справочника аэропорта.')
    expected="""WITH expected AS (SELECT airport_code,day::date flight_date FROM unnest(CAST(:airports AS text[])) AS a(airport_code) CROSS JOIN generate_series(CAST(:start AS date),CAST(:end AS date),INTERVAL '1 day') day)"""
    add('gold_calendar','Полнота и актуальность Gold по конфигурации','gold.airport_daily','coverage',expected+f''' SELECT COUNT(*) FROM expected e WHERE NOT EXISTS(SELECT 1 FROM {g} g JOIN {d} a USING(airport_id) WHERE a.airport_code=e.airport_code AND g.flight_date=e.flight_date)''','Количество отсутствующих пар аэропорт–дата; даты после настроенного периода допускаются.')
    add('bts_calendar','Покрытие BTS по конфигурации','bronze.bts_flights_raw','freshness',expected+''' SELECT COUNT(*) FROM expected e LEFT JOIN (SELECT DISTINCT origin,flight_date FROM bronze.bts_flights_raw WHERE flight_date BETWEEN :start AND :end AND origin=ANY(CAST(:airports AS text[]))) b ON b.origin=e.airport_code AND b.flight_date=e.flight_date WHERE b.flight_date IS NULL''','Проверяется наличие рейсов каждый день, а не полнота внешнего архива BTS до отдельного рейса.')
    wexpected="""WITH expected AS (SELECT airport_code,day::date weather_date FROM unnest(CAST(:airports AS text[])) AS a(airport_code) CROSS JOIN generate_series(CAST(:weather_start AS date),CAST(:weather_end AS date),INTERVAL '1 day') day)"""
    add('weather_calendar','Покрытие погоды по конфигурации','bronze.weather_hourly_raw','freshness',wexpected+''' SELECT COUNT(*) FROM expected e LEFT JOIN (SELECT DISTINCT airport_code,weather_time::date weather_date FROM bronze.weather_hourly_raw WHERE weather_time>=CAST(:weather_start AS date) AND weather_time<CAST(:weather_end AS date)+1 AND airport_code=ANY(CAST(:airports AS text[]))) w USING(airport_code,weather_date) WHERE w.weather_date IS NULL''','Нет дня полностью без погоды; наличие всех 24 часов эта проверка не гарантирует.')
    add('silver_flight_fk','Ссылки рейсов Silver','silver.flights','referential','''SELECT COUNT(*) FROM silver.flights f LEFT JOIN silver.dim_airport a ON a.airport_id=f.origin_airport_id LEFT JOIN silver.dim_airline l ON l.airline_id=f.airline_id WHERE a.airport_id IS NULL OR l.airline_id IS NULL''','Количество рейсов без аэропорта или авиакомпании в справочнике.')
    add('silver_weather_unique','Уникальность почасовой погоды','silver.weather_hourly','uniqueness','''SELECT COALESCE(SUM(n-1),0) FROM (SELECT COUNT(*) n FROM silver.weather_hourly GROUP BY airport_id,weather_time HAVING COUNT(*)>1) s''','Число лишних записей по аэропорту и времени.')
    add('gold_weather_complete','Полнота погодных признаков','gold.airport_daily','completeness',f'''SELECT COUNT(*) FROM {g} WHERE avg_temperature IS NULL OR total_precipitation IS NULL OR avg_wind_speed IS NULL OR max_wind_gust IS NULL OR NOT(avg_temperature > '-Infinity'::float8 AND avg_temperature < 'Infinity'::float8) OR NOT(total_precipitation>=0 AND total_precipitation<'Infinity'::float8) OR NOT(avg_wind_speed>=0 AND avg_wind_speed<'Infinity'::float8) OR NOT(max_wind_gust>=0 AND max_wind_gust<'Infinity'::float8)''','Пропуски, бесконечности, NaN или отрицательные осадки/ветер. По умолчанию предупреждение: CatBoost допускает NaN в числовой погоде.')
    add('gold_weather_reconcile','Погодные агрегаты Gold согласованы с Silver','gold.airport_daily','consistency',f'''WITH w AS (SELECT airport_id,weather_time::date flight_date,AVG(temperature_2m) t,SUM(precipitation) p,AVG(wind_speed_10m) v,MAX(wind_gusts_10m) m FROM silver.weather_hourly GROUP BY airport_id,weather_time::date) SELECT COUNT(*) FROM {g} g LEFT JOIN w USING(airport_id,flight_date) WHERE w.airport_id IS NULL OR (g.avg_temperature IS NULL)<>(w.t IS NULL) OR (g.total_precipitation IS NULL)<>(w.p IS NULL) OR (g.avg_wind_speed IS NULL)<>(w.v IS NULL) OR (g.max_wind_gust IS NULL)<>(w.m IS NULL) OR NOT(ABS(g.avg_temperature-w.t)<=:weather_tolerance) OR NOT(ABS(g.total_precipitation-w.p)<=:weather_tolerance) OR NOT(ABS(g.avg_wind_speed-w.v)<=:weather_tolerance) OR NOT(ABS(g.max_wind_gust-w.m)<=:weather_tolerance)''','Сверка суточного объединения и агрегатов; не доказывает правильность временной зоны исходного API.')
    return rows


def result_status(violations,rule):
    if violations<=rule['max_violations']:return 'PASS'
    return 'FAIL' if rule['severity']=='critical' else 'WARN'


def make_summary(run_id,results,is_demo):
    counts={s:sum(r['status']==s for r in results) for s in ['PASS','WARN','FAIL','ERROR']}
    status=next((s for s in ['ERROR','FAIL','WARN'] if counts[s]),'PASS')
    return dict(run_id=run_id,status=status,is_demo=is_demo,total_checks=len(results),passed_checks=counts['PASS'],warning_checks=counts['WARN'],failed_checks=counts['FAIL'],error_checks=counts['ERROR'])


def validate_rules(policy,checks):
    if set(policy['checks'])!={r['check_id'] for r in checks}:raise ValueError('Список checks в quality.yaml не совпадает с модулем.')
    for key,r in policy['checks'].items():
        if r['severity'] not in ['critical','warning'] or type(r['max_violations']) is not int or r['max_violations']<0:raise ValueError('Некорректное правило '+key)


def run_quality(engine=None,source='manual',demo=False):
    engine=engine or get_engine()
    project,policy,params=settings();checks=definitions(demo);validate_rules(policy,checks)
    run_id=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')+'-'+uuid.uuid4().hex[:12]
    snapshot=json.dumps({'project':project,'quality':policy},ensure_ascii=False,default=str)
    with engine.begin() as c:
        c.execute(text('INSERT INTO meta.quality_runs(run_id,status,is_demo,trigger_source,config_snapshot) VALUES (:r,\'RUNNING\',:d,:s,CAST(:p AS jsonb))'),dict(r=run_id,d=demo,s=source,p=snapshot))
    results=[]
    try:
        with engine.connect().execution_options(isolation_level='REPEATABLE READ') as c:
            with c.begin():
                if not demo:c.exec_driver_sql('SET TRANSACTION READ ONLY')
                c.execute(text("SELECT set_config('statement_timeout',:v,true)"),{'v':str(policy['statement_timeout_seconds']*1000)})
                if demo:prepare_demo(c,params)
                for check in checks:
                    rule=policy['checks'][check['check_id']];start=time.perf_counter()
                    r={k:check[k] for k in ['check_id','check_name','object_name','category']}
                    r.update(severity=rule['severity'],allowed_violations=rule['max_violations'])
                    try:
                        # Ошибка одной проверки не ломает выполнение остальных SQL.
                        with c.begin_nested():v=int(c.execute(text(check['sql']),params).scalar_one())
                        r.update(violations=v,status=result_status(v,rule),details=check['description'])
                    except Exception as exc:
                        # Не сохраняем текст исключения с SQL-параметрами/реквизитами подключения.
                        r.update(violations=None,status='ERROR',details='Не выполнен SQL: '+type(exc).__name__+'. Проверьте схему и доступность таблиц.')
                    r['duration_ms']=round((time.perf_counter()-start)*1000,3);results.append(r)
    except Exception as exc:
        results.append(dict(check_id='execution_error',check_name='Ошибка запуска проверок',object_name='quality',category='execution',severity='critical',allowed_violations=0,violations=None,status='ERROR',duration_ms=0.0,details='Ошибка выполнения: '+type(exc).__name__))
    summary=make_summary(run_id,results,demo)
    # Запись отдельной транзакцией: FAIL не откатывает журнал проверок.
    with engine.begin() as c:
        sql=text('''INSERT INTO meta.quality_results(run_id,check_id,check_name,object_name,category,severity,status,violations,allowed_violations,duration_ms,details) VALUES (:run_id,:check_id,:check_name,:object_name,:category,:severity,:status,:violations,:allowed_violations,:duration_ms,:details)''')
        c.execute(sql,[dict(run_id=run_id,**r) for r in results])
        c.execute(text('''UPDATE meta.quality_runs SET finished_at=NOW(),status=:status,total_checks=:total_checks,passed_checks=:passed_checks,warning_checks=:warning_checks,failed_checks=:failed_checks,error_checks=:error_checks WHERE run_id=:run_id'''),{k:v for k,v in summary.items() if k!='is_demo'})
    out=ROOT/'reports/quality'/run_id;out.mkdir(parents=True,exist_ok=False)
    pd.DataFrame(results).to_csv(out/'quality_results.csv',index=False,encoding='utf-8-sig')
    (out/'summary.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2),encoding='utf-8')
    print(pd.DataFrame(results)[['check_id','severity','status','violations']].to_string(index=False))
    print('Итог:',summary['status'],'| run_id:',run_id,'| демонстрация:',demo)
    print('Файлы:',out)
    return summary


def assert_quality(engine=None,source='forecast'):
    summary=run_quality(engine=engine,source=source)
    enforce_quality(summary)
    return summary


def prepare_demo(c,params):
    # TEMP CTAS не копирует ограничения и не меняет постоянную Gold.
    c.exec_driver_sql('CREATE TEMP TABLE quality_gold_demo ON COMMIT DROP AS SELECT * FROM gold.airport_daily')
    query='''SELECT g.airport_id FROM gold.airport_daily g JOIN silver.dim_airport a USING(airport_id) WHERE a.airport_code=:a AND g.flight_date BETWEEN :start AND :end GROUP BY g.airport_id HAVING COUNT(*)>=7 LIMIT 1'''
    aid=c.execute(text(query),dict(a=params['airports'][0],start=params['start'],end=params['end'])).scalar_one_or_none()
    if aid is None:raise ValueError('Для демонстрации нужно не менее семи суточных строк одного аэропорта.')
    days=c.execute(text('SELECT flight_date FROM pg_temp.quality_gold_demo WHERE airport_id=:a AND flight_date BETWEEN :start AND :end ORDER BY flight_date LIMIT 7'),dict(a=aid,start=params['start'],end=params['end'])).scalars().all()
    c.execute(text('INSERT INTO pg_temp.quality_gold_demo SELECT * FROM pg_temp.quality_gold_demo WHERE airport_id=:a AND flight_date=:d'),dict(a=aid,d=days[0]))
    c.execute(text('DELETE FROM pg_temp.quality_gold_demo WHERE airport_id=:a AND flight_date=:d'),dict(a=aid,d=days[1]))
    assignments=['delay_share=150','total_flights=NULL','completed_flights=-1','airport_id=-1','avg_temperature=NULL']
    for day,assignment in zip(days[2:],assignments):
        c.execute(text(f'UPDATE pg_temp.quality_gold_demo SET {assignment} WHERE airport_id=:a AND flight_date=:d'),dict(a=aid,d=day))


def latest(engine):
    with engine.connect() as c:
        for title,sql in [('Последние запуски (включая demo)','SELECT run_id,started_at,status,is_demo,total_checks,failed_checks,warning_checks,error_checks FROM meta.quality_runs ORDER BY started_at DESC LIMIT 5'),('Последние рабочие проверки','SELECT check_id,severity,status,violations,details FROM gold.quality_latest_bi ORDER BY check_id')]:
            print('\n'+title);print(pd.read_sql(text(sql),c).to_string(index=False))


def main():
    p=argparse.ArgumentParser(description=__doc__);group=p.add_mutually_exclusive_group()
    group.add_argument('--demo',action='store_true');group.add_argument('--latest',action='store_true')
    args=p.parse_args()
    try:
        engine=get_engine()
        if args.latest:latest(engine);return 0
        summary=run_quality(engine=engine,source='demo' if args.demo else 'manual',demo=args.demo)
        if args.demo:
            if summary['status']!='FAIL':raise RuntimeError('Демонстрация не дала ожидаемый FAIL; проверьте результаты.')
            try:enforce_quality(summary)
            except QualityGateError:print('DEMO OK: критическая ошибка обнаружена; барьер блокирует прогноз. Рабочая Gold не изменялась.');return 0
            raise RuntimeError('Барьер не сработал.')
        enforce_quality(summary);return 0
    except QualityGateError as exc:print(str(exc));return 2
    except Exception as exc:
        print('Контроль качества остановлен:',type(exc).__name__)
        if isinstance(exc,(ValueError,FileNotFoundError)):print(str(exc))
        else:print('Проверьте Docker, .env, миграцию 011 и журнал --latest. Прогноз не должен продолжаться после ошибки.')
        return 1


if __name__=='__main__':raise SystemExit(main())
