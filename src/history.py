"""История Gold: python -m src.history check | demo. Привязка изменений к pipeline через контекст."""
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path
import argparse
import json
from datetime import datetime,timezone
import pandas as pd
from sqlalchemy import text
from src.database import get_engine

ROOT=Path(__file__).resolve().parents[1]
ORIGIN=ContextVar('aviation_history_origin',default=None)


@contextmanager
def history_context(run_id,step_id,source):
    token=ORIGIN.set({'run_id':run_id,'step_id':step_id,'source':source})
    try:yield
    finally:ORIGIN.reset(token)


def bind_history_context(connection,filename):
    origin=ORIGIN.get() or {}
    # transaction-local: не загрязняет следующие соединения из пула.
    values={'run':origin.get('run_id') or '', 'step':origin.get('step_id') or '',
            'source':origin.get('source') or 'transformations/'+str(filename)}
    connection.execute(text("""SELECT set_config('aviation.pipeline_run_id',:run,true),
      set_config('aviation.pipeline_step_id',:step,true),set_config('aviation.change_source',:source,true)"""),values)


def check(engine=None):
    engine=engine or get_engine()
    queries={
        'Объёмы':'''SELECT (SELECT COUNT(*) FROM gold.airport_daily) gold_rows,
          COUNT(*) history_versions,COUNT(*) FILTER(WHERE valid_to IS NULL AND operation<>'DELETE') current_live,
          COUNT(*) FILTER(WHERE valid_to IS NULL AND operation='DELETE') current_deleted,
          COUNT(*) FILTER(WHERE pipeline_run_id IS NOT NULL) linked_versions FROM gold.airport_daily_history''',
        'Число версий по операциям':'SELECT operation,COUNT(*) versions FROM gold.airport_daily_history GROUP BY operation ORDER BY operation',
        'Несовпадение актуальной истории и Gold (ожидается 0)':'''SELECT COUNT(*) AS mismatches FROM gold.airport_daily g
          FULL JOIN (SELECT * FROM gold.airport_daily_history WHERE valid_to IS NULL AND operation<>'DELETE') h
          ON g.airport_id=h.airport_id AND g.flight_date=h.flight_date
          WHERE g.airport_id IS NULL OR h.airport_id IS NULL OR meta.airport_daily_comparison_payload(to_jsonb(g)) IS DISTINCT FROM meta.airport_daily_comparison_payload(h.snapshot)''',
        'Разрывы версий или периодов (ожидается 0)':'''WITH v AS(SELECT airport_id,flight_date,version_no,valid_from,valid_to,
          ROW_NUMBER() OVER(PARTITION BY airport_id,flight_date ORDER BY version_no) rn,
          LEAD(valid_from) OVER(PARTITION BY airport_id,flight_date ORDER BY version_no) next_start
          FROM gold.airport_daily_history)
          SELECT COUNT(*) AS bad_versions FROM v WHERE version_no<>rn OR valid_to IS DISTINCT FROM next_start''',
        'Некорректная связь шага и запуска (ожидается 0)':'''SELECT COUNT(*) AS bad_links FROM gold.airport_daily_history h
          LEFT JOIN meta.pipeline_steps s ON s.step_id=h.pipeline_step_id
          WHERE h.pipeline_step_id IS NOT NULL AND (s.step_id IS NULL OR s.run_id IS DISTINCT FROM h.pipeline_run_id)'''}
    result={}
    with engine.connect() as c:
        for title,sql in queries.items():
            df=pd.read_sql(text(sql),c);result[title]=df.to_dict('records');print('\n'+title);print(df.to_string(index=False))
    bad=sum(int(result[k][0][next(iter(result[k][0]))]) for k in list(queries)[2:])
    if bad:raise ValueError('Обнаружены нарушения истории; проверьте результаты.')
    return result


class DemoRollback(Exception):pass


def _demo_tables(engine):
    if ORIGIN.get() is None:raise ValueError('Сначала установите history hooks для PipelineRun.step.')
    events=[]
    with engine.begin() as c:
        bind_history_context(c,'temporary history demo')
        c.exec_driver_sql('''CREATE TEMP TABLE airport_daily_history_demo (LIKE gold.airport_daily_history INCLUDING ALL) ON COMMIT DROP;
          CREATE TEMP TABLE history_gold_demo ON COMMIT DROP AS SELECT * FROM gold.airport_daily ORDER BY airport_id,flight_date LIMIT 1;''')
        row=c.execute(text('SELECT airport_id,flight_date FROM pg_temp.history_gold_demo')).mappings().first()
        if row is None:raise ValueError('Для демонстрации нужна хотя бы одна строка Gold.')
        key=dict(row)
        c.exec_driver_sql("""SELECT meta.append_airport_daily_version('pg_temp.airport_daily_history_demo'::regclass,airport_id,flight_date,to_jsonb(g)-'updated_at','BASELINE') FROM pg_temp.history_gold_demo g;
          CREATE TRIGGER demo_history AFTER INSERT OR UPDATE OR DELETE ON pg_temp.history_gold_demo
          FOR EACH ROW EXECUTE FUNCTION meta.capture_airport_daily_history('pg_temp.airport_daily_history_demo');""")
        def record(stage,expected):
            n=int(c.execute(text('SELECT COUNT(*) FROM pg_temp.airport_daily_history_demo')).scalar_one())
            if n!=expected:raise AssertionError(f'{stage}: ожидалось {expected}, получено {n}')
            events.append({'stage':stage,'versions':n,'expected':expected});print(stage,': версий',n)
        record('Исходная версия',1)
        c.exec_driver_sql('UPDATE pg_temp.history_gold_demo SET updated_at=clock_timestamp()');record('Только updated_at',1)
        change='''UPDATE pg_temp.history_gold_demo SET total_flights=total_flights+1,
          completed_flights=completed_flights+1,delay_share=100.0*delayed_flights/(completed_flights+1)'''
        c.exec_driver_sql(change);record('Реальное изменение показателей',2)
        c.exec_driver_sql('UPDATE pg_temp.history_gold_demo SET total_flights=total_flights,updated_at=clock_timestamp()');record('Повтор тех же значений',2)
        try:
            with c.begin_nested():
                c.exec_driver_sql(change)
                raise DemoRollback()
        except DemoRollback:pass
        record('Откат изменения и его версии',2)
        c.exec_driver_sql('DELETE FROM pg_temp.history_gold_demo');record('Удаление строки',3)
        c.execute(text('''INSERT INTO pg_temp.history_gold_demo SELECT * FROM gold.airport_daily WHERE airport_id=:airport_id AND flight_date=:flight_date'''),key)
        record('Повторное появление строки',4)
        versions=pd.read_sql(text("""SELECT version_no,operation,snapshot->>'total_flights' total_flights,
          valid_from,valid_to,(valid_to IS NULL) is_current,pipeline_run_id,pipeline_step_id FROM pg_temp.airport_daily_history_demo ORDER BY version_no"""),c)
        if versions.pipeline_run_id.isna().any() or versions.pipeline_step_id.isna().any():raise AssertionError('Не сохранилась связь с демонстрационным запуском.')
        print(versions.to_string(index=False))
    out=ROOT/'reports/history_demo'/datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ');out.mkdir(parents=True,exist_ok=False)
    pd.DataFrame(events).to_csv(out/'demo_checks.csv',index=False,encoding='utf-8-sig')
    versions.to_csv(out/'demo_versions.csv',index=False,encoding='utf-8-sig')
    print('DEMO OK: 1 → 1 → 2 → 2 → 2 → 3 → 4. Рабочая Gold и её история не изменялись.')
    print('Файлы:',out)
    return events


def demo(engine=None):
    engine=engine or get_engine()
    from src.metadata import sync
    from src.pipeline_run import PipelineRun
    cat,version=sync(engine)
    with PipelineRun(engine,cat,version,'history_demo',is_demo=True) as run:
        return run.step('history_capture',_demo_tables,engine)


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('command',choices=['check','demo']);args=p.parse_args()
    try:
        if args.command=='check':check()
        else:demo()
        return 0
    except Exception as exc:
        print('Операция остановлена:',type(exc).__name__)
        if isinstance(exc,(ValueError,FileNotFoundError,AssertionError)):print(str(exc))
        else:print('Проверьте миграции 012/013 и подключение PostgreSQL; реквизиты не выводятся.')
        return 1


if __name__ == "__main__":
    from src.history import main as history_main

    raise SystemExit(history_main())