"""Проверка и атомарный импорт результатов уже выполненной тетради."""
from pathlib import Path
import hashlib
import json
import numpy as np
import pandas as pd
from sqlalchemy import text
from .core import (TARGETS, config, require, load_manifest, validate_history,
                   sha_file, airport_ids, insert_rows)

FILES = ['experiment.json','gold_snapshot.csv','validation_metrics.csv','test_metrics.csv',
         'backtest_predictions.csv','next_day_predictions.csv']
MODELS = {'seasonal_naive_7','catboost'}


def load_run(run_dir):
    run_dir = Path(run_dir)
    for name in FILES:
        require((run_dir/name).is_file(),f'Не найден файл {run_dir/name}. Завершите тетрадь.')
    manifest = load_manifest(run_dir)
    require(sha_file(run_dir/'gold_snapshot.csv') == manifest['snapshot_sha256'], 'Снимок Gold изменён после эксперимента.')
    history = validate_history(pd.read_csv(run_dir/'gold_snapshot.csv'),config()['airports'])
    require(history.flight_date.max() == pd.Timestamp(manifest['test_end']), 'Конец снимка не совпадает с тестом.')
    require(pd.Timestamp(manifest['train_end']) < pd.Timestamp(manifest['validation_end']) < pd.Timestamp(manifest['test_end']), 'Неверные границы выборок.')
    predictions = pd.read_csv(run_dir/'backtest_predictions.csv',parse_dates=['flight_date','history_through'])
    metrics = pd.concat([pd.read_csv(run_dir/'validation_metrics.csv'),pd.read_csv(run_dir/'test_metrics.csv')],ignore_index=True)
    future = pd.read_csv(run_dir/'next_day_predictions.csv',parse_dates=['flight_date','history_through'])
    require(set(predictions.split) == {'validation','test'}, 'Неполные выборки прогнозов.')
    require(set(metrics.split) == {'validation','test'}, 'Неполные метрики.')
    require(not predictions.duplicated(['airport_code','flight_date','target','model','split']).any(), 'Дубли backtest.')
    require(not metrics.duplicated(['split','target','model','airport_code']).any(), 'Дубли метрик.')
    require(not future.duplicated(['airport_code','flight_date','target','model']).any(),'Дубли следующего прогноза.')
    for df in [predictions,future]:
        require(set(df.target) == set(TARGETS) and set(df.model) == MODELS, 'Неполный набор целей или моделей.')
        require(set(df.airport_code) == set(config()['airports']), 'Неполный набор аэропортов.')
        require((df.flight_date == df.history_through + pd.Timedelta(days=1)).all(),'Горизонт должен быть один день.')
        require(np.isfinite(df.prediction).all() and (df.prediction >= 0).all(),'Некорректный прогноз.')
        require(df.loc[df.target == 'delay_share','prediction'].between(0,100).all(),'Доля прогноза вне диапазона.')
    require(np.isfinite(predictions.actual).all() and (predictions.actual >= 0).all(),'Некорректный факт.')
    recomputed = []
    for split,left,right in [('validation',manifest['train_end'],manifest['validation_end']),('test',manifest['validation_end'],manifest['test_end'])]:
        expected = history.loc[history.flight_date.between(pd.Timestamp(left)+pd.Timedelta(days=1),pd.Timestamp(right))]
        for target in TARGETS:
            for model in sorted(MODELS):
                part = predictions.loc[(predictions.split==split)&(predictions.target==target)&(predictions.model==model)]
                merged = expected[['airport_code','flight_date',target]].merge(part,on=['airport_code','flight_date'],how='outer',indicator=True,validate='one_to_one')
                require((merged['_merge']=='both').all(),f'Неполные даты {split}/{target}/{model}.')
                require(np.allclose(merged[target],merged.actual,atol=1e-8), 'Факт backtest не совпадает со снимком.')
                # Сезонный baseline обязан совпадать со значением t-7.
                if model == 'seasonal_naive_7':
                    lag = history[['airport_code','flight_date',target]].copy()
                    lag['flight_date'] += pd.Timedelta(days=7)
                    match = part.merge(lag,on=['airport_code','flight_date'],validate='one_to_one')
                    require(len(match)==len(part) and np.allclose(match.prediction,match[target]),'Baseline не равен t-7.')
                for airport,group in [('ALL',part)]+list(part.groupby('airport_code')):
                    err = group.actual-group.prediction
                    recomputed.append(dict(split=split,target=target,model=model,airport_code=airport,n=len(group),MAE=float(np.abs(err).mean()),RMSE=float(np.sqrt((err**2).mean()))))
    recomputed = pd.DataFrame(recomputed)
    keys = ['split','target','model','airport_code']
    check = recomputed.merge(metrics,on=keys,how='outer',suffixes=('_new','_file'),indicator=True,validate='one_to_one')
    require((check['_merge']=='both').all(),'Неполная таблица метрик.')
    for col in ['n','MAE','RMSE']:
        require(np.allclose(check[col+'_new'],check[col+'_file'],atol=1e-7),f'Не совпадают метрики {col}.')
    for target in TARGETS:
        val = recomputed.query("split == 'validation' and airport_code == 'ALL' and target == @target").sort_values(['MAE','model'])
        require(val.iloc[0].model == manifest['chosen_model_by_validation'][target], 'Выбор модели не подтверждён валидацией.')
    require(len(future)==20,'Ожидаются 20 будущих прогнозов: 5 аэропортов × 2 цели × 2 модели.')
    require((future.history_through == history.flight_date.max()).all(),'Неверная отсечка будущего прогноза.')
    expected_selected = future.apply(lambda r:r.model==manifest['chosen_model_by_validation'][r.target],axis=1)
    require(future.selected_for_use.equals(expected_selected),'Неверный флаг выбранной модели.')
    for target in TARGETS:
        base = future.loc[(future.target==target)&(future.model=='seasonal_naive_7')]
        ref = history.loc[history.flight_date == history.flight_date.max()-pd.Timedelta(days=6)]
        match = base.merge(ref[['airport_code',target]],on='airport_code',validate='one_to_one')
        require(len(match)==5 and np.allclose(match.prediction,match[target]),'Будущий baseline не равен t-7.')
    digest = hashlib.sha256(''.join(name+sha_file(run_dir/name) for name in FILES).encode()).hexdigest()
    return manifest,history,predictions,recomputed,future,digest


def import_run(engine,run_dir):
    manifest,history,pred,metrics,future,digest = load_run(run_dir)
    run_id = manifest['run_id']
    with engine.begin() as c:
        c.execute(text('SELECT pg_advisory_xact_lock(hashtext(:key))'),{'key':'forecast-import-'+run_id})
        old = c.execute(text('SELECT source_digest FROM meta.forecast_experiments WHERE run_id=:r'),{'r':run_id}).scalar_one_or_none()
        if old:
            require(old==digest,'Этот run_id уже импортирован с другим содержимым. Исходные результаты не перезаписываются.')
            print('Эксперимент уже импортирован; изменений нет:',run_id)
            return run_id
        ids = airport_ids(c)
        require(set(config()['airports']).issubset(ids),'Нет аэропортов в справочнике Silver.')
        c.execute(text('INSERT INTO meta.forecast_experiments(run_id,source_digest,snapshot_sha256,manifest) VALUES (:r,:d,:s,CAST(:m AS jsonb))'),{'r':run_id,'d':digest,'s':manifest['snapshot_sha256'],'m':json.dumps(manifest)})
        rows=[]
        for r in metrics.to_dict('records'):
            r['n']=int(r['n']);r['mae']=r.pop('MAE');r['rmse']=r.pop('RMSE');r['run_id']=run_id
            r['selected_for_use']=r['model']==manifest['chosen_model_by_validation'][r['target']]
            rows.append(r)
        insert_rows(c,'gold.forecast_metrics',rows)
        rows=[]
        for r in pred.to_dict('records'):
            r['airport_id']=ids[r.pop('airport_code')];r['run_id']=run_id
            r['flight_date']=r['flight_date'].date();r['history_through']=r['history_through'].date()
            r['selected_for_use']=r['model']==manifest['chosen_model_by_validation'][r['target']]
            rows.append(r)
        insert_rows(c,'gold.forecast_backtest',rows)
        # Прогноз тетради импортируется отдельным неизменяемым пакетом.
        batch='notebook-'+run_id
        c.execute(text('INSERT INTO meta.forecast_batches(batch_id,experiment_run_id,history_through,history_sha256,model_sha256) VALUES (:b,:r,:h,:s,:m)'),{'b':batch,'r':run_id,'h':history.flight_date.max().date(),'s':manifest['snapshot_sha256'],'m':'notebook-results:'+digest})
        rows=[]
        for r in future.to_dict('records'):
            r['airport_id']=ids[r.pop('airport_code')];r['batch_id']=batch
            r['flight_date']=r['flight_date'].date();r['history_through']=r['history_through'].date()
            rows.append(r)
        insert_rows(c,'gold.airport_forecasts',rows)
    print(f'Импортирован {run_id}: метрик {len(metrics)}, backtest {len(pred)}, прогнозов {len(future)}.')
    return run_id
