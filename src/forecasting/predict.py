"""Прогноз из сохранённых моделей и отдельное явное переобучение."""
from src.quality import assert_quality
from pathlib import Path
from datetime import datetime,timezone
import hashlib
import json
import numpy as np
import pandas as pd
from catboost import CatBoostRegressor
from sqlalchemy import text
from .core import (ROOT,TARGETS,COLUMNS,require,config,load_manifest,sha_file,
                   validate_history,read_history,history_digest,make_features,bound,airport_ids,insert_rows)


def bundle(run_dir,model_dir):
    manifest=load_manifest(run_dir)
    model_dir=Path(model_dir) if model_dir else ROOT/'models/forecasting'/manifest['run_id']
    require(model_dir.is_dir(),f'Нет каталога моделей: {model_dir}')
    if (model_dir/'bundle.json').is_file():
        info=json.loads((model_dir/'bundle.json').read_text(encoding='utf-8'))
        require(info['experiment_run_id']==manifest['run_id'],'Модели относятся к другому эксперименту.')
        require(info['features']==manifest['features'],'Изменился порядок признаков.')
        snapshot=model_dir/'training_snapshot.csv'
        require(sha_file(snapshot)==info['snapshot_file_sha256'],'Изменён снимок переобучения.')
        history=validate_history(pd.read_csv(snapshot),config()['airports'])
        for target in TARGETS:
            require(sha_file(model_dir/f'{target}_full_history.cbm')==info['model_sha256'][target], 'Изменён файл модели.')
    else:
        snapshot=Path(run_dir)/'gold_snapshot.csv'
        require(sha_file(snapshot)==manifest['snapshot_sha256'],'Изменён снимок эксперимента.')
        history=validate_history(pd.read_csv(snapshot),config()['airports'])
    paths={t:model_dir/f'{t}_full_history.cbm' for t in TARGETS}
    require(all(p.is_file() for p in paths.values()),'Нет full_history-моделей; завершите раздел 10 тетради.')
    models={}
    for target,path in paths.items():
        model=CatBoostRegressor();model.load_model(str(path))
        require(model.feature_names_==manifest['features'],'Порядок признаков модели не совпадает с experiment.json.')
        models[target]=model
    digest=hashlib.sha256(''.join(t+sha_file(paths[t]) for t in TARGETS).encode()).hexdigest()
    return manifest,history,models,digest,model_dir


def predict(engine,run_dir,model_dir=None):
    assert_quality(engine=engine,source='forecast_predict')
    manifest,training,models,model_hash,_=bundle(run_dir,model_dir)
    history=read_history(engine)
    prefix=history.loc[history.flight_date<=training.flight_date.max()]
    require(history_digest(prefix)==history_digest(training),
            'История обучения в Gold изменилась или неполна. Проверьте данные и выполните train для явного переобучения.')
    last=history.flight_date.max(); next_day=last+pd.Timedelta(days=1)
    future=history.groupby('airport_code',as_index=False).tail(1).copy()
    future['flight_date']=next_day
    future[COLUMNS[2:]]=np.nan
    features=make_features(pd.concat([history,future],ignore_index=True))
    future=features.loc[features.flight_date==next_day].copy()
    require(not future[[c for c in manifest['features'] if c.startswith(('total_flights_','delay_share_','cancelled_flights_'))]].isna().any().any(),'Не хватает истории лагов.')
    parts=[]
    for target in TARGETS:
        for name,values in [('seasonal_naive_7',future[f'{target}_lag_7']),('catboost',models[target].predict(future[manifest['features']]))]:
            part=future[['airport_code','flight_date']].copy()
            part['history_through']=last;part['target']=target;part['model']=name
            part['prediction']=bound(values,target)
            part['selected_for_use']=name==manifest['chosen_model_by_validation'][target]
            parts.append(part)
    result=pd.concat(parts,ignore_index=True)
    history_hash=history_digest(history)
    batch=hashlib.sha256((manifest['run_id']+history_hash+model_hash).encode()).hexdigest()
    with engine.begin() as c:
        c.execute(text('SELECT pg_advisory_xact_lock(hashtext(:key))'),{'key':'forecast-'+batch})
        exists=c.execute(text('SELECT batch_id FROM meta.forecast_batches WHERE batch_id=:b'),{'b':batch}).scalar_one_or_none()
        if exists:
            print('Этот прогноз уже сохранён; новых строк нет.')
        else:
            require(c.execute(text('SELECT COUNT(*) FROM meta.forecast_experiments WHERE run_id=:r'),{'r':manifest['run_id']}).scalar_one()==1,'Сначала выполните import-run.')
            c.execute(text('INSERT INTO meta.forecast_batches(batch_id,experiment_run_id,history_through,history_sha256,model_sha256) VALUES (:b,:r,:h,:s,:m)'),{'b':batch,'r':manifest['run_id'],'h':last.date(),'s':history_hash,'m':model_hash})
            ids=airport_ids(c);rows=[]
            for r in result.to_dict('records'):
                r['airport_id']=ids[r.pop('airport_code')];r['batch_id']=batch
                r['flight_date']=r['flight_date'].date();r['history_through']=r['history_through'].date();rows.append(r)
            insert_rows(c,'gold.airport_forecasts',rows)
            print('Сохранён прогноз:',batch)
    out=ROOT/'reports/forecasting_predictions';out.mkdir(parents=True,exist_ok=True)
    result.to_csv(out/f'{batch}.csv',index=False,encoding='utf-8-sig')
    print(result.loc[result.selected_for_use].round({'prediction':3}).to_string(index=False))
    print(f'История до {last.date()}, прогноз на {next_day.date()}. Это дата относительно набора данных.')
    return batch,result


def train(engine,run_dir):
    assert_quality(engine=engine,source='forecast_train')
    manifest=load_manifest(run_dir)
    history=read_history(engine)
    require(history.flight_date.max()>=pd.Timestamp(manifest['test_end']),'История заканчивается раньше эксперимента.')
    featured=make_features(history)
    required=[c for c in manifest['features'] if c.startswith(('total_flights_','delay_share_','cancelled_flights_'))]
    featured=featured.dropna(subset=required)
    require(len(featured)>0,'Недостаточно строк для обучения.')
    model_dir=ROOT/'models/forecasting_retrained'/datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    model_dir.mkdir(parents=True,exist_ok=False)
    for target in TARGETS:
        params={**manifest['base_parameters'],**manifest['selected_catboost_parameters'][target]}
        model=CatBoostRegressor(**params)
        model.fit(featured[manifest['features']],featured[target],cat_features=['airport_code'],verbose=False)
        model.save_model(str(model_dir/f'{target}_full_history.cbm'))
    history.to_csv(model_dir/'training_snapshot.csv',index=False,encoding='utf-8-sig')
    info={'experiment_run_id':manifest['run_id'],'features':manifest['features'],
          'history_through':str(history.flight_date.max().date()),
          'snapshot_file_sha256':sha_file(model_dir/'training_snapshot.csv'),
          'model_sha256':{t:sha_file(model_dir/f'{t}_full_history.cbm') for t in TARGETS}}
    (model_dir/'bundle.json').write_text(json.dumps(info,ensure_ascii=False,indent=2),encoding='utf-8')
    print('Новые модели:',model_dir)
    print('Используйте этот каталог в predict --model-dir. Настройки и выбор моделей не подбирались заново.')
    return model_dir
