"""Общий журнал: python -m src.pipeline_run --forecast-only --run-dir reports/forecasting/<id>."""
from src.history import history_context
from pathlib import Path
from datetime import datetime,timezone
import argparse
import json
import uuid
from sqlalchemy import text
from src.database import get_engine
from src.metadata import ROOT,sync,digest_file


def new_id():return datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')+'-'+uuid.uuid4().hex[:12]


class PipelineRun:
    def __init__(self,engine,catalog,version,name,is_demo=False):
        self.engine=engine;self.catalog=catalog;self.version=version;self.name=name;self.is_demo=is_demo
        self.run_id=new_id();self.seq=0;self.failed=False;self.events=[]
        self.transforms={t['transformation_id']:t for t in catalog['transformations']}

    def __enter__(self):
        with self.engine.begin() as c:
            c.execute(text('''INSERT INTO meta.pipeline_runs(run_id,name,status,is_demo,catalog_version) VALUES (:r,:n,'RUNNING',:d,:v)'''),{'r':self.run_id,'n':self.name,'d':self.is_demo,'v':self.version})
        print('Общий запуск:',self.run_id)
        return self

    def step(self,key,function,*args,**kwargs):
        self.seq+=1;sid=uuid.uuid4().hex;t=self.transforms[key]
        inputs=sorted({e['source_id'] for e in self.catalog['edges'] if e['transformation_id']==key})
        outputs=sorted({e['target_id'] for e in self.catalog['edges'] if e['transformation_id']==key})
        code_hash=digest_file(ROOT/t['file_path'])
        with self.engine.begin() as c:
            c.execute(text('''INSERT INTO meta.pipeline_steps(step_id,run_id,step_no,transformation_id,status,input_datasets,output_datasets,code_file,code_sha256) VALUES (:s,:r,:n,:t,'RUNNING',CAST(:i AS jsonb),CAST(:o AS jsonb),:f,:h)'''),{'s':sid,'r':self.run_id,'n':self.seq,'t':key,'i':json.dumps(inputs),'o':json.dumps(outputs),'f':t['file_path'],'h':code_hash})
        print(f'Шаг {self.seq}: {key}')
        try:
            with history_context(self.run_id,sid,key):
                result=function(*args,**kwargs)
            refs=[]
            if key=='quality':
                refs=[('quality_run',result['run_id'])]
                from src.quality import enforce_quality
                # Связь с журналом сохраняем даже при FAIL.
                self._references(sid,refs)
                enforce_quality(result)
                refs=[]
            elif key=='predict':
                refs=[('forecast_batch',str(result[0]))]
            elif key=='train':
                refs=[('model_directory',str(Path(result).relative_to(ROOT)))]
            self._references(sid,refs)
        except BaseException as exc:
            self.failed=True;self._finish_step(sid,'FAILED',type(exc).__name__)
            self.events.append({'step':key,'status':'FAILED','error_type':type(exc).__name__})
            raise
        self._finish_step(sid,'SUCCESS',None)
        self.events.append({'step':key,'status':'SUCCESS'})
        return result

    def _references(self,sid,refs):
        if not refs:return
        with self.engine.begin() as c:
            for kind,rid in refs:c.execute(text('''INSERT INTO meta.pipeline_references(step_id,reference_type,reference_id) VALUES (:s,:t,:i) ON CONFLICT DO NOTHING'''),{'s':sid,'t':kind,'i':rid})

    def _finish_step(self,sid,status,error):
        with self.engine.begin() as c:c.execute(text('''UPDATE meta.pipeline_steps SET status=:status,finished_at=clock_timestamp(),duration_ms=EXTRACT(EPOCH FROM(clock_timestamp()-started_at))*1000,error_type=:e WHERE step_id=:s'''),{'status':status,'e':error,'s':sid})

    def __exit__(self,typ,exc,tb):
        status='FAILED' if typ or self.failed else 'SUCCESS'
        with self.engine.begin() as c:c.execute(text('UPDATE meta.pipeline_runs SET status=:s,finished_at=clock_timestamp(),error_type=:e WHERE run_id=:r'),{'s':status,'e':typ.__name__ if typ else None,'r':self.run_id})
        out=ROOT/'reports/pipeline'/self.run_id;out.mkdir(parents=True,exist_ok=True)
        data={'run_id':self.run_id,'status':status,'is_demo':self.is_demo,'catalog_version':self.version,'steps':self.events}
        (out/'summary.json').write_text(json.dumps(data,ensure_ascii=False,indent=2),encoding='utf-8')
        print('Общий итог:',status,'| run_id:',self.run_id)
        return False


def execute(engine,run_dir=None,model_dir=None,forecast_only=False,demo_failure=False):
    cat,version=sync(engine)
    name='demo_quality_gate' if demo_failure else ('forecast_only' if forecast_only else 'data_and_forecast')
    with PipelineRun(engine,cat,version,name,is_demo=demo_failure) as run:
        if demo_failure:
            # Проверка распространения отказа без изменения Gold и без вызова predict.
            from src.quality import QualityGateError
            def rejected():raise QualityGateError('Ожидаемый демонстрационный отказ контроля качества.')
            run.step('quality',rejected)
            raise AssertionError('После отказа исполнение не должно дойти сюда.')
        if not forecast_only:
            from src.config import load_config
            from src.ingestion.bts import run_bts_pipeline
            from src.ingestion.weather import run_weather_pipeline
            from src.transform import build_silver,build_gold
            cfg=load_config()
            run.step('ingest_bts',run_bts_pipeline,cfg)
            run.step('ingest_weather',run_weather_pipeline,cfg)
            run.step('build_silver',build_silver)
            run.step('build_gold',build_gold)
        from src.quality import run_quality
        run.step('quality',run_quality,engine=engine,source='pipeline:'+run.run_id)
        if run_dir:
            from src.forecasting.predict import predict
            # predict содержит уже установленную собственную повторную проверку качества.
            run.step('predict',predict,engine,ROOT/run_dir,ROOT/model_dir if model_dir else None)
        return run.run_id


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--run-dir');p.add_argument('--model-dir')
    p.add_argument('--forecast-only',action='store_true');p.add_argument('--demo-failure',action='store_true')
    args=p.parse_args()
    if not args.demo_failure and args.forecast_only and not args.run_dir:p.error('--forecast-only требует --run-dir')
    try:
        engine=get_engine()
        execute(engine,args.run_dir,args.model_dir,args.forecast_only,args.demo_failure)
        return 0
    except Exception as exc:
        from src.quality import QualityGateError
        if args.demo_failure and isinstance(exc,QualityGateError):
            print('DEMO OK: общий запуск FAILED, прогноз не вызывался; рабочие данные не изменялись.');return 0
        print('Запуск остановлен:',type(exc).__name__)
        if isinstance(exc,(ValueError,FileNotFoundError,QualityGateError)):print(str(exc))
        else:print('Подробности: meta.pipeline_runs, meta.pipeline_steps и журнал качества. Проверьте миграцию 012.')
        return 1


if __name__=='__main__':raise SystemExit(main())
