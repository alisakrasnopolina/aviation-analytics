"""Каталог и lineage: python -m src.metadata sync | show | lineage delay_share | check."""
from pathlib import Path
import argparse
import hashlib
import json
import re
import pandas as pd
import yaml
from sqlalchemy import text
from src.database import get_engine

ROOT=Path(__file__).resolve().parents[1]


def digest_file(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def validate_catalog(cat):
    groups=[('datasets','dataset_id'),('transformations','transformation_id'),('metrics','metric_id')]
    for group,key in groups:
        ids=[r[key] for r in cat[group]]
        if len(ids)!=len(set(ids)) or any(not re.fullmatch(r'[a-z][a-z0-9_]*',i) for i in ids):raise ValueError('Некорректные/повторные id: '+group)
    datasets={r['dataset_id'] for r in cat['datasets']};transforms={r['transformation_id'] for r in cat['transformations']}
    seen=set();graph={i:[] for i in datasets}
    for e in cat['edges']:
        key=(e['source_id'],e['target_id'],e['transformation_id'])
        if key in seen or key[0] not in datasets or key[1] not in datasets or key[2] not in transforms:raise ValueError('Некорректная связь lineage.')
        seen.add(key);graph[key[0]].append(key[1])
    visited=set();active=set()
    def visit(node):
        if node in active:raise ValueError('Цикл в lineage.')
        if node in visited:return
        active.add(node)
        for child in graph[node]:visit(child)
        active.remove(node);visited.add(node)
    for node in graph:visit(node)
    for m in cat['metrics']:
        if m['dataset_id'] not in datasets:raise ValueError('Неизвестный набор показателя '+m['metric_id'])
    for t in cat['transformations']:
        rel=Path(t['file_path'])
        if rel.is_absolute() or '..' in rel.parts:raise ValueError('Путь кода должен быть внутри проекта.')
    return cat


def load_catalog():
    cat=validate_catalog(yaml.safe_load((ROOT/'config/metadata.yaml').read_text(encoding='utf-8-sig')))
    for t in cat['transformations']:
        path=ROOT/t['file_path']
        if not path.is_file():raise FileNotFoundError('Не найден файл каталога: '+t['file_path'])
        t['code_sha256']=digest_file(path)
    serial=json.dumps(cat,ensure_ascii=False,sort_keys=True,separators=(',',':'))
    return cat,hashlib.sha256(serial.encode()).hexdigest(),serial


def sync(engine=None):
    engine=engine or get_engine();cat,version,serial=load_catalog()
    with engine.begin() as c:
        c.execute(text("SELECT pg_advisory_xact_lock(hashtext('aviation-metadata-sync'))"))
        c.execute(text('INSERT INTO meta.catalog_versions(catalog_version,snapshot) VALUES (:v,CAST(:s AS jsonb)) ON CONFLICT DO NOTHING'),{'v':version,'s':serial})
        for group,table,key in [('datasets','dataset_catalog','dataset_id'),('transformations','transformation_catalog','transformation_id'),('metrics','metric_catalog','metric_id')]:
            for item in cat[group]:
                r={**item,'catalog_version':version}
                if group=='metrics':r['source_fields']=json.dumps(r['source_fields'],ensure_ascii=False)
                columns=list(r);expr=[f'CAST(:{k} AS jsonb)' if k=='source_fields' else ':'+k for k in columns]
                updates=','.join(f'{k}=EXCLUDED.{k}' for k in columns if k!=key)
                c.execute(text(f"INSERT INTO meta.{table} ({','.join(columns)}) VALUES ({','.join(expr)}) ON CONFLICT ({key}) DO UPDATE SET {updates}"),r)
        for e in cat['edges']:
            c.execute(text('''INSERT INTO meta.lineage_edges(source_id,target_id,transformation_id,catalog_version) VALUES (:source_id,:target_id,:transformation_id,:catalog_version) ON CONFLICT(source_id,target_id,transformation_id) DO UPDATE SET catalog_version=EXCLUDED.catalog_version'''),{**e,'catalog_version':version})
        c.execute(text('DELETE FROM meta.lineage_edges WHERE catalog_version<>:v'),{'v':version})
    print(f"Каталог синхронизирован: наборов {len(cat['datasets'])}, преобразований {len(cat['transformations'])}, связей {len(cat['edges'])}, показателей {len(cat['metrics'])}.")
    print('Версия каталога:',version)
    return cat,version


def show(engine):
    with engine.connect() as c:
        for title,sql in [('Наборы','SELECT dataset_id,layer,kind,location,grain FROM meta.dataset_catalog ORDER BY layer,dataset_id'),('Показатели','SELECT metric_id,name,formula,unit,limitations FROM meta.metric_catalog ORDER BY metric_id')]:
            print('\n'+title);print(pd.read_sql(text(sql),c).to_string(index=False))


def lineage(engine,metric):
    with engine.connect() as c:
        root=c.execute(text('SELECT dataset_id FROM meta.metric_catalog WHERE metric_id=:m'),{'m':metric}).scalar_one_or_none()
        if root is None:raise ValueError('Неизвестный показатель. Сначала sync, затем show.')
        sql=text('''WITH RECURSIVE ancestors(node) AS (
            SELECT CAST(:root AS text) UNION
            SELECT e.source_id FROM meta.lineage_edges e JOIN ancestors a ON e.target_id=a.node
        ) SELECT e.source_id,s.location source_location,e.target_id,t.location target_location,
                 e.transformation_id,x.file_path
          FROM meta.lineage_edges e JOIN ancestors a ON e.target_id=a.node
          JOIN meta.dataset_catalog s ON s.dataset_id=e.source_id
          JOIN meta.dataset_catalog t ON t.dataset_id=e.target_id
          JOIN meta.transformation_catalog x USING(transformation_id)
          ORDER BY e.target_id,e.source_id,e.transformation_id''')
        df=pd.read_sql(sql,c,params={'root':root})
    print('Показатель:',metric,'| набор:',root)
    print(df.to_string(index=False))
    out=ROOT/'reports/metadata';out.mkdir(parents=True,exist_ok=True)
    df.to_csv(out/f'lineage_{metric}.csv',index=False,encoding='utf-8-sig')
    return df


def check(engine):
    with engine.connect() as c:
        cat=c.execute(text("SELECT dataset_id,location FROM meta.dataset_catalog WHERE kind IN ('table','view') ORDER BY dataset_id")).all()
        result=[]
        for name,location in cat:
            exists=c.execute(text('SELECT to_regclass(:loc) IS NOT NULL'),{'loc':location}).scalar_one()
            result.append({'dataset_id':name,'location':location,'status':'OK' if exists else 'MISSING'})
    df=pd.DataFrame(result);print(df.to_string(index=False))
    if not result or (df.status!='OK').any():raise ValueError('Не все объекты каталога доступны в PostgreSQL.')
    print('Каталог согласован с наличием объектов PostgreSQL.')


def main():
    p=argparse.ArgumentParser(description=__doc__);sub=p.add_subparsers(dest='command',required=True)
    for cmd in ['sync','show','check']:sub.add_parser(cmd)
    lin=sub.add_parser('lineage');lin.add_argument('metric')
    args=p.parse_args()
    try:
        e=get_engine()
        if args.command=='sync':sync(e)
        elif args.command=='show':show(e)
        elif args.command=='check':check(e)
        else:lineage(e,args.metric)
        return 0
    except Exception as exc:
        print('Операция остановлена:',type(exc).__name__)
        if isinstance(exc,(ValueError,FileNotFoundError)):print(str(exc))
        else:print('Проверьте Docker, .env и миграцию 012. Параметры подключения не выводятся.')
        return 1


if __name__=='__main__':raise SystemExit(main())
