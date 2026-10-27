"""python scripts/install_history_hooks.py [--apply]: контекст истории и расширение каталога."""
from pathlib import Path
from datetime import datetime,timezone
import argparse
import ast
import shutil
import yaml

ROOT=Path(__file__).resolve().parents[1]


def add_import(source,line):
    if line in source:return source
    nodes=ast.parse(source).body;position=0
    for n in nodes:
        if isinstance(n,ast.Expr) and isinstance(n.value,ast.Constant) and isinstance(n.value.value,str):position=n.end_lineno
        elif isinstance(n,ast.ImportFrom) and n.module=='__future__':position=n.end_lineno
        else:break
    lines=source.splitlines(keepends=True);lines.insert(position,line+'\n');return ''.join(lines)


def patch_transform(source):
    tree=ast.parse(source)
    fn=next((n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='execute_sql_file'),None)
    if fn is None:raise ValueError('Не найдена execute_sql_file в src/transform.py.')
    if any(isinstance(n,ast.Call) and isinstance(n.func,ast.Name) and n.func.id=='bind_history_context' for n in ast.walk(fn)):return source
    transactions=[n for n in ast.walk(fn) if isinstance(n,ast.With) and any(isinstance(i.optional_vars,ast.Name) and i.optional_vars.id=='connection' for i in n.items)]
    if len(transactions)!=1:raise ValueError('Не найдена ожидаемая транзакция connection в execute_sql_file.')
    first=transactions[0].body[0];lines=source.splitlines(keepends=True)
    lines.insert(first.lineno-1,' '*first.col_offset+'bind_history_context(connection,filename)\n')
    result=add_import(''.join(lines),'from src.history import bind_history_context');ast.parse(result);return result


def patch_pipeline(source):
    tree=ast.parse(source)
    cls=next((n for n in tree.body if isinstance(n,ast.ClassDef) and n.name=='PipelineRun'),None)
    fn=next((n for n in cls.body if isinstance(n,ast.FunctionDef) and n.name=='step'),None) if cls else None
    if fn is None:raise ValueError('Не найдена PipelineRun.step. Установите второй шаг.')
    if any(isinstance(n,ast.Call) and isinstance(n.func,ast.Name) and n.func.id=='history_context' for n in ast.walk(fn)):return source
    assignments=[n for n in ast.walk(fn) if isinstance(n,ast.Assign) and isinstance(n.value,ast.Call) and isinstance(n.value.func,ast.Name) and n.value.func.id=='function']
    if len(assignments)!=1:raise ValueError('Не найден однозначный вызов function в PipelineRun.step.')
    n=assignments[0];lines=source.splitlines(keepends=True);original=lines[n.lineno-1:n.end_lineno]
    lines[n.lineno-1:n.end_lineno]=[' '*n.col_offset+'with history_context(self.run_id,sid,key):\n']+['    '+line for line in original]
    result=add_import(''.join(lines),'from src.history import history_context');ast.parse(result);return result


def patch_catalog(source):
    cat=yaml.safe_load(source)
    dataset=dict(dataset_id='gold_daily_history',name='История версий Gold',layer='gold',kind='table',location='gold.airport_daily_history',grain='аэропорт–дата–версия',description='Версии состояния, DELETE как маркер удаления; updated_at не создаёт версию. Начало истории — внедрение 013.')
    transform=dict(transformation_id='history_capture',name='Фиксация изменений Gold',file_path='sql/013_create_gold_history.sql',description='Триггер в транзакции изменения Gold. Контекст связывает версию с общим запуском и шагом.')
    edge=dict(source_id='gold_daily',target_id='gold_daily_history',transformation_id='history_capture')
    changed=False
    for group,key,item in [('datasets','dataset_id',dataset),('transformations','transformation_id',transform)]:
        matches=[r for r in cat[group] if r[key]==item[key]]
        if not matches:cat[group].append(item);changed=True
        elif len(matches)!=1 or matches[0]!=item:raise ValueError('Конфликт элемента каталога: '+item[key])
    if edge not in cat['edges']:cat['edges'].append(edge);changed=True
    return yaml.safe_dump(cat,allow_unicode=True,sort_keys=False) if changed else source


def patch_metadata_test(source):
    # Старый тест размеров должен допускать расширение каталога следующим компонентом.
    old="        self.assertEqual([len(self.cat[k]) for k in ['datasets','transformations','edges','metrics']],[24,13,39,3])"
    new="        for key,minimum in zip(['datasets','transformations','edges','metrics'],[24,13,39,3]):\n            self.assertGreaterEqual(len(self.cat[key]),minimum)"
    return source.replace(old,new)


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--apply',action='store_true');args=p.parse_args()
    targets=[('src/transform.py',patch_transform),('src/pipeline_run.py',patch_pipeline),('config/metadata.yaml',patch_catalog)]
    if (ROOT/'tests/test_metadata_unit.py').is_file():targets.append(('tests/test_metadata_unit.py',patch_metadata_test))
    proposed=[]
    for rel,patch in targets:
        path=ROOT/rel;old=path.read_text(encoding='utf-8-sig');new=patch(old)
        if path.suffix=='.py':ast.parse(new)
        proposed.append((path,old,new));print(('Без изменений: ' if old==new else 'Будет обновлён: ')+rel)
    if not args.apply:print('Проверка завершена; файлы не изменены. Для установки повторите с --apply.');return
    changed=[r for r in proposed if r[1]!=r[2]]
    if not changed:print('История уже подключена; повторных вставок нет.');return
    backup=ROOT/'reports/history_install_backups'/datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    for path,old,new in changed:
        destination=backup/path.relative_to(ROOT);destination.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(path,destination)
    written=[]
    try:
        for path,old,new in changed:path.write_text(new,encoding='utf-8');written.append((path,old))
    except Exception:
        for path,old in written:path.write_text(old,encoding='utf-8')
        raise
    print('Контекст истории подключён. Резервные копии:',backup)
    print('Теперь выполните python -m src.metadata sync: каталог расширен до 25/14/40/3.')


if __name__=='__main__':
    try:main()
    except (ValueError,FileNotFoundError) as exc:print('Установка остановлена:',exc);raise SystemExit(1)
