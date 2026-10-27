"""Проверка совместимости и подключение контроля качества без замены исходных файлов целиком."""
from pathlib import Path
from datetime import datetime,timezone
import argparse
import ast
import shutil

ROOT=Path(__file__).resolve().parents[1]
IMPORT='from src.quality import assert_quality'


def has_guard(fn):
    return any(isinstance(n,ast.Call) and isinstance(n.func,ast.Name) and n.func.id=='assert_quality' for n in ast.walk(fn))


def patch(source,kind):
    tree=ast.parse(source)
    lines=source.splitlines(keepends=True)
    additions=[]
    if kind=='main':
        fn=next((n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='main'),None)
        if fn is None:raise ValueError('Не найдена функция main.')
        if not has_guard(fn):
            anchors=[n for n in fn.body if isinstance(n,ast.Expr) and isinstance(n.value,ast.Call) and isinstance(n.value.func,ast.Name) and n.value.func.id=='run_transformations']
            if len(anchors)!=1:raise ValueError('Ожидается ровно один вызов run_transformations в main.')
            additions.append((anchors[0].end_lineno,"    assert_quality(source='main_after_gold')\n"))
    elif kind=='predict':
        for name,source_name in [('predict','forecast_predict'),('train','forecast_train')]:
            fn=next((n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name==name),None)
            if fn is None or 'engine' not in [a.arg for a in fn.args.args]:raise ValueError('Не найдена ожидаемая функция '+name+'(engine,...).')
            if not has_guard(fn):
                first=fn.body[0]
                # Вставка после docstring, если он есть.
                pos=first.end_lineno if isinstance(first,ast.Expr) and isinstance(first.value,ast.Constant) and isinstance(first.value.value,str) else first.lineno-1
                additions.append((pos,f"    assert_quality(engine=engine,source='{source_name}')\n"))
    for pos,line in sorted(additions,reverse=True):lines.insert(pos,line)
    result=''.join(lines)
    if additions and IMPORT not in result:
        # Учитываем возможный __future__ и модульный docstring.
        header=0
        for node in tree.body:
            if isinstance(node,ast.Expr) and isinstance(node.value,ast.Constant) and isinstance(node.value.value,str):header=node.end_lineno
            elif isinstance(node,ast.ImportFrom) and node.module=='__future__':header=node.end_lineno
            else:break
        result_lines=result.splitlines(keepends=True);result_lines.insert(header,IMPORT+'\n');result=''.join(result_lines)
    ast.parse(result)
    return result


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--apply',action='store_true');args=p.parse_args()
    targets=[(ROOT/'src/main.py','main'),(ROOT/'src/forecasting/predict.py','predict')]
    proposed=[]
    # Сначала готовим и проверяем ВСЕ изменения; несовместимость не оставляет частичную установку.
    for path,kind in targets:
        original=path.read_text(encoding='utf-8-sig');new=patch(original,kind)
        proposed.append((path,original,new))
    for path,old,new in proposed:print(('Без изменений: ' if old==new else 'Будет подключён контроль: ')+str(path.relative_to(ROOT)))
    if not args.apply:
        print('Проверка завершена; файлы не изменены. Для установки повторите с --apply.');return
    changed=[r for r in proposed if r[1]!=r[2]]
    if not changed:print('Контроль уже подключён; повторных вставок нет.');return
    backup=ROOT/'reports/quality_install_backups'/datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    for path,old,new in changed:
        dest=backup/path.relative_to(ROOT);dest.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(path,dest)
    written=[]
    try:
        for path,old,new in changed:path.write_text(new,encoding='utf-8');written.append((path,old))
    except Exception:
        for path,old in written:path.write_text(old,encoding='utf-8')
        raise
    print('Контроль подключён. Резервные копии:',backup)
    print('main: после Gold; predict/train: до расчёта и записи нового прогноза/модели.')


if __name__=='__main__':
    try:main()
    except (ValueError,FileNotFoundError) as exc:print('Установка остановлена:',exc);raise SystemExit(1)
