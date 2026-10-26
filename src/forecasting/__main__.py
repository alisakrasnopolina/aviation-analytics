"""python -m src.forecasting --help"""
import argparse
import sys
import pandas as pd
from sqlalchemy import text
from src.database import get_engine
from .core import ROOT
from .import_run import import_run
from .predict import predict,train


def main():
    parser=argparse.ArgumentParser(description='Прогнозное ядро: импорт результатов, прогноз и переобучение.')
    commands=parser.add_subparsers(dest='command',required=True)
    for name in ['import-run','predict','train']:
        command=commands.add_parser(name)
        command.add_argument('--run-dir',required=True,type=str,help='Каталог reports/forecasting/<run_id> с результатами тетради')
        if name=='predict':
            command.add_argument('--model-dir',type=str,default=None,help='Необязательно: каталог с full_history моделями')
    commands.add_parser('check')
    args=parser.parse_args()
    try:
        engine=get_engine()
        if args.command=='import-run':import_run(engine,ROOT/args.run_dir)
        elif args.command=='predict':predict(engine,ROOT/args.run_dir,ROOT/args.model_dir if args.model_dir else None)
        elif args.command=='train':train(engine,ROOT/args.run_dir)
        else:
            queries={
                'Объёмы таблиц':"SELECT 'experiments' AS object,COUNT(*) AS rows FROM meta.forecast_experiments UNION ALL SELECT 'metrics',COUNT(*) FROM gold.forecast_metrics UNION ALL SELECT 'backtest',COUNT(*) FROM gold.forecast_backtest UNION ALL SELECT 'batches',COUNT(*) FROM meta.forecast_batches UNION ALL SELECT 'forecasts',COUNT(*) FROM gold.airport_forecasts",
                'Последние тестовые метрики':"SELECT target,model,n,mae,rmse,selected_for_use FROM gold.forecast_metrics_bi WHERE split='test' AND airport_code='ALL' ORDER BY target,model",
                'Выбранные прогнозы':"SELECT airport_code,flight_date,history_through,target,model,prediction FROM gold.airport_forecasts_bi WHERE selected_for_use ORDER BY target,airport_code"}
            with engine.connect() as c:
                for title,query in queries.items():
                    print('\n'+title);print(pd.read_sql(text(query),c).round({'mae':4,'rmse':4,'prediction':3}).to_string(index=False))
    except Exception as exc:
        # Не выводим DSN, пароль или полные параметры SQL-соединения.
        print(f'Операция остановлена ({type(exc).__name__}).',file=sys.stderr)
        if isinstance(exc,(ValueError,FileNotFoundError,KeyError)):
            print(str(exc),file=sys.stderr)
        else:
            print('Проверьте Docker, .env, зависимости и применение миграции 010. Существующие таблицы истории не изменяются.',file=sys.stderr)
        return 1
    return 0


if __name__=='__main__':
    raise SystemExit(main())
