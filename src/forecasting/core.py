from pathlib import Path
import hashlib
import json
import numpy as np
import pandas as pd
from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[2]
TARGETS = ['total_flights', 'delay_share']
WEATHER = ['avg_temperature', 'total_precipitation', 'avg_wind_speed', 'max_wind_gust']
COLUMNS = ['airport_code','flight_date','total_flights','completed_flights',
           'cancelled_flights','diverted_flights','delayed_flights','delay_share'] + WEATHER


def require(condition, message):
    if not condition:
        raise ValueError(message)


def sha_file(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def config():
    settings = json.loads((ROOT / 'config/forecasting.json').read_text(encoding='utf-8-sig'))
    require(settings['targets'] == TARGETS and settings['horizon_days'] == 1,
            'Этот модуль реализует две заданные цели и горизонт ровно один день.')
    return settings


def load_manifest(run_dir):
    manifest = json.loads((Path(run_dir) / 'experiment.json').read_text(encoding='utf-8-sig'))
    require(manifest['targets'] == TARGETS, 'Несовместимые целевые показатели.')
    require(manifest['chosen_model_by_validation'] == config()['expected_selection'],
            'Выбор моделей отличается от проверенного. Сначала проверьте эксперимент и config/forecasting.json.')
    return manifest


def validate_history(df, airports):
    df = df.copy()
    require(set(COLUMNS).issubset(df.columns), 'Не хватает столбцов Gold.')
    df['flight_date'] = pd.to_datetime(df.flight_date, errors='raise')
    require(not df[COLUMNS[:2]].isna().any().any(), 'Пустой аэропорт или дата.')
    require((df.flight_date == df.flight_date.dt.normalize()).all(), 'Ожидались суточные даты.')
    require(set(df.airport_code) == set(airports), 'Не совпадает перечень аэропортов.')
    require(not df.duplicated(['airport_code','flight_date']).any(), 'Дубликаты суточных записей.')
    for c in COLUMNS[2:]:
        df[c] = pd.to_numeric(df[c], errors='raise')
    counters = COLUMNS[2:7]
    require(np.isfinite(df[counters + ['delay_share']]).all().all(), 'Некорректные цели или счётчики.')
    require((df[counters] >= 0).all().all() and (df[counters] % 1 == 0).all().all(), 'Некорректные количества рейсов.')
    require((df.completed_flights > 0).all(), 'Нулевая база расчёта доли задержек.')
    require((df.completed_flights <= df.total_flights).all(), 'Выполненных больше общего числа.')
    require((df.total_flights == df.completed_flights + df.cancelled_flights).all(), 'Не согласованы отмены и выполненные рейсы.')
    require((df.delayed_flights <= df.completed_flights).all(), 'Неверный числитель доли задержек.')
    require(df.delay_share.between(0,100).all(), 'Доля вне 0–100.')
    require(np.allclose(df.delay_share,100*df.delayed_flights/df.completed_flights,atol=1e-6), 'Неверная формула доли задержек.')
    require(not np.isinf(df[WEATHER].to_numpy(dtype=float)).any(), 'Бесконечности в погоде.')
    start, end = df.flight_date.min(), df.flight_date.max()
    require((end-start).days >= 28, 'Нужно минимум 29 дней истории.')
    for airport, part in df.groupby('airport_code'):
        dates = pd.DatetimeIndex(part.flight_date.sort_values())
        require(dates.equals(pd.date_range(start,end)), f'Пропуски дат или разные периоды аэропортов: {airport}.')
    return df.sort_values(['airport_code','flight_date']).reset_index(drop=True)


def history_digest(df):
    canonical = df[COLUMNS].sort_values(['airport_code','flight_date']).copy()
    canonical['flight_date'] = pd.to_datetime(canonical.flight_date).dt.strftime('%Y-%m-%d')
    return hashlib.sha256(canonical.to_csv(index=False,float_format='%.10f',na_rep='NULL',lineterminator='\n').encode()).hexdigest()


def read_history(engine):
    with engine.connect() as connection:
        df = pd.read_sql(text('SELECT ' + ','.join(COLUMNS) + ' FROM gold.airport_daily_bi ORDER BY airport_code,flight_date'),connection)
    return validate_history(df, config()['airports'])


def make_features(frame):
    df = frame.sort_values(['airport_code', 'flight_date']).copy()
    date = df['flight_date']
    df['day_of_week'] = date.dt.dayofweek
    df['month'] = date.dt.month
    df['is_weekend'] = (date.dt.dayofweek >= 5).astype(int)
    df['time_index'] = (date - pd.Timestamp('2023-01-01')).dt.days
    df['year_sin'] = np.sin(2 * np.pi * (date.dt.dayofyear - 1) / 365.25)
    df['year_cos'] = np.cos(2 * np.pi * (date.dt.dayofyear - 1) / 365.25)
    for col in TARGETS + ['cancelled_flights']:
        group = df.groupby('airport_code')[col]
        for lag in [1, 7, 14, 28]:
            df[f'{col}_lag_{lag}'] = group.shift(lag)
        for window in [7, 28]:
            df[f'{col}_mean_{window}'] = group.transform(lambda x: x.shift(1).rolling(window, min_periods=window).mean())
    for col in WEATHER:
        df[f'{col}_lag_1'] = df.groupby('airport_code')[col].shift(1)
        df[f'{col}_mean_7'] = df.groupby('airport_code')[col].transform(lambda x: x.shift(1).rolling(7, min_periods=1).mean())
    return df


def bound(values, target):
    values = np.asarray(values,dtype=float)
    require(np.isfinite(values).all(), 'Модель вернула нечисловой прогноз.')
    return np.clip(values,0,100 if target == 'delay_share' else np.inf)


def airport_ids(connection):
    return dict(connection.execute(text('SELECT airport_code,airport_id FROM silver.dim_airport')).all())


def insert_rows(connection, table, rows):
    if not rows:
        return
    cols = list(rows[0])
    statement = text(f"INSERT INTO {table} ({','.join(cols)}) VALUES ({','.join(':'+c for c in cols)})")
    for start in range(0,len(rows),1000):
        connection.execute(statement,rows[start:start+1000])
