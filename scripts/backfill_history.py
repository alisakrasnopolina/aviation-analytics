"""Historical backfill for five US airports, 2023-01 through 2025-12.

Run from the project root:
    python -m scripts.backfill_history --test
    python -m scripts.backfill_history --full

Requires load_bts_month(..., update_watermark=False) support in src/ingestion/bts.py.
"""

import argparse
import calendar
import time
from datetime import date, timedelta

import requests
from sqlalchemy import bindparam, text

from src.config import load_config
from src.database import (
    finish_load_failed,
    finish_load_success,
    get_engine,
    get_source_watermark,
    start_load,
    update_source_watermark,
)
from src.ingestion.bts import load_bts_month, next_month
from src.ingestion.weather import (
    download_weather,
    load_weather_to_bronze,
    prepare_weather_dataframe,
    save_raw_json,
)

START_MONTH = "2023-01"
END_MONTH = "2025-12"
TEST_MONTH = "2023-01"
EXPECTED_AIRPORTS = {"JFK", "LAX", "ORD", "ATL", "DFW"}


def months_between(start_month, end_month):
    current = start_month
    while current <= end_month:
        yield current
        current = next_month(current)


def month_bounds(month):
    year, number = map(int, month.split("-"))
    start = date(year, number, 1)
    end = date(year, number, calendar.monthrange(year, number)[1])
    return start, end


def bts_month_is_complete(month, airport_codes):
    first, last = month_bounds(month)
    query = text("""
        SELECT origin, COUNT(DISTINCT flight_date) AS days
        FROM bronze.bts_flights_raw
        WHERE flight_date BETWEEN :first AND :last
          AND origin IN :codes
        GROUP BY origin
    """).bindparams(bindparam("codes", expanding=True))
    with get_engine().connect() as connection:
        found = dict(connection.execute(
            query, {"first": first, "last": last, "codes": airport_codes}
        ).all())
    expected = (last - first).days + 1
    return all(found.get(code) == expected for code in airport_codes)


def backfill_bts(months, airport_codes):
    for month in months:
        if bts_month_is_complete(month, airport_codes):
            print(f"[BTS] {month}: уже загружен для всех аэропортов, пропуск")
            continue
        print(f"\n[BTS] {month} | аэропорты: {', '.join(airport_codes)}")
        load_bts_month(
            month,
            airport_codes,
            update_watermark=False,
        )


def download_weather_with_retry(airport, start_date, end_date, variables):
    for attempt in range(1, 5):
        try:
            return download_weather(
                airport["code"],
                airport["latitude"],
                airport["longitude"],
                start_date,
                end_date,
                variables,
            )
        except requests.RequestException as exc:
            if attempt == 4:
                raise
            pause = 10 * (2 ** (attempt - 1))
            print(f"Ошибка Open-Meteo: {exc}. Повтор через {pause} с.")
            time.sleep(pause)
    raise RuntimeError("Не удалось получить данные Open-Meteo")


def weather_month_is_complete(code, first, last):
    query = text("""
        SELECT COUNT(DISTINCT weather_time::date)
        FROM bronze.weather_hourly_raw
        WHERE airport_code = :code
          AND weather_time::date BETWEEN :first AND :last
    """)
    with get_engine().connect() as connection:
        found = connection.execute(
            query, {"code": code, "first": first, "last": last}
        ).scalar_one()
    return found == (last - first).days + 1


def backfill_weather(months, airports, variables):
    for airport in airports:
        code = airport["code"]
        for month in months:
            first, last = month_bounds(month)
            if weather_month_is_complete(code, first, last):
                print(f"[Open-Meteo] {code} {month}: уже загружен, пропуск")
                continue
            start_date, end_date = first.isoformat(), last.isoformat()
            print(f"\n[Open-Meteo] {code}: {start_date} — {end_date}")

            load_id = start_load(
                source="OpenMeteo",
                object_name=code,
                period_from=start_date,
                period_to=end_date,
            )
            try:
                data = download_weather_with_retry(
                    airport, start_date, end_date, variables
                )
                path = save_raw_json(data, code, start_date, end_date)
                df = prepare_weather_dataframe(data, code, path)

                expected_dates = {
                    first + timedelta(days=offset)
                    for offset in range((last - first).days + 1)
                }
                observed_dates = set(df["weather_time"].dt.date)
                if observed_dates != expected_dates:
                    raise ValueError(
                        f"Неполный погодный период {code} {month}: "
                        f"ожидалось {len(expected_dates)} дней, "
                        f"получено {len(observed_dates)}"
                    )

                inserted = load_weather_to_bronze(df)
                finish_load_success(
                    load_id=load_id,
                    rows_received=len(df),
                    rows_inserted=inserted,
                    rows_skipped=len(df) - inserted,
                )
                print(f"Получено: {len(df)}; добавлено: {inserted}")
            except Exception as exc:
                finish_load_failed(load_id=load_id, error_message=exc)
                raise


def check_full_coverage(airport_codes):
    first, _ = month_bounds(START_MONTH)
    _, last = month_bounds(END_MONTH)
    expected_days = (last - first).days + 1
    engine = get_engine()

    flights_sql = text("""
        SELECT origin AS airport_code, COUNT(DISTINCT flight_date) AS days
        FROM bronze.bts_flights_raw
        WHERE flight_date BETWEEN :first AND :last
          AND origin IN :codes
        GROUP BY origin
    """).bindparams(bindparam("codes", expanding=True))

    weather_sql = text("""
        SELECT airport_code, COUNT(DISTINCT weather_time::date) AS days
        FROM bronze.weather_hourly_raw
        WHERE weather_time::date BETWEEN :first AND :last
          AND airport_code IN :codes
        GROUP BY airport_code
    """).bindparams(bindparam("codes", expanding=True))

    params = {"first": first, "last": last, "codes": airport_codes}
    with engine.connect() as connection:
        flight_days = dict(connection.execute(flights_sql, params).all())
        weather_days = dict(connection.execute(weather_sql, params).all())

    print("\nПроверка количества дней в Bronze:")
    problems = []
    for code in airport_codes:
        flights = flight_days.get(code, 0)
        weather = weather_days.get(code, 0)
        print(f"{code}: рейсы — {flights}; погода — {weather}; ожидалось — {expected_days}")
        if flights != expected_days or weather != expected_days:
            problems.append(code)

    if problems:
        raise RuntimeError(
            "Неполные данные для: " + ", ".join(problems)
            + ". Watermark не изменён."
        )


def advance_watermarks(airport_codes):
    old_bts = get_source_watermark("BTS", "flights")
    if old_bts is None or old_bts < END_MONTH:
        update_source_watermark("BTS", "flights", "month", END_MONTH)

    _, last = month_bounds(END_MONTH)
    final_date = last.isoformat()
    for code in airport_codes:
        old_weather = get_source_watermark("OpenMeteo", code)
        if old_weather is None or old_weather < final_date:
            update_source_watermark("OpenMeteo", code, "date", final_date)
    print("Watermark обновлены до конца 2025 года.")


def main():
    parser = argparse.ArgumentParser(description="Историческая дозагрузка BTS и Open-Meteo")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--test", action="store_true", help="Только январь 2023, без watermark")
    mode.add_argument("--full", action="store_true", help="Все месяцы 2023–2025")
    args = parser.parse_args()

    config = load_config()
    airports = config["airports"]
    codes = [airport["code"] for airport in airports]
    if set(codes) != EXPECTED_AIRPORTS or len(codes) != len(EXPECTED_AIRPORTS):
        raise ValueError(
            "В config/project.yaml должны быть ровно JFK, LAX, ORD, ATL, DFW"
        )

    start = TEST_MONTH if args.test else START_MONTH
    end = TEST_MONTH if args.test else END_MONTH
    months = list(months_between(start, end))
    print(f"Режим: {'ТЕСТ' if args.test else 'ПОЛНАЯ ЗАГРУЗКА'}; {start} — {end}")

    backfill_bts(months, codes)
    backfill_weather(months, airports, config["weather"]["hourly_variables"])

    if args.full:
        check_full_coverage(codes)
        advance_watermarks(codes)
    else:
        print("Тест завершён. Watermark не изменялись.")

    print("\nИсторическая дозагрузка успешно завершена.")


if __name__ == "__main__":
    main()
