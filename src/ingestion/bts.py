import hashlib
import time
import zipfile
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from sqlalchemy import MetaData, Table
from sqlalchemy.dialects.postgresql import insert as pg_insert

from src.database import (
    get_engine,
    start_load,
    finish_load_success,
    finish_load_failed,
    get_source_watermark,
    update_source_watermark,
)

BASE_URL = "https://transtats.bts.gov/PREZIP"

RAW_COLUMNS = [
    "Year",
    "Quarter",
    "Month",
    "DayofMonth",
    "DayOfWeek",
    "FlightDate",

    "Reporting_Airline",
    "DOT_ID_Reporting_Airline",
    "Flight_Number_Reporting_Airline",

    "OriginAirportID",
    "Origin",
    "OriginCityName",
    "OriginState",

    "DestAirportID",
    "Dest",
    "DestCityName",
    "DestState",

    "CRSDepTime",
    "DepTime",
    "DepDelay",
    "DepDelayMinutes",
    "DepDel15",

    "CRSArrTime",
    "ArrTime",
    "ArrDelay",
    "ArrDelayMinutes",
    "ArrDel15",

    "Cancelled",
    "CancellationCode",
    "Diverted",

    "AirTime",
    "Distance",

    "CarrierDelay",
    "WeatherDelay",
    "NASDelay",
    "SecurityDelay",
    "LateAircraftDelay",
]

COLUMN_MAPPING = {
    "Year": "year",
    "Quarter": "quarter",
    "Month": "month",
    "DayofMonth": "day_of_month",
    "DayOfWeek": "day_of_week",
    "FlightDate": "flight_date",

    "Reporting_Airline": "reporting_airline",
    "DOT_ID_Reporting_Airline": "dot_id_reporting_airline",
    "Flight_Number_Reporting_Airline":
        "flight_number_reporting_airline",

    "OriginAirportID": "origin_airport_id",
    "Origin": "origin",
    "OriginCityName": "origin_city_name",
    "OriginState": "origin_state",

    "DestAirportID": "dest_airport_id",
    "Dest": "dest",
    "DestCityName": "dest_city_name",
    "DestState": "dest_state",

    "CRSDepTime": "crs_dep_time",
    "DepTime": "dep_time",
    "DepDelay": "dep_delay",
    "DepDelayMinutes": "dep_delay_minutes",
    "DepDel15": "dep_del15",

    "CRSArrTime": "crs_arr_time",
    "ArrTime": "arr_time",
    "ArrDelay": "arr_delay",
    "ArrDelayMinutes": "arr_delay_minutes",
    "ArrDel15": "arr_del15",

    "Cancelled": "cancelled",
    "CancellationCode": "cancellation_code",
    "Diverted": "diverted",

    "AirTime": "air_time",
    "Distance": "distance",

    "CarrierDelay": "carrier_delay",
    "WeatherDelay": "weather_delay",
    "NASDelay": "nas_delay",
    "SecurityDelay": "security_delay",
    "LateAircraftDelay": "late_aircraft_delay",
}

def next_month(month_string):
    date = datetime.strptime(
        month_string,
        "%Y-%m",
    )

    if date.month == 12:
        return f"{date.year + 1}-01"

    return f"{date.year}-{date.month + 1:02d}"

def build_bts_url(month_string):
    year, month = month_string.split("-")

    month_number = int(month)

    filename = (
        "On_Time_Reporting_Carrier_"
        "On_Time_Performance_1987_present_"
        f"{year}_{month_number}.zip"
    )

    url = f"{BASE_URL}/{filename}"

    return url, filename

def download_bts_zip(month_string):
    url, filename = build_bts_url(
        month_string
    )

    year, month = month_string.split("-")

    directory = Path(
        f"data/raw/bts/{year}/{month}"
    )

    directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    zip_path = directory / filename

    last_error = None

    for attempt in range(1, 4):
        try:
            print(
                f"Скачивание BTS "
                f"{month_string}, попытка {attempt}"
            )

            with requests.get(
                url,
                stream=True,
                timeout=120,
            ) as response:

                response.raise_for_status()

                with open(
                    zip_path,
                    "wb",
                ) as file:

                    for chunk in response.iter_content(
                        chunk_size=1024 * 1024
                    ):
                        if chunk:
                            file.write(chunk)

            return zip_path

        except requests.RequestException as error:
            last_error = error

            if attempt < 3:
                time.sleep(2 ** (attempt - 1))

    raise last_error

def read_bts_zip(zip_path):
    with zipfile.ZipFile(zip_path) as archive:

        csv_files = [
            name
            for name in archive.namelist()
            if name.lower().endswith(".csv")
        ]

        if len(csv_files) != 1:
            raise RuntimeError(
                "В архиве BTS ожидается "
                "ровно один CSV-файл."
            )

        csv_name = csv_files[0]

        with archive.open(csv_name) as file:
            df = pd.read_csv(
                file,
                usecols=RAW_COLUMNS,
                low_memory=False,
            )

    return df

def prepare_bts_dataframe(
    df,
    source_file,
    airports,
):
    df = df[
        df["Origin"].isin(airports)
    ].copy()

    df["FlightDate"] = pd.to_datetime(
        df["FlightDate"],
        errors="coerce",
    ).dt.date

    df = df.rename(
        columns=COLUMN_MAPPING
    )

    key_columns = [
        "flight_date",
        "reporting_airline",
        "flight_number_reporting_airline",
        "origin",
        "dest",
        "crs_dep_time",
    ]

    key_text = (
        df[key_columns]
        .astype("string")
        .fillna("<NA>")
        .agg("|".join, axis=1)
    )

    df["row_hash"] = key_text.map(
        lambda value: hashlib.sha256(
            value.encode("utf-8")
        ).hexdigest()
    )

    df["source_file"] = str(source_file)

    df = (
        df.astype(object)
        .where(pd.notna(df), None)
    )

    return df


def load_bts_to_bronze(df):
    engine = get_engine()

    metadata = MetaData()

    table = Table(
        "bts_flights_raw",
        metadata,
        schema="bronze",
        autoload_with=engine,
    )

    inserted_rows = 0

    records = df.to_dict(
        orient="records"
    )

    chunk_size = 1000

    with engine.begin() as connection:

        for start in range(
            0,
            len(records),
            chunk_size,
        ):
            batch = records[
                start:start + chunk_size
            ]

            statement = (
                pg_insert(table)
                .values(batch)
                .on_conflict_do_nothing(
                    index_elements=["row_hash"]
                )
            )

            result = connection.execute(
                statement
            )

            inserted_rows += result.rowcount

    return inserted_rows

def load_bts_month(
    month_string,
    airports,
):
    load_id = start_load(
        source="BTS",
        object_name="flights",
        period_from=month_string,
        period_to=month_string,
    )

    try:
        zip_path = download_bts_zip(
            month_string
        )

        raw_df = read_bts_zip(
            zip_path
        )

        df = prepare_bts_dataframe(
            raw_df,
            zip_path,
            airports,
        )

        rows_received = len(df)

        inserted_rows = (
            load_bts_to_bronze(df)
        )

        rows_skipped = (
            rows_received - inserted_rows
        )

        finish_load_success(
            load_id=load_id,
            rows_received=rows_received,
            rows_inserted=inserted_rows,
            rows_skipped=rows_skipped,
        )

        update_source_watermark(
            source="BTS",
            object_name="flights",
            watermark_type="month",
            watermark_value=month_string,
        )

        print(
            f"BTS {month_string}:"
        )
        print(
            f"Получено строк: "
            f"{rows_received}"
        )
        print(
            f"Добавлено: "
            f"{inserted_rows}"
        )
        print(
            f"Пропущено: "
            f"{rows_skipped}"
        )

    except Exception as error:

        finish_load_failed(
            load_id=load_id,
            error_message=error,
        )

        print(
            f"Ошибка BTS "
            f"{month_string}: {error}"
        )

        raise

def run_bts_pipeline(config):
    airport_codes = [
        airport["code"]
        for airport in config["airports"]
    ]

    default_start_month = (
        config["bts"]["default_start_month"]
    )

    target_end_month = (
        config["bts"]["target_end_month"]
    )

    last_loaded = get_source_watermark(
        source="BTS",
        object_name="flights",
    )

    if last_loaded is None:
        current_month = default_start_month
    else:
        current_month = next_month(
            last_loaded
        )

    if current_month > target_end_month:
        print(
            "Новых данных BTS "
            "для загрузки нет."
        )
        return

    while current_month <= target_end_month:
        load_bts_month(
            current_month,
            airport_codes,
        )

        current_month = next_month(
            current_month
        )