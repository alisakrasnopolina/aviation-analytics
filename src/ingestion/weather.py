import json
from pathlib import Path

import pandas as pd
import requests

from sqlalchemy import text

from datetime import datetime, timedelta

from src.database import (
    get_engine,
    start_load,
    finish_load_success,
    finish_load_failed,
    get_source_watermark,
    update_source_watermark,
)

API_URL = "https://archive-api.open-meteo.com/v1/archive"

def download_weather(
    airport_code,
    latitude,
    longitude,
    start_date,
    end_date,
    hourly_variables,
):
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ",".join(hourly_variables),
        "timezone": "auto",
    }

    response = requests.get(
        API_URL,
        params=params,
        timeout=30,
    )

    response.raise_for_status()

    return response.json()


def save_raw_json(data, airport_code, start_date, end_date):
    directory = Path(
        f"data/raw/weather/{airport_code}"
    )

    directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    filename = (
        f"{start_date}_{end_date}.json"
    )

    path = directory / filename

    with open(
        path,
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            data,
            file,
            ensure_ascii=False,
            indent=2,
        )

    return path


def prepare_weather_dataframe(data, airport_code, source_file):
    df = pd.DataFrame(data["hourly"])

    df["time"] = pd.to_datetime(df["time"])

    df.insert(
        0,
        "airport_code",
        airport_code,
    )

    df["source_file"] = str(source_file)

    df = df.rename(
        columns={"time": "weather_time"}
    )

    return df


def load_weather_to_bronze(df):
    engine = get_engine()

    insert_sql = text("""
        INSERT INTO bronze.weather_hourly_raw (
            airport_code,
            weather_time,
            temperature_2m,
            relative_humidity_2m,
            precipitation,
            rain,
            snowfall,
            weather_code,
            pressure_msl,
            cloud_cover,
            wind_speed_10m,
            wind_direction_10m,
            wind_gusts_10m,
            source_file
        )
        VALUES (
            :airport_code,
            :weather_time,
            :temperature_2m,
            :relative_humidity_2m,
            :precipitation,
            :rain,
            :snowfall,
            :weather_code,
            :pressure_msl,
            :cloud_cover,
            :wind_speed_10m,
            :wind_direction_10m,
            :wind_gusts_10m,
            :source_file
        )
        ON CONFLICT (airport_code, weather_time)
        DO NOTHING
    """)

    records = df.to_dict(orient="records")

    with engine.begin() as connection:
        result = connection.execute(insert_sql, records)

    return result.rowcount

def run_weather_pipeline(config):
    default_start_date = (
        config["weather"]["default_start_date"]
    )

    target_end_date = (
        config["weather"]["target_end_date"]
    )

    hourly_variables = (
        config["weather"]["hourly_variables"]
    )

    for airport in config["airports"]:
        airport_code = airport["code"]
        latitude = airport["latitude"]
        longitude = airport["longitude"]

        last_loaded = get_source_watermark(
            source="OpenMeteo",
            object_name=airport_code,
        )

        if last_loaded is None:
            start_date = default_start_date
        else:
            last_date = datetime.strptime(
                last_loaded,
                "%Y-%m-%d",
            ).date()

            start_date = str(
                last_date
                + timedelta(days=1)
            )

        end_date = target_end_date

        if start_date > end_date:
            print(
                f"Open-Meteo {airport_code}: "
                "новых данных нет."
            )
            continue

        print(
            f"Open-Meteo {airport_code}: "
            f"{start_date} — {end_date}"
        )

        load_id = start_load(
            source="OpenMeteo",
            object_name=airport_code,
            period_from=start_date,
            period_to=end_date,
        )

        try:
            data = download_weather(
                airport_code,
                latitude,
                longitude,
                start_date,
                end_date,
                hourly_variables,
            )

            raw_path = save_raw_json(
                data,
                airport_code,
                start_date,
                end_date,
            )

            df = prepare_weather_dataframe(
                data,
                airport_code,
                raw_path,
            )

            inserted_rows = (
                load_weather_to_bronze(df)
            )

            rows_received = len(df)
            rows_skipped = (
                rows_received
                - inserted_rows
            )

            finish_load_success(
                load_id=load_id,
                rows_received=rows_received,
                rows_inserted=inserted_rows,
                rows_skipped=rows_skipped,
            )

            update_source_watermark(
                source="OpenMeteo",
                object_name=airport_code,
                watermark_type="date",
                watermark_value=end_date,
            )

            print(
                f"Получено: {rows_received}; "
                f"добавлено: {inserted_rows}; "
                f"пропущено: {rows_skipped}"
            )

        except Exception as error:
            finish_load_failed(
                load_id=load_id,
                error_message=error,
            )

            print(
                f"Ошибка Open-Meteo "
                f"{airport_code}: {error}"
            )

            raise