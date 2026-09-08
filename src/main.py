from src.config import load_config
from src.ingestion.bts import run_bts_pipeline
from src.ingestion.weather import run_weather_pipeline


def main():
    config = load_config()

    print("=" * 60)
    print("Загрузка данных BTS")
    print("=" * 60)

    run_bts_pipeline(config)

    print()

    print("=" * 60)
    print("Загрузка данных Open-Meteo")
    print("=" * 60)

    run_weather_pipeline(config)

    print()

    print("=" * 60)
    print("Загрузка данных завершена")
    print("=" * 60)


if __name__ == "__main__":
    main()