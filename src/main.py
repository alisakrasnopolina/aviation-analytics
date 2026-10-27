from src.quality import assert_quality
from src.config import load_config
from src.ingestion.bts import run_bts_pipeline
from src.ingestion.weather import run_weather_pipeline
from src.migrations import run_migrations
from src.transform import run_transformations


def print_section(title):
    print()
    print(title)


def main():
    print_section(
        "Запуск прогнозно-аналитического конвейера"
    )

    # Загружаем настройки проекта
    config = load_config()

    # 1. Проверяем и применяем изменения схемы БД
    print_section(
        "1. Проверка миграций базы данных"
    )
    run_migrations()

    # 2. Загружаем данные BTS в Bronze
    print_section(
        "2. Загрузка данных BTS"
    )
    run_bts_pipeline(config)

    # 3. Загружаем данные Open-Meteo в Bronze
    print_section(
        "3. Загрузка данных Open-Meteo"
    )
    run_weather_pipeline(config)

    # 4. Формируем Silver и Gold
    print_section(
        "4. Обработка данных Bronze -> Silver -> Gold"
    )
    run_transformations()
    assert_quality(source='main_after_gold')

    print_section(
        "Конвейер обработки данных успешно завершён"
    )


if __name__ == "__main__":
    main()