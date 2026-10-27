from src.history import bind_history_context
from pathlib import Path

from src.database import get_engine


TRANSFORMATIONS_DIR = Path(
    "transformations"
)


def execute_sql_file(filename):
    path = TRANSFORMATIONS_DIR / filename

    sql = path.read_text(
        encoding="utf-8"
    )

    engine = get_engine()

    with engine.begin() as connection:
        bind_history_context(connection,filename)
        connection.exec_driver_sql(sql)


def build_silver():
    print("Формирование слоя Silver")

    execute_sql_file("silver.sql")

    print("Silver успешно сформирован.")


def build_gold():
    print("Формирование слоя Gold")

    execute_sql_file("gold.sql")

    print("Gold успешно сформирован.")


def run_transformations():
    build_silver()
    build_gold()


if __name__ == "__main__":
    run_transformations()