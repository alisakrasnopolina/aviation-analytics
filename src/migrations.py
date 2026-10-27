from pathlib import Path

from sqlalchemy import text

from src.database import get_engine


MIGRATIONS_DIR = Path("sql")


def ensure_migrations_table():
    """
    Создаёт служебную таблицу учёта миграций.

    Это выполняется до запуска миграций, чтобы проект мог
    разворачиваться как на существующей, так и на чистой БД.
    """
    engine = get_engine()

    sql = """
    CREATE SCHEMA IF NOT EXISTS meta;

    CREATE TABLE IF NOT EXISTS meta.schema_migrations (
        version VARCHAR(20) PRIMARY KEY,
        filename VARCHAR(255) NOT NULL,
        applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """

    with engine.begin() as connection:
        connection.exec_driver_sql(sql)


def get_applied_migrations():
    """
    Возвращает множество уже применённых версий миграций.
    """
    engine = get_engine()

    query = text("""
        SELECT version
        FROM meta.schema_migrations
    """)

    with engine.connect() as connection:
        result = connection.execute(query)

        return {
            row[0]
            for row in result
        }


def apply_migration(path: Path):
    """
    Выполняет одну миграцию и регистрирует её
    только после успешного выполнения.
    """
    version = path.name.split("_", 1)[0]
    filename = path.name

    sql = path.read_text(
        encoding="utf-8"
    )

    engine = get_engine()

    with engine.begin() as connection:
        connection.exec_driver_sql(
            sql,
            execution_options={"no_parameters": True},
        )

        connection.execute(
            text("""
                INSERT INTO meta.schema_migrations (
                    version,
                    filename
                )
                VALUES (
                    :version,
                    :filename
                )
            """),
            {
                "version": version,
                "filename": filename,
            },
        )


def run_migrations():
    """
    Последовательно применяет все ещё не выполненные
    SQL-миграции.
    """
    ensure_migrations_table()

    applied = get_applied_migrations()

    migration_files = sorted(
        MIGRATIONS_DIR.glob("*.sql")
    )

    applied_count = 0

    for path in migration_files:
        version = path.name.split("_", 1)[0]

        if version in applied:
            print(
                f"Миграция {version} уже применена."
            )
            continue

        print(
            f"Применение миграции "
            f"{version}: {path.name}"
        )

        apply_migration(path)

        applied.add(version)
        applied_count += 1

        print(
            f"Миграция {version} успешно применена."
        )

    if applied_count == 0:
        print(
            "Новых миграций схемы нет."
        )
    else:
        print(
            f"Применено миграций: "
            f"{applied_count}"
        )


if __name__ == "__main__":
    run_migrations()