import os

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy import text


load_dotenv()


def get_engine():
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    database = os.getenv("POSTGRES_DB")

    url = (
        f"postgresql+psycopg2://"
        f"{user}:{password}@{host}:{port}/{database}"
    )

    return create_engine(url)

def start_load(source, object_name, period_from=None, period_to=None):
    engine = get_engine()

    sql = text("""
        INSERT INTO meta.load_log (
            source,
            object_name,
            period_from,
            period_to,
            status
        )
        VALUES (
            :source,
            :object_name,
            :period_from,
            :period_to,
            'RUNNING'
        )
        RETURNING load_id
    """)

    with engine.begin() as connection:
        load_id = connection.execute(
            sql,
            {
                "source": source,
                "object_name": object_name,
                "period_from": period_from,
                "period_to": period_to,
            },
        ).scalar_one()

    return load_id

def finish_load_success(
    load_id,
    rows_received,
    rows_inserted,
    rows_skipped,
):
    engine = get_engine()

    sql = text("""
        UPDATE meta.load_log
        SET
            finished_at = CURRENT_TIMESTAMP,
            rows_received = :rows_received,
            rows_inserted = :rows_inserted,
            rows_skipped = :rows_skipped,
            status = 'SUCCESS'
        WHERE load_id = :load_id
    """)

    with engine.begin() as connection:
        connection.execute(
            sql,
            {
                "load_id": load_id,
                "rows_received": rows_received,
                "rows_inserted": rows_inserted,
                "rows_skipped": rows_skipped,
            },
        )

def finish_load_success(
    load_id,
    rows_received,
    rows_inserted,
    rows_skipped,
):
    engine = get_engine()

    sql = text("""
        UPDATE meta.load_log
        SET
            finished_at = CURRENT_TIMESTAMP,
            rows_received = :rows_received,
            rows_inserted = :rows_inserted,
            rows_skipped = :rows_skipped,
            status = 'SUCCESS'
        WHERE load_id = :load_id
    """)

    with engine.begin() as connection:
        connection.execute(
            sql,
            {
                "load_id": load_id,
                "rows_received": rows_received,
                "rows_inserted": rows_inserted,
                "rows_skipped": rows_skipped,
            },
        )

def finish_load_failed(load_id, error_message):
    engine = get_engine()

    sql = text("""
        UPDATE meta.load_log
        SET
            finished_at = CURRENT_TIMESTAMP,
            status = 'FAILED',
            error_message = :error_message
        WHERE load_id = :load_id
    """)

    with engine.begin() as connection:
        connection.execute(
            sql,
            {
                "load_id": load_id,
                "error_message": str(error_message),
            },
        )

def get_source_watermark(source, object_name):
    engine = get_engine()

    sql = text("""
        SELECT watermark_value
        FROM meta.source_state
        WHERE source = :source
          AND object_name = :object_name
    """)

    with engine.connect() as connection:
        result = connection.execute(
            sql,
            {
                "source": source,
                "object_name": object_name,
            },
        ).scalar_one_or_none()

    return result

def update_source_watermark(
    source,
    object_name,
    watermark_type,
    watermark_value,
):
    engine = get_engine()

    sql = text("""
        INSERT INTO meta.source_state (
            source,
            object_name,
            watermark_type,
            watermark_value,
            updated_at
        )
        VALUES (
            :source,
            :object_name,
            :watermark_type,
            :watermark_value,
            CURRENT_TIMESTAMP
        )
        ON CONFLICT (source, object_name)
        DO UPDATE SET
            watermark_type = EXCLUDED.watermark_type,
            watermark_value = EXCLUDED.watermark_value,
            updated_at = CURRENT_TIMESTAMP
    """)

    with engine.begin() as connection:
        connection.execute(
            sql,
            {
                "source": source,
                "object_name": object_name,
                "watermark_type": watermark_type,
                "watermark_value": watermark_value,
            },
        )

