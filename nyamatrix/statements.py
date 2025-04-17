import logging
import sqlalchemy.exc
from pathlib import Path
from urllib.parse import urlparse
from sqlalchemy import Engine, text, create_engine


def _read_file(filename: str):
    path = Path(__file__).parent / "statements" / f"{filename}"
    if not path.exists():
        raise FileNotFoundError(f"Statement file not found: {filename}.")
    with open(path, "r", encoding="utf-8") as file:
        return file.read()


def SQL(filename: str):
    return _read_file(f"{filename}.sql")


def test_database_connection(db_uri: str) -> bool:
    parsed_uri = urlparse(db_uri)
    if not parsed_uri.scheme:
        logging.error("Invalid database URI format: missing scheme")
        return False

    try:
        engine = create_engine(db_uri)
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        return True
    except sqlalchemy.exc.SQLAlchemyError as e:
        logging.error(f"Database connection error: {str(e)}")
        return False
    except Exception as e:
        logging.error(f"Unexpected error testing database connection: {str(e)}")
        return False


def fetch_count(engine: Engine, query: str, params: dict | None = None) -> int:
    try:
        with engine.connect() as connection:
            result = connection.execute(text(query), params)
            return (result.fetchone() or [0])[0]
    except sqlalchemy.exc.SQLAlchemyError as e:
        logging.error(f"SQLAlchemy error: {str(e)}")
    except Exception as e:
        logging.error(f"Unexpected error executing query: {str(e)}")
    return 0
