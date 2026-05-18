"""Database helpers."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

import pandas as pd
from sqlalchemy import Engine, create_engine, text

from .schema import all_tables, metadata


def create_db_engine(database_url: str) -> Engine:
    """Create the SQLAlchemy engine."""

    return create_engine(database_url, future=True)


def initialize_database(engine: Engine) -> None:
    """Create all project tables."""

    metadata.create_all(engine, tables=all_tables)


def truncate_table(engine: Engine, table_name: str) -> None:
    """Delete all rows from a table."""

    with engine.begin() as connection:
        connection.execute(text(f"DELETE FROM {table_name}"))


def read_table(engine: Engine, query: str) -> pd.DataFrame:
    """Read a SQL query into a DataFrame."""

    with engine.begin() as connection:
        return pd.read_sql_query(text(query), connection)


def write_dataframe(
    engine: Engine,
    table_name: str,
    frame: pd.DataFrame,
    if_exists: str = "append",
) -> None:
    """Write a DataFrame to a database table."""

    if frame.empty:
        return
    frame.to_sql(table_name, engine, if_exists=if_exists, index=False)


@contextmanager
def begin(engine: Engine) -> Iterator:
    """Yield a transactional connection."""

    with engine.begin() as connection:
        yield connection

