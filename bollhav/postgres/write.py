from typing import Generator
from functools import partial
from psycopg import Connection
import polars as pl
from bollhav.model import Model
from bollhav.modes import WriteMode
from modes.append import append
from modes.truncate_insert import truncate_insert
from modes.overwrite_insert import overwrite_insert
from modes.update_insert import update_insert
from modes.view import create_view
from datetime import datetime


def write(
    conn: Connection,
    df_gen: Generator[pl.DataFrame, None, None],
    model: Model,
    since: datetime | None = None,
    until: datetime | None = None,
    filter_column: str | None = None,
) -> None:
    match model.write_mode:
        case WriteMode.VIEW:
            write_using_mode = create_view
        case WriteMode.APPEND:
            write_using_mode = append
        case WriteMode.TRUNCATE_INSERT:
            write_using_mode = truncate_insert
        case WriteMode.OVERWRITE_INSERT:
            write_using_mode = partial(
                overwrite_insert, since=since, until=until, filter_column=filter_column
            )
        case WriteMode.UPDATE_INSERT:
            write_using_mode = update_insert
        case _:
            raise ValueError(f"Unhandled write mode: {model.write_mode}")

    try:
        with conn:
            for df in df_gen:
                if len(df) == 0:
                    continue
                write_using_mode(conn=conn, model=model, df=df)
    finally:
        conn.close()
