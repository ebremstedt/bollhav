from typing import Generator
from functools import partial
from psycopg import Connection
import polars as pl
from bollhav.model import Model
from bollhav.modes import WriteMode
from modes import truncate_insert, overwrite_insert, update_insert, create_view, append
from datetime import datetime


def write(
    conn: Connection,
    df_gen: Generator[pl.DataFrame, None, None],
    model: Model,
    since: datetime | None = None,
    until: datetime | None = None,
) -> None:
    match model.write_mode:
        case WriteMode.VIEW:
            write_function = create_view
        case WriteMode.APPEND:
            write_function = append
        case WriteMode.TRUNCATE_INSERT:
            write_function = truncate_insert
        case WriteMode.OVERWRITE_INSERT:
            write_function = partial(overwrite_insert, since=since, until=until)
        case WriteMode.UPDATE_INSERT:
            write_function = partial(update_insert, since=since, until=until)
        case _:
            raise ValueError(f"Unhandled write mode: {model.write_mode}")

    for df in df_gen:
        if len(df) == 0:
            continue
        write_function(conn=conn, model=model, df=df)
