import logging
from typing import Generator
from functools import partial
from psycopg import Connection
import polars as pl
from bollhav.model.write_modes import WriteMode
from bollhav.model.model import Model

logger = logging.getLogger(__name__)
from bollhav.postgres.modes import (
    recreate_insert,
    truncate_insert,
    overwrite_insert,
    update_insert,
    create_replace_view,
    append,
    ensure_schema_and_table as _ensure_schema_and_table,
)
from datetime import datetime


def write_dataframes(
    conn: Connection,
    model: Model,
    df_gen: Generator[pl.DataFrame, None, None],
    since: datetime | None = None,
    until: datetime | None = None,
    ensure_schema_and_table: bool = True,
) -> None:
    match model.target.write_mode:
        case WriteMode.APPEND:
            write_function = append
        case WriteMode.RECREATE_INSERT:
            write_function = recreate_insert
        case WriteMode.TRUNCATE_INSERT:
            write_function = truncate_insert
        case WriteMode.OVERWRITE_INSERT:
            if since is None or until is None:
                raise ValueError("Since and until must be set for OVERWRITE_INSERT")
            write_function = partial(overwrite_insert, since=since, until=until)
        case WriteMode.UPDATE_INSERT:
            write_function = update_insert
        case _:
            raise ValueError(f"Unhandled write mode: {model.target.write_mode}")

    if ensure_schema_and_table:
        logger.debug("Ensuring schema and table for %s", model.target.full_name)
        _ensure_schema_and_table(conn=conn, model=model)

    for df in df_gen:
        if len(df) == 0:
            continue
        df = df.select([col.name for col in model.target.columns])
        logger.debug(
            "Writing %d rows to %s (%s)",
            len(df),
            model.target.full_name,
            model.target.write_mode.value,
        )
        write_function(conn=conn, model=model, df=df)


def write(
    conn: Connection,
    model: Model,
    df_gen: Generator[pl.DataFrame, None, None] | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
    ensure_schema_and_table: bool = True,
) -> None:
    if model.target.write_mode in (
        WriteMode.APPEND,
        WriteMode.RECREATE_INSERT,
        WriteMode.TRUNCATE_INSERT,
        WriteMode.OVERWRITE_INSERT,
        WriteMode.UPDATE_INSERT,
    ):
        if not df_gen:
            raise ValueError(
                "Modes APPEND, RECREATE_INSERT, TRUNCATE_INSERT, OVERWRITE_INSERT, UPDATE_INSERT need a dataframe"
            )
        write_dataframes(
            conn=conn,
            model=model,
            df_gen=df_gen,
            since=since,
            until=until,
            ensure_schema_and_table=ensure_schema_and_table,
        )
    if model.target.write_mode == WriteMode.VIEW:
        if df_gen:
            raise ValueError("Modes VIEW does not need a dataframe")
        create_replace_view(conn=conn, model=model)
