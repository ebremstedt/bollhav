import logging
from datetime import datetime
from typing import Generator
from functools import partial
from psycopg import Connection
import polars as pl
from bollhav.model.write_modes import WriteMode
from bollhav.model.model import Model
from bollhav.postgres.modes import (
    recreate_insert,
    truncate_insert,
    overwrite_insert,
    update_insert,
    create_replace_view,
    append,
)
from bollhav.postgres.schema import ensure_schema_and_table

logger = logging.getLogger(__name__)


def write_dataframes(
    conn: Connection,
    model: Model,
    df_gen: Generator[pl.DataFrame, None, None],
    since: datetime | None = None,
    until: datetime | None = None,
    create_if_missing: bool = True,
) -> None:
    """Write a stream of DataFrames to a Postgres table using the model's write mode.

    Iterates over `df_gen` and writes each non-empty DataFrame according to
    `model.target.write_mode`. Empty frames are skipped. Columns are reordered
    to match the model definition before writing.

    Args:
        conn: Active psycopg connection.
        model: Model describing the target table and write behaviour.
        df_gen: Generator yielding DataFrames to write.
        since: Start of the overwrite window (UTC). Required for RECREATE_PARTITION.
        until: End of the overwrite window (UTC, exclusive). Required for RECREATE_PARTITION.
        create_if_missing: If True, create the schema and table before writing.

    Raises:
        ValueError: If `since`/`until` are missing for RECREATE_PARTITION, or if the
            write mode is not handled.
    """
    match model.target.write_mode:
        case WriteMode.APPEND:
            write_function = append
        case WriteMode.RECREATE_TABLE_INSERT:
            write_function = recreate_insert
        case WriteMode.TRUNCATE_TABLE_INSERT:
            write_function = truncate_insert
        case WriteMode.RECREATE_PARTITION:
            if since is None or until is None:
                raise ValueError("Since and until must be set for RECREATE_PARTITION")
            write_function = partial(overwrite_insert, since=since, until=until)
        case WriteMode.UPDATE_INSERT:
            write_function = update_insert
        case _:
            raise ValueError(f"Unhandled write mode: {model.target.write_mode}")

    if create_if_missing:
        logger.debug("Ensuring schema and table for %s", model.target.full_name)
        ensure_schema_and_table(conn=conn, model=model)

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
    create_if_missing: bool = True,
) -> None:
    """Write data to Postgres using the write mode defined on the model.

    Routes to `write_dataframes` for table-based modes, or `create_replace_view`
    for VIEW mode. Validates that a DataFrame generator is provided (or not) as
    appropriate for the selected mode.

    Args:
        conn: Active psycopg connection.
        model: Model describing the target and write behaviour.
        df_gen: Generator yielding DataFrames. Required for all non-VIEW modes.
        since: Start of the overwrite window (UTC). Required for RECREATE_PARTITION.
        until: End of the overwrite window (UTC, exclusive). Required for RECREATE_PARTITION.
        create_if_missing: If True, create the schema and table before writing.

    Raises:
        ValueError: If `df_gen` is missing for a table mode, or provided for VIEW mode.
    """
    if model.target.write_mode in (
        WriteMode.APPEND,
        WriteMode.RECREATE_TABLE_INSERT,
        WriteMode.TRUNCATE_TABLE_INSERT,
        WriteMode.RECREATE_PARTITION,
        WriteMode.UPDATE_INSERT,
    ):
        if not df_gen:
            raise ValueError(
                "Modes APPEND, RECREATE_TABLE_INSERT, TRUNCATE_TABLE_INSERT, RECREATE_PARTITION, UPDATE_INSERT need a dataframe"
            )
        write_dataframes(
            conn=conn,
            model=model,
            df_gen=df_gen,
            since=since,
            until=until,
            create_if_missing=create_if_missing,
        )
    if model.target.write_mode == WriteMode.VIEW:
        if df_gen:
            raise ValueError("Modes VIEW does not need a dataframe")
        create_replace_view(conn=conn, model=model)
