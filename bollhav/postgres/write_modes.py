import logging
from datetime import datetime
from typing import Generator
from functools import partial
from psycopg import Connection
import polars as pl
from bollhav.model.write_modes import WriteMode
from bollhav.model.model import Model
from bollhav.postgres.modes import (
    recreate_partition,
    upsert_no_delete,
    create_replace_view,
    append,
)
from bollhav.postgres.actions import run_pre_model_actions

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
        case WriteMode.RECREATE_PARTITION:
            if since is None or until is None:
                raise ValueError("Since and until must be set for RECREATE_PARTITION")
            write_function = partial(recreate_partition, since=since, until=until)
        case WriteMode.UPSERT_NO_DELETE:
            write_function = upsert_no_delete
        case _:
            raise ValueError(f"Unhandled write mode: {model.target.write_mode}")

    if create_if_missing:
        logger.debug("Ensuring schema and table for %s", model.target.full_name)
        run_pre_model_actions(conn=conn, model=model)

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

    Two paths, chosen by `model.target.staging`:

      * **Direct** (`staging is None`, default) — each DataFrame in
        `df_gen` is written straight to the target in its own
        transaction. Fast, simple, but a crash mid-stream leaves
        partial writes in the target.
      * **Staged** (`staging=Staging(...)`) — each DataFrame COPYs
        into a per-interval staging table. After the generator drains,
        one transaction moves staging → target, drops staging, and
        flips the model's state row to `applied`. A crash mid-stream
        leaves no partial writes in the target (staging is GC'd on
        next run); the state row stays `pending` and the interval
        reruns.

    The staged path requires `model.state = State(...)` and is currently
    APPEND-only (enforced by `bollhav.postgres.staging.stage`).

    Args:
        conn: Active psycopg connection.
        model: Model describing the target and write behaviour.
        df_gen: Generator yielding DataFrames. Required for all non-VIEW modes.
        since: Start of the overwrite window (UTC). Required for
            RECREATE_PARTITION and for the staged path.
        until: End of the overwrite window (UTC, exclusive). Required for
            RECREATE_PARTITION and for the staged path.
        create_if_missing: If True, create the schema and table before writing.

    Raises:
        ValueError: If `df_gen` is missing for a table mode, provided for
            VIEW mode, or if `since`/`until` are missing for the staged path.
    """
    if model.target.write_mode == WriteMode.VIEW:
        if df_gen:
            raise ValueError("Modes VIEW does not need a dataframe")
        create_replace_view(conn=conn, model=model)
        return

    if model.target.write_mode not in (
        WriteMode.APPEND,
        WriteMode.RECREATE_PARTITION,
        WriteMode.UPSERT_NO_DELETE,
    ):
        raise ValueError(f"Unhandled write mode: {model.target.write_mode}")

    if not df_gen:
        raise ValueError(
            "Modes APPEND, RECREATE_PARTITION, UPSERT_NO_DELETE need a dataframe"
        )

    if model.target.staging is not None:
        # Staged path: chunks COPY into staging table; final tx atomically
        # moves staging → target and flips the state row to applied.
        if since is None or until is None:
            raise ValueError(
                "since and until are required when target.staging is set — "
                "they identify the state row to flip on flush"
            )
        _write_staged(
            conn=conn,
            model=model,
            df_gen=df_gen,
            since=since,
            until=until,
            create_if_missing=create_if_missing,
        )
        return

    # Direct path: each chunk writes straight to target, one tx per chunk.
    write_dataframes(
        conn=conn,
        model=model,
        df_gen=df_gen,
        since=since,
        until=until,
        create_if_missing=create_if_missing,
    )


def _write_staged(
    conn: Connection,
    model: Model,
    df_gen: Generator[pl.DataFrame, None, None],
    since: datetime,
    until: datetime,
    create_if_missing: bool,
) -> None:
    """Staged write — stream chunks into a per-interval staging table,
    then atomically flush staging → target + flip state row on exit.

    Delegates to `bollhav.postgres.staging.stage` for the heavy lifting;
    this function is just the glue between `write()`'s generator-based
    API and `stage()`'s `.write(df)` API."""
    from bollhav.postgres.staging import stage

    if create_if_missing:
        logger.debug("Ensuring schema and table for %s", model.target.full_name)
        run_pre_model_actions(conn=conn, model=model)

    with stage(conn, model, since=since, until=until) as s:
        for df in df_gen:
            if len(df) == 0:
                continue
            df = df.select([col.name for col in model.target.columns])
            logger.debug(
                "Staging %d rows for %s (interval %s..%s)",
                len(df),
                model.target.full_name,
                since,
                until,
            )
            s.write(df)
