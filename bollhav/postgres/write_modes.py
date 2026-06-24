import logging
from datetime import datetime
from typing import Generator
from functools import partial
from psycopg import Connection
import polars as pl
from bollhav.model.write_modes import WriteMode
from bollhav.model.model import Model
from bollhav.model.modelrun import ModelRun
from bollhav.postgres.modes import (
    recreate_partition,
    upsert_no_delete,
    append,
)
from bollhav.postgres.messages.error import (
    RecreatePartitionRequiresWindowError,
    UnhandledWriteModeError,
    WriteOnViewError,
    MissingDataFrameError,
)
from bollhav.postgres.data import PostgresData

logger = logging.getLogger(__name__)


def write_dataframes(
    conn: Connection,
    model: Model,
    df_gen: Generator[pl.DataFrame, None, None],
    since: datetime | None = None,
    until: datetime | None = None,
) -> None:
    """Write a stream of DataFrames to a Postgres table using the model's write mode.

    Iterates over `df_gen` and writes each non-empty DataFrame according to
    `model.target.write_mode`. Empty frames are skipped. Columns are reordered
    to match the model definition before writing.

    Assumes the target assets already exist — the `@model_lifecycle` hook
    ensures them (`PostgresData.ensure_assets`) once before the run; this
    function just writes.

    Args:
        conn: Active psycopg connection.
        model: Model describing the target table and write behaviour.
        df_gen: Generator yielding DataFrames to write.
        since: Start of the overwrite window (UTC). Required for RECREATE_PARTITION.
        until: End of the overwrite window (UTC, exclusive). Required for RECREATE_PARTITION.

    Raises:
        ValueError: If `since`/`until` are missing for RECREATE_PARTITION, or if the
            write mode is not handled.
    """
    match model.target.write_mode:
        case WriteMode.APPEND:
            write_function = append
        case WriteMode.RECREATE_PARTITION:
            if since is None or until is None:
                raise RecreatePartitionRequiresWindowError()
            write_function = partial(recreate_partition, since=since, until=until)
        case WriteMode.UPSERT_NO_DELETE:
            write_function = upsert_no_delete
        case _:
            raise UnhandledWriteModeError(model.target.write_mode)

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
    run: ModelRun,
    df_gen: Generator[pl.DataFrame, None, None] | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
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
        run: ModelRun — `run.model` describes the target/write behaviour;
            `run.run_id` keys the staging table.
        df_gen: Generator yielding DataFrames. Required for all non-VIEW modes.
        since: Start of the overwrite window (timezone-aware; any zone).
            Required for RECREATE_PARTITION and for the staged path.
        until: End of the overwrite window (timezone-aware, exclusive).
            Required for RECREATE_PARTITION and for the staged path.

    Raises:
        ValueError: If `df_gen` is missing for a table mode, or if the
            write mode is VIEW (views are created by `@model_lifecycle`,
            not written here).
    """
    model = run.model
    if model.is_view:
        raise WriteOnViewError(model.target.full_name)

    if model.target.write_mode not in (
        WriteMode.APPEND,
        WriteMode.RECREATE_PARTITION,
        WriteMode.UPSERT_NO_DELETE,
    ):
        raise UnhandledWriteModeError(model.target.write_mode)

    if not df_gen:
        raise MissingDataFrameError()

    if model.target.stage:
        _write_staged(conn=conn, run=run, df_gen=df_gen)
        return

    write_dataframes(
        conn=conn,
        model=model,
        df_gen=df_gen,
        since=since,
        until=until,
    )


def _write_staged(
    conn: Connection,
    run: ModelRun,
    df_gen: Generator[pl.DataFrame, None, None],
) -> None:
    """Staged write — COPY each chunk into the per-interval staging table.

    Just lands rows; the staging table must already exist. Its lifecycle
    (create before the execute, merge into the target + drop after) is
    owned by `@execute_lifecycle` via `PostgresData`. Both sides key the
    table on `run.run_id`, so they target the same one."""
    postgres_data = PostgresData(model=run.model, conn=conn)
    for df in df_gen:
        if len(df) == 0:
            continue
        postgres_data.write_to_staging(run.run_id, df)
