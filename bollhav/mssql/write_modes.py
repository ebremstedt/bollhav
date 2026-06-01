import logging
import pyodbc
import polars as pl
from datetime import datetime
from typing import Generator
from bollhav.model.model import Model
from bollhav.model.write_modes import WriteMode
from bollhav.mssql.modes import append, merge, create_replace_view
from bollhav.mssql.schema import ensure_schema_table_and_indexes

logger = logging.getLogger(__name__)


def write_dataframes(
    conn: pyodbc.Connection,
    model: Model,
    df_gen: Generator[pl.DataFrame, None, None],
    create_if_missing: bool = True,
    fast_executemany: bool = True,
) -> None:
    match model.target.write_mode:
        case WriteMode.APPEND:
            write_function = append
        case WriteMode.UPSERT_NO_DELETE:
            write_function = merge
        case _:
            raise ValueError(
                f"Unhandled write mode for MSSQL: {model.target.write_mode}"
            )

    if create_if_missing:
        ensure_schema_table_and_indexes(conn=conn, model=model)

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
        write_function(conn=conn, model=model, df=df, fast_executemany=fast_executemany)


def write(
    conn: pyodbc.Connection,
    model: Model,
    df_gen: Generator[pl.DataFrame, None, None] | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
    create_if_missing: bool = True,
    fast_executemany: bool = True,
) -> None:
    """Write data to MSSQL using the write mode defined on the model.

    Two paths, chosen by `model.target.staging`:

      * **Direct** (`staging is None`, default) — each DataFrame in
        `df_gen` is written straight to the target with fast_executemany.
        A crash mid-stream leaves partial writes in the target.
      * **Staged** (`staging=Staging(...)` or `MssqlStaging(...)`) — each
        DataFrame bulk-inserts into a per-interval staging table. After
        the generator drains, one transaction moves staging → target and
        (in INTERVAL mode) drops staging. A crash mid-stream leaves no
        partial writes in the target — staging is GC'd on next run.

    The staged path is APPEND-only and requires `model.state is None`
    (MSSQL state coordination isn't implemented yet — see
    `bollhav.mssql.staging._assert_supported` for the error message).
    """
    if model.target.write_mode == WriteMode.VIEW:
        if df_gen:
            raise ValueError("WriteMode.VIEW does not take a dataframe")
        create_replace_view(conn=conn, model=model)
        return

    if not df_gen:
        raise ValueError(
            f"{model.target.write_mode.value} requires a dataframe generator"
        )

    if model.target.staging is not None:
        if since is None or until is None:
            raise ValueError("since and until are required when target.staging is set")
        _write_staged(
            conn=conn,
            model=model,
            df_gen=df_gen,
            since=since,
            until=until,
            create_if_missing=create_if_missing,
            fast_executemany=fast_executemany,
        )
        return

    write_dataframes(
        conn=conn,
        model=model,
        df_gen=df_gen,
        create_if_missing=create_if_missing,
        fast_executemany=fast_executemany,
    )


def _write_staged(
    conn: pyodbc.Connection,
    model: Model,
    df_gen: Generator[pl.DataFrame, None, None],
    since: datetime,
    until: datetime,
    create_if_missing: bool,
    fast_executemany: bool,
) -> None:
    """Staged write — stream chunks into a per-interval staging table,
    then atomically flush staging → target on exit.

    `fast_executemany` is accepted for API parity with `write()` but
    `stage()` always uses fast_executemany inside `bulk_insert_to_staging`
    — staging tables don't need a slow path."""
    del fast_executemany
    from bollhav.mssql.staging import stage

    if create_if_missing:
        logger.debug("Ensuring schema and table for %s", model.target.full_name)
        ensure_schema_table_and_indexes(conn=conn, model=model)

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
