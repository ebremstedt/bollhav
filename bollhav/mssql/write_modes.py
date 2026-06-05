import logging
import pyodbc
import polars as pl
from datetime import datetime
from typing import Generator
from bollhav.model.model import Model
from bollhav.model.write_modes import WriteMode
from bollhav.mssql.modes import append, merge

logger = logging.getLogger(__name__)


def write_dataframes(
    conn: pyodbc.Connection,
    model: Model,
    df_gen: Generator[pl.DataFrame, None, None],
    fast_executemany: bool = True,
) -> None:
    """Write a stream of DataFrames straight to the target table using the
    model's write mode. Empty frames are skipped; columns are reordered to
    the model definition before writing.

    Assumes the target assets already exist.
    """
    match model.target.write_mode:
        case WriteMode.APPEND:
            write_function = append
        case WriteMode.UPSERT_NO_DELETE:
            write_function = merge
        case _:
            raise ValueError(
                f"Unhandled write mode for MSSQL: {model.target.write_mode}"
            )

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
    fast_executemany: bool = True,
) -> None:
    """Write data to MSSQL using the write mode defined on the model.

    Two paths, chosen by `model.target.stage`:

      * **Direct** (`staging is None`, default) — each DataFrame in
        `df_gen` is written straight to the target with fast_executemany.
        A crash mid-stream leaves partial writes in the target.
      * **Staged** (`staging=Staging(...)` or `MssqlStaging(...)`) — each
        DataFrame bulk-inserts into the per-interval staging table. The
        table's lifecycle (create before the execute, atomic apply +
        drop after) is owned by `@execute_lifecycle` via `MssqlData`;
        this function just lands chunks. Both sides key the table on
        `model.run_id`, so they target the same one.

    Either way, the target assets are assumed to already exist —
    `@model_lifecycle` ensures them before the interval loop. Views are
    created there too (`MssqlData.create_or_replace_view`), so a view's
    execute body has nothing to write.

    The staged path requires `model.state is None` (MSSQL has no state
    coordination — `MssqlData` rejects a stateful model).
    """
    if model.is_view:
        raise ValueError(
            f"{model.target.full_name!r} is a VIEW — created by "
            f"@model_lifecycle, not write()."
        )

    if model.target.write_mode not in (
        WriteMode.APPEND,
        WriteMode.UPSERT_NO_DELETE,
        WriteMode.RECREATE_PARTITION,
    ):
        raise ValueError(f"Unhandled write mode for MSSQL: {model.target.write_mode}")

    if not df_gen:
        raise ValueError(
            f"{model.target.write_mode.value} requires a dataframe generator"
        )

    if model.target.stage:
        _write_staged(conn=conn, model=model, df_gen=df_gen)
        return

    write_dataframes(
        conn=conn,
        model=model,
        df_gen=df_gen,
        fast_executemany=fast_executemany,
    )


def _write_staged(
    conn: pyodbc.Connection,
    model: Model,
    df_gen: Generator[pl.DataFrame, None, None],
) -> None:
    from bollhav.mssql.data import MssqlData

    data = MssqlData(model=model, conn=conn)
    for df in df_gen:
        if len(df) == 0:
            continue
        data.write_to_staging(model.run_id, df)
