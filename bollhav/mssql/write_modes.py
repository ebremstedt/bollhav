import logging
import pyodbc
import polars as pl
from typing import Generator
from bollhav.model.model import Model
from bollhav.model.write_modes import WriteMode
from bollhav.mssql.modes import append, merge, truncate_write, create_replace_view
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
        case WriteMode.TRUNCATE_TABLE_INSERT:
            write_function = truncate_write
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
    create_if_missing: bool = True,
    fast_executemany: bool = True,
) -> None:
    if model.target.write_mode == WriteMode.VIEW:
        if df_gen:
            raise ValueError("WriteMode.VIEW does not take a dataframe")
        create_replace_view(conn=conn, model=model)
        return

    if not df_gen:
        raise ValueError(
            f"{model.target.write_mode.value} requires a dataframe generator"
        )
    write_dataframes(
        conn=conn,
        model=model,
        df_gen=df_gen,
        create_if_missing=create_if_missing,
        fast_executemany=fast_executemany,
    )
