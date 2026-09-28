import logging
from datetime import datetime
from functools import partial
from typing import Generator

import polars as pl
import pyarrow as pa  # pyright: ignore[reportMissingImports]  # optional iceberg extra
from pyiceberg.catalog import Catalog  # pyright: ignore[reportMissingImports]  # optional iceberg extra
from pyiceberg.table import Table  # pyright: ignore[reportMissingImports]  # optional iceberg extra

from bollhav.iceberg.data import IcebergViewsNotSupportedError
from bollhav.iceberg.modes import append, overwrite, upsert
from bollhav.iceberg.schema import arrow_schema
from bollhav.model.model import Model
from bollhav.model.modelrun import ModelRun
from bollhav.model.write_modes import WriteMode

logger = logging.getLogger(__name__)


# ── errors ──────────────────────────────────────────────────────────


class UnhandledWriteModeError(ValueError):
    """The model's `write_mode` isn't one the Iceberg writer can handle. Raised
    when dispatching a write so an unsupported mode fails loudly."""

    def __init__(self, write_mode: object) -> None:
        super().__init__(f"Unhandled write mode for Iceberg: {write_mode}")


class RecreatePartitionRequiresWindowError(ValueError):
    """RECREATE_PARTITION replaces a `[since, until)` window, so the window
    bounds are required."""

    def __init__(self) -> None:
        super().__init__("RECREATE_PARTITION requires since and until")


class MissingDataFrameError(ValueError):
    """`write` was called without `df_gen` — a table write needs the rows."""

    def __init__(self) -> None:
        super().__init__("df_gen is required to write a table")


# ── helpers ─────────────────────────────────────────────────────────


def _identifier(model: Model) -> str:
    # Iceberg namespace.table — the model's schema is the namespace; the
    # model's catalog names the pyiceberg Catalog, which the caller connects.
    return f"{model.target.schema}.{model.target.name}"


def _as_arrow(chunk: pl.DataFrame | pa.Table, schema: pa.Schema) -> pa.Table:
    """Normalize one chunk: polars -> arrow (near zero-copy), columns selected
    in the declared order, then cast to the model's declared arrow schema — so
    chunk-to-chunk inference drift (all-NULL columns, int32 vs int64, a
    non-UTC timezone) never reaches the table."""
    if isinstance(chunk, pl.DataFrame):
        chunk = chunk.to_arrow()
    return chunk.select(schema.names).cast(schema)


# ── writes ──────────────────────────────────────────────────────────


def write_dataframes(
    catalog: Catalog,
    model: Model,
    df_gen: Generator[pl.DataFrame | pa.Table, None, None],
    since: datetime | None = None,
    until: datetime | None = None,
) -> None:
    """Write a stream of chunks to the model's Iceberg table using the model's
    write mode. Chunks may be polars DataFrames or pyarrow Tables — polars is
    converted with the (near) zero-copy `to_arrow()`. Empty chunks are skipped;
    every chunk is selected and cast to the model's DECLARED schema
    (IcebergColumn types), which also normalizes tz-aware timestamps to UTC
    (Iceberg's timestamptz is UTC-only).

    Assumes the target assets already exist — `@model_lifecycle` ensures them
    (`IcebergData`) when the model fires; this function just writes.

    Mode mapping (each chunk is one Iceberg commit):

      * APPEND             -> `append` per chunk.
      * UPSERT_NO_DELETE   -> `upsert` per chunk, joined on the model's merge
                              key columns.
      * RECREATE_PARTITION -> first chunk `overwrite`s the `[since, until)`
                              window (delete + append in one atomic commit),
                              remaining chunks `append`. Requires the window
                              and a `partition_on=True` column.
    """
    # Resolve the writers before pulling any chunk, so a bad mode or a missing
    # window fails before the read starts (same shape as postgres.write_modes).
    match model.target.write_mode:
        case WriteMode.APPEND:
            first_write = rest_write = append
        case WriteMode.UPSERT_NO_DELETE:
            first_write = rest_write = upsert
        case WriteMode.RECREATE_PARTITION:
            if since is None or until is None:
                raise RecreatePartitionRequiresWindowError()
            # first chunk replaces the window, the remaining chunks append into it
            first_write = partial(overwrite, since=since, until=until)
            rest_write = append
        case _:
            raise UnhandledWriteModeError(model.target.write_mode)

    declared = arrow_schema(model)
    table: Table | None = None
    write_chunk = first_write
    for chunk in df_gen:
        arrow = _as_arrow(chunk, declared)
        if arrow.num_rows == 0:
            continue
        if table is None:
            table = catalog.load_table(_identifier(model))
        logger.debug(
            "Writing %d rows to %s (%s)",
            arrow.num_rows,
            model.target.full_name,
            model.target.write_mode.value,
        )
        write_chunk(table, model, arrow)
        write_chunk = rest_write


def write(
    conn: Catalog,
    run: ModelRun,
    df_gen: Generator[pl.DataFrame | pa.Table, None, None] | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
) -> None:
    """Write data to Iceberg using the write mode defined on the model — the
    Iceberg counterpart of `bollhav.postgres.write`, with the same signature,
    so an execute reads as read -> transform -> write on either backend.

    There is no staged path: every chunk is already its own atomic Iceberg
    commit. Views are created by `@model_lifecycle`, not written here (and an
    Iceberg target does not support them).

    Args:
        conn: The pyiceberg Catalog the model's table lives in.
        run: ModelRun — `run.model` describes the target and write behaviour.
        df_gen: Generator yielding polars DataFrames or arrow Tables.
        since: Start of the overwrite window (timezone-aware). Required for
            RECREATE_PARTITION.
        until: End of the overwrite window (timezone-aware, exclusive).
            Required for RECREATE_PARTITION.

    Raises:
        IcebergViewsNotSupportedError: If the model is a view.
        MissingDataFrameError: If `df_gen` is missing.
    """
    model = run.model
    if model.is_view:
        raise IcebergViewsNotSupportedError(model.target.full_name)
    if df_gen is None:
        raise MissingDataFrameError()

    write_dataframes(
        catalog=conn,
        model=model,
        df_gen=df_gen,
        since=since,
        until=until,
    )
