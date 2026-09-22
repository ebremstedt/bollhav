"""The Iceberg write operations, named as Iceberg names them — append,
upsert, overwrite — the way the mssql module's `merge` speaks MSSQL."""

import logging
from datetime import datetime

import pyarrow as pa  # pyright: ignore[reportMissingImports]  # optional iceberg extra
from pyiceberg.expressions import (  # pyright: ignore[reportMissingImports]  # optional iceberg extra
    And,
    GreaterThanOrEqual,
    LessThan,
)
from pyiceberg.table import Table  # pyright: ignore[reportMissingImports]  # optional iceberg extra

from bollhav.model.model import Model

logger = logging.getLogger(__name__)


# ── errors ──────────────────────────────────────────────────────────


class UpsertRequiresMergeKeysError(ValueError):
    """`upsert` joins on the model's merge key columns (primary key / unique),
    and the model declares none."""

    def __init__(self, full_name: str) -> None:
        super().__init__(
            f"upsert on {full_name!r} requires primary key / unique columns to join on"
        )


class OverwriteRequiresPartitionColumnError(ValueError):
    """`overwrite` replaces a time window, which needs the column that carries
    the window — mark it with `partition_on=True`."""

    def __init__(self, full_name: str) -> None:
        super().__init__(
            f"overwrite on {full_name!r} requires a column with partition_on=True"
        )


# ── operations ──────────────────────────────────────────────────────


def append(table: Table, model: Model, arrow: pa.Table) -> None:
    table.append(arrow)


def upsert(table: Table, model: Model, arrow: pa.Table) -> None:
    join_cols = [column.name for column in model.target.merge_key_columns]
    if not join_cols:
        raise UpsertRequiresMergeKeysError(model.target.full_name)
    table.upsert(arrow, join_cols=join_cols)


def overwrite(
    table: Table, model: Model, arrow: pa.Table, since: datetime, until: datetime
) -> None:
    """Replace the `[since, until)` window with `arrow` — delete + append in a
    single atomic Iceberg commit (`overwrite_filter`)."""
    partition_column = model.target.partitioned_by
    if partition_column is None:
        raise OverwriteRequiresPartitionColumnError(model.target.full_name)
    table.overwrite(
        arrow,
        overwrite_filter=And(
            GreaterThanOrEqual(partition_column, since.isoformat()),
            LessThan(partition_column, until.isoformat()),
        ),
    )
