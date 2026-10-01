import logging
from datetime import datetime
import pyarrow as pa  # pyright: ignore[reportMissingImports]  # optional iceberg extra
from pyiceberg.expressions import (  # pyright: ignore[reportMissingImports]  # optional iceberg extra
    And,
    GreaterThanOrEqual,
    In,
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


class UpsertDuplicateKeysError(ValueError):
    """A chunk handed to `upsert` carries the same merge key more than once, so
    which of those rows should win is undefined. Deduplicate before writing."""

    def __init__(self, full_name: str) -> None:
        super().__init__(
            f"upsert on {full_name!r}: the chunk has duplicate rows for the "
            f"merge key columns"
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
    if len(join_cols) == 1:
        table.upsert(arrow, join_cols=join_cols)
    else:
        _upsert_composite(table, model, arrow, join_cols)


def _upsert_composite(
    table: Table, model: Model, arrow: pa.Table, join_cols: list[str]
) -> None:
    """Upsert on a composite key without pyiceberg's `Table.upsert`. That one
    matches a composite key with an `a = x AND b = y` term per row, OR-ed
    together, and the process dies with a segfault (not an exception) once a
    chunk holds some thousands of rows — well inside the default `Batch.size`.

    Here the target rows that *could* match are found with one `IN` per key
    column: a superset of the real matches, since it also takes rows that
    pair one row's `a` with another row's `b`. That superset is then replaced
    in a single atomic commit (`overwrite_filter`, as in `overwrite`) by the
    chunk plus the rows of the superset the chunk does not carry."""
    keys = list(zip(*(arrow.column(name).to_pylist() for name in join_cols)))
    incoming = set(keys)
    if len(incoming) != len(keys):
        raise UpsertDuplicateKeysError(model.target.full_name)

    may_match = And(
        *[
            In(name, arrow.column(name).unique().to_pylist())  # pyright: ignore[reportCallIssue]
            for name in join_cols
        ]
    )
    existing = table.scan(row_filter=may_match).to_arrow()
    if existing.num_rows == 0:
        table.append(arrow)
        return

    existing_keys = zip(*(existing.column(name).to_pylist() for name in join_cols))
    untouched = existing.filter(
        pa.array([key not in incoming for key in existing_keys], pa.bool_())
    )
    table.overwrite(
        pa.concat_tables(
            [untouched.select(arrow.schema.names).cast(arrow.schema), arrow]
        ),
        overwrite_filter=may_match,
    )


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
            GreaterThanOrEqual(partition_column, since.isoformat()),  # pyright: ignore[reportCallIssue]
            LessThan(partition_column, until.isoformat()),  # pyright: ignore[reportCallIssue]
        ),
    )
