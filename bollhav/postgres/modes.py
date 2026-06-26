import logging
import psycopg
from psycopg import sql
import polars as pl
from typing import cast, LiteralString
from datetime import datetime
from bollhav.model.model import Model
from bollhav.postgres.columns import PostgresColumn
from bollhav.postgres.errors import PostgresError
from bollhav.postgres.schema import ensure_schema

logger = logging.getLogger(__name__)


# ── errors ──
class NaiveDatetimeError(PostgresError):
    """A datetime that must be an unambiguous instant was naive. Postgres
    compares `timestamptz` by instant, so any zone is fine — but a naive value's
    instant depends on the session timezone, so it's rejected."""

    def __init__(self, name: str, dt) -> None:
        super().__init__(f"{name} must be timezone-aware, got naive {dt!r}")


class RecreatePartitionRequiresPartitionColumnError(PostgresError):
    """`recreate_partition` was called on a target with no partition column —
    there's no window column to DELETE/INSERT against. The target needs a column
    with `partition_on=True`."""

    def __init__(self, full_name: str) -> None:
        super().__init__(
            f"recreate_partition requires model.target to have a column with "
            f"partition_on=True (got none on {full_name!r})"
        )


class CreateReplaceViewRequiresSourceModelError(PostgresError):
    """`create_replace_view` found no upstream Source carrying a `SourceModel`
    with a `.query`. A view is defined by that query, so without it there's
    nothing to create."""

    def __init__(self, full_name: str) -> None:
        super().__init__(
            f"create_replace_view requires a Source with a SourceModel type "
            f"whose .query is set, in upstream=[...] on "
            f"{full_name!r}"
        )


def _assert_aware(dt: datetime, name: str) -> None:
    # A timezone-aware datetime is an unambiguous instant, and Postgres compares
    # `timestamptz` by instant — so any zone is allowed, not just UTC. Only a
    # naive datetime is rejected: its instant depends on the session timezone.
    if dt.tzinfo is None:
        raise NaiveDatetimeError(name, dt)


def append(
    conn: psycopg.Connection,
    model: Model,
    df: pl.DataFrame,
) -> None:
    col_names = sql.SQL(", ").join(sql.Identifier(c) for c in df.columns)
    query = sql.SQL("COPY {schema}.{table} ({cols}) FROM STDIN").format(
        schema=sql.Identifier(model.target.schema_resolved),
        table=sql.Identifier(model.target.name_resolved),
        cols=col_names,
    )
    with conn.transaction():
        with conn.cursor() as cursor:
            with cursor.copy(query) as copy:
                for row in df.rows():
                    copy.write_row(row)


def recreate_partition(
    conn: psycopg.Connection,
    model: Model,
    df: pl.DataFrame,
    since: datetime,
    until: datetime,
) -> None:
    _assert_aware(since, "since")
    _assert_aware(until, "until")
    partition_col = model.target.partitioned_by
    if partition_col is None:
        raise RecreatePartitionRequiresPartitionColumnError(model.target.full_name)
    with conn.transaction():
        conn.execute(
            sql.SQL(
                "DELETE FROM {schema}.{table} WHERE {col} >= %s AND {col} < %s"
            ).format(
                schema=sql.Identifier(model.target.schema_resolved),
                table=sql.Identifier(model.target.name_resolved),
                col=sql.Identifier(partition_col),
            ),
            [since, until],
        )
        col_names = sql.SQL(", ").join(sql.Identifier(c) for c in df.columns)
        copy_query = sql.SQL("COPY {schema}.{table} ({cols}) FROM STDIN").format(
            schema=sql.Identifier(model.target.schema_resolved),
            table=sql.Identifier(model.target.name_resolved),
            cols=col_names,
        )
        with conn.cursor().copy(copy_query) as copy:
            for row in df.rows():
                copy.write_row(row)


def upsert_no_delete(conn: psycopg.Connection, model: Model, df: pl.DataFrame) -> None:
    unique_columns = [col.name for col in model.target.unique_columns]
    temp_table = f"temp_{model.target.name_resolved}"

    col_names = sql.SQL(", ").join(
        sql.Identifier(col.name) for col in model.target.columns
    )
    pk_cols = sql.SQL(", ").join(sql.Identifier(c) for c in unique_columns)
    update_set = sql.SQL(", ").join(
        sql.SQL("{col} = EXCLUDED.{col}").format(col=sql.Identifier(col.name))
        for col in model.target.columns
        if col.name not in unique_columns
    )

    pg_cols = [c for c in model.target.columns if isinstance(c, PostgresColumn)]
    col_defs = sql.SQL(", ").join(
        sql.SQL("{name} {type}").format(
            name=sql.Identifier(col.name),
            type=sql.SQL(col.data_type.value),
        )
        for col in pg_cols
    )

    with conn.transaction():
        # Cleanup leftover from a previous failed run
        conn.execute(
            sql.SQL("DROP TABLE IF EXISTS {temp}").format(
                temp=sql.Identifier(temp_table)
            )
        )
        conn.execute(
            sql.SQL("CREATE TEMP TABLE {temp} ({col_defs}) ON COMMIT DROP").format(
                temp=sql.Identifier(temp_table),
                col_defs=col_defs,
            )
        )
        with conn.cursor().copy(
            sql.SQL("COPY {temp} ({cols}) FROM STDIN").format(
                temp=sql.Identifier(temp_table),
                cols=col_names,
            )
        ) as copy:
            for row in df.rows():
                copy.write_row(row)
        conn.execute(
            sql.SQL(
                "INSERT INTO {schema}.{table} ({cols}) "
                "SELECT {cols} FROM {temp} t "
                "ON CONFLICT ({pk_cols}) DO UPDATE SET {update_set}"
            ).format(
                schema=sql.Identifier(model.target.schema_resolved),
                table=sql.Identifier(model.target.name_resolved),
                cols=col_names,
                temp=sql.Identifier(temp_table),
                pk_cols=pk_cols,
                update_set=update_set,
            )
        )


def create_replace_view(
    conn: psycopg.Connection,
    model: Model,
) -> None:
    from bollhav.model.source import SourceModel

    src = next(
        (
            s
            for s in model.upstream
            if isinstance(s.type, SourceModel) and s.type.query is not None
        ),
        None,
    )
    source = src.type if src is not None else None
    if not isinstance(source, SourceModel) or source.query is None:
        raise CreateReplaceViewRequiresSourceModelError(model.target.full_name)

    with conn.transaction():
        ensure_schema(conn, model.target.schema_resolved)
        conn.execute(
            sql.SQL("CREATE OR REPLACE VIEW {schema}.{view} AS {query}").format(
                schema=sql.Identifier(model.target.schema_resolved),
                view=sql.Identifier(model.target.name_resolved),
                query=sql.SQL(cast(LiteralString, source.query)),
            )
        )
