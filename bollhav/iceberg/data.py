"""Iceberg's `@model_lifecycle` asset handler — the sibling of PostgresData
and MssqlData, so table creation happens when the model fires, not when the
writer first sees data.

The model's `Target.writer` says what `conn` must be. `Writer.PYICEBERG`, the
default, takes the pyiceberg Catalog: for an Iceberg pipeline the catalog IS
the data connection. `Writer.TRINO` takes a Trino DB-API connection and is for
views, since pyiceberg's SqlCatalog cannot create views while Trino can, and
only Trino reads them.
"""

import logging
from typing import TYPE_CHECKING, Any, Protocol

from pyiceberg.catalog import Catalog  # pyright: ignore[reportMissingImports]  # optional iceberg extra
from pyiceberg.expressions import AlwaysTrue  # pyright: ignore[reportMissingImports]  # optional iceberg extra
from pyiceberg.partitioning import (  # pyright: ignore[reportMissingImports]  # optional iceberg extra
    UNPARTITIONED_PARTITION_SPEC,
    PartitionField,
    PartitionSpec,
)
from pyiceberg.schema import Schema  # pyright: ignore[reportMissingImports]  # optional iceberg extra
from pyiceberg.transforms import (  # pyright: ignore[reportMissingImports]  # optional iceberg extra
    DayTransform,
    IdentityTransform,
)
from pyiceberg.types import (  # pyright: ignore[reportMissingImports]  # optional iceberg extra
    DateType,
    TimestampType,
    TimestamptzType,
)

from bollhav.iceberg.schema import iceberg_schema
from bollhav.model.writer import Writer

if TYPE_CHECKING:
    from bollhav.model.model import Model

logger = logging.getLogger(__name__)


# ── errors ──────────────────────────────────────────────────────────


class IcebergViewsNotSupportedError(ValueError):
    """`write` was called for a view model. A view has no rows to write: the
    lifecycle creates it through Trino from the model's query."""

    def __init__(self, full_name: str) -> None:
        super().__init__(f"{full_name!r}: a view is created, not written to")


class IcebergViewWithoutBodyError(ValueError):
    """The view model's query resolved to None, so there is no body to create
    the view from."""

    def __init__(self, full_name: str) -> None:
        super().__init__(f"{full_name!r}: the view's query resolved to None")


class IcebergWriterConnectionError(TypeError):
    """`data_conn` does not match the model's declared writer: a
    `Writer.PYICEBERG` model needs the pyiceberg Catalog, a `Writer.TRINO`
    model a Trino DB-API connection."""

    def __init__(self, full_name: str, writer: Writer, got: object) -> None:
        wanted = (
            "the pyiceberg Catalog"
            if writer is Writer.PYICEBERG
            else "a Trino DB-API connection"
        )
        super().__init__(
            f"{full_name!r}: writer={writer.name} needs {wanted} as data_conn, "
            f"got {type(got).__name__}"
        )


class IcebergWrongWriterError(RuntimeError):
    """The handler was asked to do the other writer's work: a catalog operation
    on a `Writer.TRINO` handler, or a Trino statement on a `Writer.PYICEBERG`
    one. Model validation rules this out for tables, so reaching it means the
    handler was driven outside the lifecycle."""

    def __init__(self, full_name: str, writer: Writer, needed: str) -> None:
        super().__init__(
            f"{full_name!r}: a writer={writer.name} handler has no {needed}"
        )


class IcebergStagingNotSupportedError(ValueError):
    """`staging` on an Iceberg target — Iceberg commits are already atomic
    snapshot swaps, so a staging table adds nothing. Drop `staging` from the
    model."""

    def __init__(self, full_name: str) -> None:
        super().__init__(
            f"{full_name!r}: Iceberg targets do not use staging — commits are "
            f"already atomic"
        )


# ── handler ─────────────────────────────────────────────────────────


class DbApiConnection(Protocol):
    """What the handler needs from a Trino connection (`trino.dbapi`)."""

    def cursor(self) -> Any: ...


def _quoted(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


class IcebergData:
    def __init__(self, model: "Model", conn: "Catalog | DbApiConnection") -> None:
        self.model = model
        self.writer = model.target.writer or Writer.PYICEBERG
        self.catalog: Catalog | None = None
        self.trino: DbApiConnection | None = None
        if self.writer is Writer.PYICEBERG:
            if not isinstance(conn, Catalog):
                raise IcebergWriterConnectionError(
                    model.target.full_name, self.writer, conn
                )
            self.catalog = conn
        else:
            if isinstance(conn, Catalog) or not hasattr(conn, "cursor"):
                raise IcebergWriterConnectionError(
                    model.target.full_name, self.writer, conn
                )
            self.trino = conn
        self.identifier = f"{model.target.schema}.{model.target.name}"

    def _catalog(self) -> Catalog:
        if self.catalog is None:
            raise IcebergWrongWriterError(
                self.model.target.full_name, self.writer, "pyiceberg Catalog"
            )
        return self.catalog

    def _execute(self, statement: str) -> None:
        if self.trino is None:
            raise IcebergWrongWriterError(
                self.model.target.full_name, self.writer, "Trino connection"
            )
        logger.debug("Trino: %s", statement)
        cursor = self.trino.cursor()
        try:
            cursor.execute(statement)
            cursor.fetchall()
        finally:
            cursor.close()

    def create_schema(self) -> None:
        target = self.model.target
        if self.catalog is not None:
            if not self.catalog.namespace_exists(target.schema):
                self.catalog.create_namespace(target.schema)
        else:
            self._execute(
                f"CREATE SCHEMA IF NOT EXISTS "
                f"{_quoted(str(target.catalog))}.{_quoted(target.schema)}"
            )

    def create_table(self) -> None:
        if not self._catalog().table_exists(self.identifier):
            schema = iceberg_schema(self.model)
            self._catalog().create_table(
                self.identifier,
                schema=schema,
                partition_spec=self._partition_spec(schema=schema),
                properties=(
                    {"comment": self.model.description}
                    if self.model.description
                    else {}
                ),
            )

    def _partition_spec(self, *, schema: Schema) -> PartitionSpec:
        """The `partition_on` column becomes the table's partition spec: by
        day for date and timestamp columns, by value for anything else.
        Without one the table is unpartitioned."""
        column = self.model.target.partitioned_by
        if column is None:
            return UNPARTITIONED_PARTITION_SPEC
        source = schema.find_field(column)
        if isinstance(source.field_type, (DateType, TimestampType, TimestamptzType)):
            transform, name = DayTransform(), f"{column}_day"
        else:
            transform, name = IdentityTransform(), column
        return PartitionSpec(
            PartitionField(
                source_id=source.field_id,
                field_id=1000,
                transform=transform,
                name=name,
            )
        )

    def recreate_table(self) -> None:
        if self._catalog().table_exists(self.identifier):
            self._catalog().drop_table(self.identifier)
        self.create_table()

    def truncate_table(self) -> None:
        if self._catalog().table_exists(self.identifier):
            self._catalog().load_table(self.identifier).delete(AlwaysTrue())

    def create_or_replace_view(self, body: object) -> None:
        """`CREATE OR REPLACE VIEW` through Trino, with the model's description
        as the view comment. `body` is the model's resolved query, Trino SQL."""
        if body is None:
            raise IcebergViewWithoutBodyError(self.model.target.full_name)
        target = self.model.target
        name = f"{_quoted(str(target.catalog))}.{_quoted(target.schema)}.{_quoted(target.name)}"
        comment = (
            f" COMMENT '{self.model.description.replace(chr(39), chr(39) * 2)}'"
            if self.model.description
            else ""
        )
        self._execute(f"CREATE OR REPLACE VIEW {name}{comment} AS {body}")

    def create_indexes(self) -> None:
        # Iceberg has no indexes. Target rejects declared ones at build time;
        # the lifecycle still calls this for the partition_on column, which
        # only serves the overwrite window filter here.
        return None

    def add_unique_constraint(self) -> None:
        # Iceberg has no constraints; merge keys live on the model and are
        # enforced by upsert's join, not by the table.
        return None

    def create_staging_schema(self) -> None:
        raise IcebergStagingNotSupportedError(self.model.target.full_name)

    def gc_orphan_staging_tables(self) -> None:
        raise IcebergStagingNotSupportedError(self.model.target.full_name)
