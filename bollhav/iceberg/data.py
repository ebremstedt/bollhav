"""Iceberg's `@model_lifecycle` asset handler — the sibling of PostgresData
and MssqlData, so table creation happens when the model fires, not when the
writer first sees data. The `conn` is the pyiceberg Catalog: for an Iceberg
pipeline the catalog IS the data connection.
"""

import logging
from typing import TYPE_CHECKING

from pyiceberg.catalog import Catalog  # pyright: ignore[reportMissingImports]  # optional iceberg extra
from pyiceberg.expressions import AlwaysTrue  # pyright: ignore[reportMissingImports]  # optional iceberg extra

from bollhav.iceberg.schema import iceberg_schema

if TYPE_CHECKING:
    from bollhav.model.model import Model

logger = logging.getLogger(__name__)


# ── errors ──────────────────────────────────────────────────────────


class IcebergViewsNotSupportedError(ValueError):
    """Materialization.VIEW on an Iceberg target — pyiceberg's SqlCatalog has
    no view DDL, so view models can't target Iceberg (yet)."""

    def __init__(self, full_name: str) -> None:
        super().__init__(f"{full_name!r}: Iceberg targets do not support views")


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


class IcebergData:
    def __init__(self, model: "Model", conn: Catalog) -> None:
        self.model = model
        self.catalog = conn
        self.identifier = f"{model.target.schema}.{model.target.name}"

    def create_schema(self) -> None:
        if not self.catalog.namespace_exists(self.model.target.schema):
            self.catalog.create_namespace(self.model.target.schema)

    def create_table(self) -> None:
        if not self.catalog.table_exists(self.identifier):
            self.catalog.create_table(
                self.identifier, schema=iceberg_schema(self.model)
            )

    def recreate_table(self) -> None:
        if self.catalog.table_exists(self.identifier):
            self.catalog.drop_table(self.identifier)
        self.create_table()

    def truncate_table(self) -> None:
        if self.catalog.table_exists(self.identifier):
            self.catalog.load_table(self.identifier).delete(AlwaysTrue())

    def create_or_replace_view(self, body: object) -> None:
        raise IcebergViewsNotSupportedError(self.model.target.full_name)

    def create_indexes(self) -> None:
        # Iceberg has no indexes; the partition_on column is used by the
        # overwrite window filter. A real PartitionSpec is a later feature.
        logger.debug(
            "%s: skipping indexes — Iceberg has none", self.model.target.full_name
        )

    def add_unique_constraint(self) -> None:
        # Iceberg has no constraints; merge keys live on the model and are
        # enforced by upsert's join, not by the table.
        logger.debug(
            "%s: skipping unique constraint — Iceberg has none",
            self.model.target.full_name,
        )

    def create_staging_schema(self) -> None:
        raise IcebergStagingNotSupportedError(self.model.target.full_name)

    def gc_orphan_staging_tables(self) -> None:
        raise IcebergStagingNotSupportedError(self.model.target.full_name)
