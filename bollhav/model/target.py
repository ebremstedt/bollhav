from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable

from bollhav.model.database import Database, DatabaseColumn, DatabaseIndex
from bollhav.model.staging import Staging
from bollhav.model.write_modes import WriteMode
from bollhav.model.column_sorting import sort_columns


def resolve_schema_name(name: str, suffix: str, appendix: str | None) -> str:
    """Apply a rotating `suffix` (and optional date `appendix`) to a schema
    name — `warehouse` → `warehouse_pr123_2425_` — or return it unchanged when
    no suffix is set. Pure: callers pass the base name, never a resolved one,
    so it's idempotent. (Note the trailing `_`, kept for back-compat.)"""
    if not suffix:
        return name
    s = f"{name}_{suffix}"
    if appendix:
        s = s + "_" + datetime.now(tz=timezone.utc).strftime(appendix) + "_"
    return s


@dataclass
class Target:
    name: str
    suffix: str = ""
    suffix_appendix: str | None = None
    schema: str = ""
    schema_suffix: str = ""
    schema_suffix_appendix: str | None = "%y%V"
    catalog: str | None = None
    database: Database | None = None

    columns: Sequence[DatabaseColumn] = field(default_factory=list)
    indexes: Sequence[DatabaseIndex] = field(default_factory=list)
    column_sorting: Callable | None = sort_columns
    unique_columns: list = field(init=False, default_factory=list)
    primary_key_columns: list = field(init=False, default_factory=list)
    partitioned_by_index: bool = field(init=False, default=False)
    sensitive: bool = field(init=False, default=False)

    write_mode: WriteMode = WriteMode.APPEND
    dsn_env_var: str | None = None
    extra: dict | None = None
    recreate_table: bool = False
    truncate_table: bool = False
    staging: Staging | None = None
    stage: bool = field(init=False, default=False)

    @property
    def name_resolved(self) -> str:
        """Base table name with `suffix` (and optional date `suffix_appendix`)
        appended. Empty `suffix` returns the bare name unchanged."""
        if not self.suffix:
            return self.name
        s = f"{self.name}_{self.suffix}"
        if self.suffix_appendix:
            s += "_" + datetime.now(tz=timezone.utc).strftime(self.suffix_appendix)
        return s

    @property
    def schema_resolved(self) -> str:
        """The schema name with its `schema_suffix` (+ optional date
        `schema_suffix_appendix`) applied. Empty suffix returns the bare
        schema."""
        return resolve_schema_name(
            self.schema, self.schema_suffix, self.schema_suffix_appendix
        )

    @property
    def full_name(self) -> str:
        """`catalog.schema.name_resolved` when catalog is set, else
        `schema.name_resolved` (or just `name_resolved` when schema is unset —
        same as before catalog existed)."""
        base = (
            f"{self.schema_resolved}.{self.name_resolved}"
            if self.schema_resolved
            else self.name_resolved
        )
        return f"{self.catalog}.{base}" if self.catalog else base

    @property
    def canonical_full_name(self) -> str:
        """Stable identity used to NAME the state table — the *base* schema
        (WITHOUT `schema_suffix` / `schema_suffix_appendix`) plus the resolved
        table name.

        The state SCHEMA still carries the suffix + date appendix
        (`z_bollhav_<suffix>_<week>_`), so stripping them here keeps the table
        *name* inside that schema stable across the rotation: one cleanly-named
        state table per model (`…_orders_<digest>`), not a fresh
        `…_<suffix>_<week>_orders_<digest>` name every week. Table-level
        `suffix` is kept — a distinct physical table keeps distinct state.

        Unlike `full_name` (which resolves the schema and so changes with every
        suffix/appendix roll), this is the week-stable model identity."""
        base = (
            f"{self.schema}.{self.name_resolved}" if self.schema else self.name_resolved
        )
        return f"{self.catalog}.{base}" if self.catalog else base

    @property
    def partitioned_by(self) -> str | None:
        """Name of the column marked `partition_on=True`, or `None`
        if no column is marked. Derived from `columns`."""
        for c in self.columns:
            if getattr(c, "partition_on", False):
                return c.name
        return None

    @property
    def merge_key_columns(self) -> list:
        """Columns to use as the upsert/merge join key. Prefers `primary_key=True`
        columns (a PK is the canonical row identity); falls back to `unique=True`
        columns for models that haven't been migrated to use `primary_key=True`."""
        return self.primary_key_columns or self.unique_columns

    def __post_init__(self) -> None:
        if self.recreate_table and self.truncate_table:
            raise ValueError(
                "recreate_table and truncate_table cannot both be True — "
                "recreate already leaves the table empty"
            )
        if self.database is not None and len(self.columns) == 0:
            raise ValueError("columns must be set when database is provided")
        if len(self.columns) > 0 and self.database is None:
            raise ValueError("database must be set when columns is provided")
        if self.database is not None and not self.catalog:
            raise ValueError(
                f"catalog must be set on model {self.name!r} — a database-backed "
                f"model's identity is catalog.schema.table, so the catalog is "
                f"required to keep names unique across databases in the shared "
                f"library (referencing by anything less risks collisions)."
            )

        partition_cols = [c for c in self.columns if getattr(c, "partition_on", False)]
        if len(partition_cols) > 1:
            names = ", ".join(repr(c.name) for c in partition_cols)
            raise ValueError(
                f"At most one column can have partition_on=True, got: {names}"
            )

        self.sensitive = (
            any(getattr(c, "sensitive", False) for c in self.columns)
            if self.columns
            else False
        )
        self.stage = self.staging is not None
        self.unique_columns = (
            [c for c in self.columns if getattr(c, "unique", False)]
            if self.columns
            else []
        )
        self.primary_key_columns = (
            [c for c in self.columns if getattr(c, "primary_key", False)]
            if self.columns
            else []
        )
        self.partitioned_by_index = self.partitioned_by is not None

        if self.write_mode == WriteMode.UPSERT_NO_DELETE and not self.merge_key_columns:
            raise ValueError(
                "WriteMode.UPSERT_NO_DELETE requires at least one column with "
                "primary_key=True or unique=True"
            )
        if (
            self.write_mode == WriteMode.RECREATE_PARTITION
            and self.partitioned_by is None
        ):
            raise ValueError(
                "WriteMode.RECREATE_PARTITION requires one column with partition_on=True"
            )

        if self.columns and self.column_sorting:
            col_names = [c.name for c in self.columns]
            sorted_names = self.column_sorting(col_names)
            name_to_col = {c.name: c for c in self.columns}
            self.columns = [name_to_col[n] for n in sorted_names]

        if self.indexes:
            col_names = {c.name for c in self.columns}
            for idx in self.indexes:
                referenced = list(getattr(idx, "columns", [])) + list(
                    getattr(idx, "included", [])
                )
                unknown = [c for c in referenced if c not in col_names]
                if unknown:
                    raise ValueError(
                        f"Index {idx.name!r} references unknown column(s): {unknown}"
                    )
