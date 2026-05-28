from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable

from bollhav.model.database import Database, DatabaseColumn, DatabaseIndex
from bollhav.model.model_type import ModelType
from bollhav.model.write_modes import WriteMode
from bollhav.model.column_sorting import sort_columns
from bollhav.model.target_schema import TargetSchema


@dataclass
class Target:
    name: str
    suffix: str = ""
    suffix_appendix: str | None = None
    schema: TargetSchema = field(default_factory=TargetSchema)
    catalog: str | None = None
    database: Database | None = None
    columns: list[DatabaseColumn] = field(default_factory=list)
    indexes: list[DatabaseIndex] = field(default_factory=list)
    model_type: ModelType = ModelType.TABLE
    write_mode: WriteMode = WriteMode.APPEND
    dsn_env_var: str | None = None
    column_sorting: Callable | None = sort_columns
    extra: dict | None = None
    recreate_table: bool = False
    truncate_table: bool = False

    sensitive: bool = field(init=False, default=False)
    unique_columns: list = field(init=False, default_factory=list)
    primary_key_columns: list = field(init=False, default_factory=list)
    partitioned_by_index: bool = field(init=False, default=False)

    @property
    def resolved_name(self) -> str:
        """Base table name with `suffix` (and optional date `suffix_appendix`)
        appended. Empty `suffix` returns the bare name unchanged."""
        if not self.suffix:
            return self.name
        s = f"{self.name}_{self.suffix}"
        if self.suffix_appendix:
            s += "_" + datetime.now(tz=timezone.utc).strftime(self.suffix_appendix)
        return s

    @property
    def full_name(self) -> str:
        """`catalog.schema.resolved_name` when catalog is set, else
        `schema.resolved_name` (or just `resolved_name` when schema is unset —
        same as before catalog existed)."""
        base = (
            f"{self.schema.resolved}.{self.resolved_name}"
            if self.schema.resolved
            else self.resolved_name
        )
        return f"{self.catalog}.{base}" if self.catalog else base

    @property
    def is_view(self) -> bool:
        return self.model_type == ModelType.VIEW

    @property
    def is_table(self) -> bool:
        return self.model_type == ModelType.TABLE

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
        if self.model_type == ModelType.VIEW and self.write_mode != WriteMode.VIEW:
            raise ValueError("ModelType.VIEW must use WriteMode.VIEW")
        if self.model_type == ModelType.TABLE and self.write_mode == WriteMode.VIEW:
            raise ValueError("ModelType.TABLE cannot use WriteMode.VIEW")
        if self.recreate_table and self.truncate_table:
            raise ValueError(
                "recreate_table and truncate_table cannot both be True — "
                "recreate already leaves the table empty"
            )
        if (
            self.recreate_table or self.truncate_table
        ) and self.write_mode == WriteMode.VIEW:
            raise ValueError(
                "recreate_table/truncate_table are not applicable to WriteMode.VIEW"
            )
        if self.database is not None and len(self.columns) == 0:
            raise ValueError("columns must be set when database is provided")
        if len(self.columns) > 0 and self.database is None:
            raise ValueError("database must be set when columns is provided")

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
