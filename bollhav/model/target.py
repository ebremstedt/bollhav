from dataclasses import dataclass, field
from typing import Callable

from bollhav.model.database import Database, DatabaseColumn, DatabaseIndex
from bollhav.model.model_type import ModelType
from bollhav.model.write_modes import WriteMode
from bollhav.model.column_sorting import sort_columns
from bollhav.model.schema import Schema


@dataclass
class Target:
    name: str
    schema: Schema = field(default_factory=Schema)
    database: Database | None = None
    columns: list[DatabaseColumn] = field(default_factory=list)
    indexes: list[DatabaseIndex] = field(default_factory=list)
    model_type: ModelType = ModelType.TABLE
    write_mode: WriteMode = WriteMode.APPEND
    partitioned_by: str | None = None
    dsn_env_var: str | None = None
    column_sorting: Callable | None = sort_columns
    extra: dict | None = None
    recreate_table: bool = False
    truncate_table: bool = False

    sensitive: bool = field(init=False, default=False)
    unique_columns: list = field(init=False, default_factory=list)
    partitioned_by_index: bool = field(init=False, default=False)

    @property
    def full_name(self) -> str:
        return (
            f"{self.schema.resolved}.{self.name}" if self.schema.resolved else self.name
        )

    @property
    def is_view(self) -> bool:
        return self.model_type == ModelType.VIEW

    @property
    def is_table(self) -> bool:
        return self.model_type == ModelType.TABLE

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
        if self.partitioned_by and self.columns:
            col_names = {c.name for c in self.columns}
            if self.partitioned_by not in col_names:
                raise ValueError(
                    f"Partitioned_by references unknown column: {self.partitioned_by!r}"
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
        self.partitioned_by_index = self.partitioned_by is not None

        if self.write_mode == WriteMode.UPSERT_NO_DELETE and not self.unique_columns:
            raise ValueError(
                "WriteMode.UPSERT_NO_DELETE requires at least one column with unique=True"
            )
        if (
            self.write_mode == WriteMode.RECREATE_PARTITION
            and self.partitioned_by is None
        ):
            raise ValueError(
                "WriteMode.RECREATE_PARTITION requires partitioned_by to be set"
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
