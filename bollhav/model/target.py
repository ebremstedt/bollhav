from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable

from bollhav.model.database import Database, DatabaseColumn, DatabaseIndex
from bollhav.model.model_type import ModelType
from bollhav.model.staging import Staging
from bollhav.model.write_modes import WriteMode
from bollhav.model.column_sorting import sort_columns
from bollhav.model.target_schema import TargetSchema


@dataclass
class Mutations:
    """━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    ⚠  MUTATED AT RUNTIME — NOT CONFIG
    ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    Lives on `Target.mutations`. Every read- or write-site spells out
    its mutability in the access path: `target.mutations.recreated`
    cannot be confused with a config field — and the plural reads as
    "things that have already been mutated on this target."

    Per-pipeline-run tracker for one-shot setup operations on a
    Target. Every field below starts `False`. The first time the
    corresponding DDL statement runs in `ensure_schema_and_table`
    during a pipeline invocation, the flag flips to `True` so
    subsequent intervals in the same loop skip the redundant work.

    `apply_runtime_overrides` builds a fresh `Target` (and therefore
    a fresh `Mutations`) for every `@load_models` invocation, so
    these flags reset naturally between pipeline runs.

    Single-worker only: two pipelines on the same model with
    `recreate_table=True` each see their own fresh `Mutations` and
    would both DROP, potentially clobbering each other. The flag
    isn't a cross-process lock — see `model_lock` if you need that.
    """

    schema_created: bool = False
    table_created: bool = False
    recreated: bool = False  # `recreate_table` DROP done?
    truncated: bool = False  # `truncate_table` TRUNCATE done?
    indexes_created: bool = False
    uniques_added: bool = False


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
    # ⚠ Destructive one-shot ops. Run ONCE on the first
    # `ensure_schema_and_table` call of a pipeline run (gated by
    # `mutations` below). NOT SAFE for parallel runs of the same
    # model — see `Mutations` docstring.
    recreate_table: bool = False
    truncate_table: bool = False
    staging: Staging | None = None

    sensitive: bool = field(init=False, default=False)
    unique_columns: list = field(init=False, default_factory=list)
    primary_key_columns: list = field(init=False, default_factory=list)
    partitioned_by_index: bool = field(init=False, default=False)
    # ⚠ MUTATED AT RUNTIME — see Mutations docstring above. Fields
    # default to all-False (nothing's been done yet). Do not pre-set
    # these in your `Target(...)` call; they're filled in for you.
    mutations: Mutations = field(init=False, default_factory=Mutations)

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
    def full_name(self) -> str:
        """`catalog.schema.name_resolved` when catalog is set, else
        `schema.name_resolved` (or just `name_resolved` when schema is unset —
        same as before catalog existed)."""
        base = (
            f"{self.schema.resolved}.{self.name_resolved}"
            if self.schema.resolved
            else self.name_resolved
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
    def setup_complete(self) -> bool:
        """True iff every *applicable* one-shot setup op has been
        done. A flag staying False is fine when its directive isn't
        set: e.g. `mutations.recreated` only flips when
        `recreate_table=True`, so on a non-recreate target the flag
        stays False forever and we treat that as "nothing to do."

        Lets `ensure_schema_and_table` short-circuit the empty
        BEGIN/COMMIT roundtrip on intervals after the first."""
        m = self.mutations
        return (
            m.schema_created
            and m.table_created
            and (m.recreated or not self.recreate_table)
            and (m.truncated or not self.truncate_table)
            and (m.indexes_created or self.partitioned_by is None)
            and (m.uniques_added or not self.unique_columns)
        )

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
