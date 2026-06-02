from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Callable

from bollhav.model.actions import Level, OnFailure, Phase
from bollhav.model.database import Database, DatabaseColumn, DatabaseIndex
from bollhav.model.model_type import ModelType
from bollhav.model.staging import Staging
from bollhav.model.write_modes import WriteMode
from bollhav.model.column_sorting import sort_columns
from bollhav.model.target_schema import TargetSchema

if TYPE_CHECKING:
    from bollhav.model.actions import Action


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
    # ⚠ Destructive PRE actions. Each runs ONCE on the first write of a
    # pipeline run; the runner records that in `_applied_model_actions`. NOT SAFE for
    # parallel runs of the same model — see ACTIONS.md.
    recreate_table: bool = False
    truncate_table: bool = False
    staging: Staging | None = None

    # Two lists. Execution order is `default_actions ++ actions` per
    # phase: framework defaults run first, then user-added ones.
    #
    # `default_actions` — framework-provided actions (CREATE SCHEMA /
    # TABLE / INDEX / UNIQUE / staging setup, etc.). `None` means
    # "resolve lazily from the backend's `default_actions()` factory"
    # so this Target stays backend-agnostic. Set to `[]` to opt out
    # of every framework default. Set to a custom list (e.g.
    # `[a for a in default_actions() if a.name != "truncated"]`) to
    # opt out selectively.
    #
    # `actions` — user-added actions. Always run after defaults.
    default_actions: "list[Action] | None" = None
    actions: "list[Action]" = field(default_factory=list)

    # POST-action failure policy. PRE is always fail-fast.
    on_failure: OnFailure = OnFailure.FAIL_FAST

    sensitive: bool = field(init=False, default=False)
    staging_activated: bool = field(init=False, default=False)
    unique_columns: list = field(init=False, default_factory=list)
    primary_key_columns: list = field(init=False, default_factory=list)
    partitioned_by_index: bool = field(init=False, default=False)

    # ⚠ MUTATED AT RUNTIME — the runner records "this action fired
    # this pipeline run" by setting `_applied_model_actions[action.name] = True`.
    # Do not pre-populate; the runner owns this dict.
    _applied_model_actions: dict[str, bool] = field(init=False, default_factory=dict)

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
    def effective_actions(self) -> "list[Action]":
        """The full list of actions the runner will execute, in
        execution order: framework defaults first, then user-added.

        `default_actions=None` means "framework defaults haven't been
        resolved yet" — returns just `actions`. The runner calls
        `resolve_default_actions()` on first use so this property
        starts returning the merged list."""
        if self.default_actions is None:
            return list(self.actions)
        return [*self.default_actions, *self.actions]

    @property
    def setup_complete(self) -> bool:
        """True iff every applicable PRE action has already fired
        this pipeline run.

        Walks `effective_actions`, filters to MODEL/PRE, and checks
        whether each action that *would apply* (i.e. `should_run`
        returns True) is recorded in `_applied_model_actions`. An action whose
        `should_run` is False is treated as "nothing to do," not
        "not done."

        Lets `run_pre_model_actions` short-circuit the empty BEGIN/COMMIT
        roundtrip on intervals after the first."""
        if self.default_actions is None:
            return False  # not yet resolved; runner will populate
        for action in self.effective_actions:
            if not (action.level is Level.MODEL and action.phase is Phase.PRE):
                continue
            # Runner records every processed MODEL/PRE action in
            # `_applied_model_actions` — True if it ran, False if
            # `should_run` skipped it. Either way the key being
            # present means "the runner already made the call about
            # this action this pipeline run."
            if action.name not in self._applied_model_actions:
                return False
        return True

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
        self.staging_activated = self.staging is not None
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
