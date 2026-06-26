"""Typed errors raised by the model runtime.

Three families, each with its own base:

- `RuntimeConfigError` — an invalid `@load_models` env/config combination
  (user-fixable: wrong env var, bad value). The original use of this module.
- `LifecycleError` — an internal lifecycle invariant was violated (e.g. a
  decorator called with no `run`). Signals a misuse/bug, not bad config.
- `ModelDiscoveryError` — a problem found while scanning a folder and building
  the model set (e.g. a duplicate model name, an empty tag expression).

Each subclass owns its own message so the call sites stay free of error
prose — they just `raise <SpecificError>(...)`. All bases subclass
`ValueError`, so callers catching `ValueError` (and the existing tests) keep
working unchanged. Sibling of `warning.py` and `info.py` in the `messages`
package."""

from __future__ import annotations


class RuntimeConfigError(ValueError):
    """Base for an invalid combination/value of `@load_models` env vars.

    Subclasses `ValueError`, so existing `except ValueError` handlers keep
    catching every config error unchanged."""


class ConflictingRunModeError(RuntimeConfigError):
    """`LATEST_ENABLED` and `BACKFILL_ENABLED` were both set — they pick
    mutually exclusive run modes, so exactly one (or neither) may be true."""

    def __init__(self) -> None:
        super().__init__("LATEST_ENABLED and BACKFILL_ENABLED cannot both be true")


class MissingSchemaSuffixError(RuntimeConfigError):
    """`USE_SCHEMA_SUFFIX=True` was set without a non-empty `SCHEMA_SUFFIX` to
    apply, so there's nothing to suffix the schema with."""

    def __init__(self) -> None:
        super().__init__("USE_SCHEMA_SUFFIX=True requires non-empty SCHEMA_SUFFIX")


class MissingTableSuffixError(RuntimeConfigError):
    """`USE_TABLE_SUFFIX=True` was set without a non-empty `TABLE_SUFFIX` to
    apply, so there's nothing to suffix the table with."""

    def __init__(self) -> None:
        super().__init__("USE_TABLE_SUFFIX=True requires non-empty TABLE_SUFFIX")


class WindowOverrideWithoutLatestError(RuntimeConfigError):
    """`WINDOW_OVERRIDE` was set outside `LATEST_ENABLED` mode. It only adjusts
    the inferred latest-window; in backfill mode since/until are explicit and
    no window is inferred, so the override has nothing to act on."""

    def __init__(self) -> None:
        super().__init__(
            "WINDOW_OVERRIDE only applies when LATEST_ENABLED=True — "
            "in backfill mode since/until are set explicitly and no window is inferred"
        )


class NegativeLookbackError(RuntimeConfigError):
    """`LOOKBACK_OVERRIDE` was given a negative value — a lookback extends the
    window backwards by a non-negative amount, so negatives are meaningless."""

    def __init__(self, value: int) -> None:
        super().__init__(f"LOOKBACK_OVERRIDE must be non-negative, got {value}")


class InvalidStateModeError(RuntimeConfigError):
    """`STATE_MODE` was set to a value outside the known modes
    (`discover` / `bulldozer` / `torch`)."""

    def __init__(self, value: str, valid: list[str]) -> None:
        super().__init__(f"STATE_MODE must be one of {valid}, got {value!r}")


class InvalidTimezoneError(RuntimeConfigError):
    """`TIMEZONE_OVERRIDE` was not a valid IANA timezone name (e.g.
    `Europe/Stockholm`), so it can't be resolved to a zone."""

    def __init__(self, value: str) -> None:
        super().__init__(f"TIMEZONE_OVERRIDE is not a valid IANA timezone: {value!r}")


class RecreatePartitionWithoutWindowError(RuntimeConfigError):
    """A `RECREATE_PARTITION` model's run resolved no window at all, so there's
    no partition to recreate. Raised during state setup (before execution),
    when the run mode produced no since/until. Run it windowed instead."""

    def __init__(self, full_name: str) -> None:
        super().__init__(
            f"{full_name!r} uses WriteMode.RECREATE_PARTITION, which is "
            f"window-scoped, but this run resolved no window — run it windowed "
            f"(LATEST_ENABLED or a BACKFILL window)."
        )


class RecreatePartitionWithoutIntervalError(RuntimeConfigError):
    """A `RECREATE_PARTITION` model reached execution with no interval — the
    per-unit counterpart of `RecreatePartitionWithoutWindowError`. The write
    targets a specific partition, so a NULL interval has nothing to recreate.
    Run it windowed instead."""

    def __init__(self, full_name: str) -> None:
        super().__init__(
            f"RECREATE_PARTITION is window-scoped, but {full_name!r} ran with "
            f"no interval — run it windowed (LATEST_ENABLED or a BACKFILL window)."
        )


class LifecycleError(ValueError):
    """Base for a violated model-lifecycle invariant — a misuse or internal
    bug (e.g. a lifecycle decorator invoked with no `run`), not bad config.

    Subclasses `ValueError` so existing `except ValueError` handlers keep
    catching it unchanged."""


class MissingRunError(LifecycleError):
    """A lifecycle hook ran without a `run`. The wrapper needs the `ModelRun`
    to resolve the model, window, and state, so a missing run is a wiring bug
    in the caller, not a user config error. `hook` names the entry point for
    the message (e.g. `execute`, `@model_lifecycle`)."""

    def __init__(self, hook: str = "execute") -> None:
        super().__init__(
            f"{hook} was called without a `run` — it's required "
            f"(the lifecycle hook brackets one model's run)."
        )


class MissingDataConnError(LifecycleError):
    """A lifecycle-wrapped function was called with no `data_conn`. It's the
    required connection for target DDL and writes — open it in `main()`
    (autocommit) and thread it through to the wrapped function."""

    def __init__(self) -> None:
        super().__init__(
            "data_conn is required and must not be None — open it in "
            "main() (autocommit) and pass it to the lifecycle-wrapped function."
        )


class MssqlStateRequiresPostgresConnError(LifecycleError):
    """A stateful MSSQL model was given one connection for both data and state.
    State always lives in Postgres, so it needs its own Postgres `state_conn`
    (psycopg) passed alongside the MSSQL `data_conn` (pyodbc) — state
    coordination can't run on the MSSQL connection."""

    def __init__(self, full_name: str) -> None:
        super().__init__(
            f"{full_name!r} is an MSSQL model with state, so state lives in "
            f"Postgres — pass a separate Postgres `state_conn=` (psycopg) "
            f"alongside the MSSQL `data_conn=` (pyodbc). State coordination "
            f"can't run on the MSSQL connection."
        )


class ModelDiscoveryError(ValueError):
    """Base for a problem found while discovering models — scanning a folder
    and building the model set (duplicate names, bad tag expression, etc.).

    Subclasses `ValueError` so existing `except ValueError` handlers keep
    catching it unchanged."""


class EmptyTagsError(ModelDiscoveryError):
    """`load_models` was called with no tag expression. Tags select which
    models to run, so a non-empty expression is required (use a catch-all
    like `"*"` to match everything)."""

    def __init__(self) -> None:
        super().__init__("tags must be a non-empty expression.")


class DuplicateModelError(ModelDiscoveryError):
    """Two model files declare the same `full_name` (catalog.schema.table).
    A model's full name must be unique across the scanned folder, since it
    keys the target, the state rows, and the dependency graph."""

    def __init__(self, full_name: str, file, existing) -> None:
        super().__init__(
            f"Duplicate model {full_name!r} found in {file} "
            f"(already defined in {existing})"
        )


class ModelDefinitionError(ValueError):
    """Base for an invalid model/target definition caught in a dataclass
    `__post_init__` field validator — a contradictory or incomplete set of
    fields on a `Model`, `Target`, `Contract`, `TZInterval`, `Source*`, or
    `Staging` (e.g. `recreate_table` and `truncate_table` both set, or a
    timezone-naive bound).

    Subclasses `ValueError` so existing `except ValueError` handlers keep
    catching it unchanged."""


class NaiveIntervalBoundsError(ModelDefinitionError):
    """A `TZInterval` was built with a timezone-naive `since` or `until`.
    Intervals are compared and stored as absolute instants, so both bounds
    must be timezone-aware."""

    def __init__(self) -> None:
        super().__init__("since and until must be timezone-aware")


class SinceAfterUntilError(ModelDefinitionError):
    """A `TZInterval` was built with `since` not strictly before `until` — an
    empty or inverted window. The interval is the half-open span `[since,
    until)`, so `since` must come first."""

    def __init__(self) -> None:
        super().__init__("since must be before until")


class SinceEqualsUntilError(ModelDefinitionError):
    """A `TZInterval` was built with `since` equal to `until` — a zero-width
    window covers no time, so it's rejected."""

    def __init__(self) -> None:
        super().__init__("Since can not be equal to until")


class NaiveContractBeginError(ModelDefinitionError):
    """A `Contract.begin` was set to a timezone-naive datetime. Contract
    bounds are absolute instants, so a set `begin` must be timezone-aware."""

    def __init__(self) -> None:
        super().__init__("contract.begin must be timezone-aware")


class NaiveContractEndError(ModelDefinitionError):
    """A `Contract.end` was set to a timezone-naive datetime. Contract bounds
    are absolute instants, so a set `end` must be timezone-aware."""

    def __init__(self) -> None:
        super().__init__("contract.end must be timezone-aware")


class BatchSizeExceedsMaxError(ModelDefinitionError):
    """A `Batch.size` (or another batch size) exceeded the hard cap
    `MAX_BATCH_SIZE`. `source` names where the value came from for the
    message (e.g. `Batch.size`)."""

    def __init__(self, source: str, batch_size: int, max_batch_size: int) -> None:
        super().__init__(
            f"{source} batch_size={batch_size} exceeds max {max_batch_size}"
        )


class StagingWriteModeError(ModelDefinitionError):
    """A `Staging.write_mode` was set to something other than `APPEND` or
    `UPSERT_NO_DELETE`. `RECREATE_PARTITION` / `VIEW` are target-side
    concepts that don't apply to chunks landing in a staging table."""

    def __init__(self, write_mode) -> None:
        super().__init__(
            f"Staging.write_mode must be WriteMode.APPEND or "
            f"WriteMode.UPSERT_NO_DELETE — got "
            f"{write_mode!r}. RECREATE_PARTITION and VIEW are "
            f"target-side concepts that don't apply to chunks landing "
            f"in a staging table."
        )


class RecreateAndTruncateError(ModelDefinitionError):
    """A `Target` set both `recreate_table` and `truncate_table` — recreate
    already leaves the table empty, so truncate is redundant and the two
    together are contradictory."""

    def __init__(self) -> None:
        super().__init__(
            "recreate_table and truncate_table cannot both be True — "
            "recreate already leaves the table empty"
        )


class ColumnsWithoutDatabaseError(ModelDefinitionError):
    """A `Target` declared `columns` without a `database` — columns describe a
    database-backed table's schema, so the `database` must be set too."""

    def __init__(self) -> None:
        super().__init__("database must be set when columns is provided")


class DatabaseWithoutColumnsError(ModelDefinitionError):
    """A `Target` set `database` without any `columns` — a database-backed
    table needs its column schema, so `columns` must be set too."""

    def __init__(self) -> None:
        super().__init__("columns must be set when database is provided")


class MissingCatalogError(ModelDefinitionError):
    """A database-backed `Target` left `catalog` unset. A model's identity is
    `catalog.schema.table`, so the catalog is required to keep names unique
    across databases in the shared library."""

    def __init__(self, name: str) -> None:
        super().__init__(
            f"catalog must be set on model {name!r} — a database-backed "
            f"model's identity is catalog.schema.table, so the catalog is "
            f"required to keep names unique across databases in the shared "
            f"library (referencing by anything less risks collisions)."
        )


class MultiplePartitionColumnsError(ModelDefinitionError):
    """A `Target` marked more than one column `partition_on=True`. A table
    has a single partition key, so at most one column may carry it. `names`
    lists the offending columns."""

    def __init__(self, names: str) -> None:
        super().__init__(f"At most one column can have partition_on=True, got: {names}")


class UpsertWithoutKeyError(ModelDefinitionError):
    """A `Target` uses `WriteMode.UPSERT_NO_DELETE` but no column is marked
    `primary_key=True` or `unique=True` — the upsert needs a merge key to
    join on."""

    def __init__(self) -> None:
        super().__init__(
            "WriteMode.UPSERT_NO_DELETE requires at least one column with "
            "primary_key=True or unique=True"
        )


class RecreatePartitionWithoutColumnError(ModelDefinitionError):
    """A `Target` uses `WriteMode.RECREATE_PARTITION` but no column is marked
    `partition_on=True` — the write targets a specific partition, so it needs
    a partition column."""

    def __init__(self) -> None:
        super().__init__(
            "WriteMode.RECREATE_PARTITION requires one column with partition_on=True"
        )


class UnknownIndexColumnError(ModelDefinitionError):
    """An index on a `Target` references column(s) that aren't declared on the
    table. `index_name` names the index; `unknown` lists the missing
    column(s)."""

    def __init__(self, index_name: str, unknown: list) -> None:
        super().__init__(
            f"Index {index_name!r} references unknown column(s): {unknown}"
        )


class TimelessModelWithBatchingError(ModelDefinitionError):
    """A `temporality=TIMELESS` model declared `batching` — a timeless model
    is one whole unit, not windowed, so it can't be batched."""

    def __init__(self, name: str) -> None:
        super().__init__(
            f"model {name!r} is temporality=TIMELESS but has batching — a "
            f"timeless model is one whole unit, not windowed. "
            f"Drop `batching` (or pick temporality=TEMPORAL)."
        )


class TimelessModelWithContractWindowError(ModelDefinitionError):
    """A `temporality=TIMELESS` model declared a `contract` begin/end — a
    timeless model has no time axis to bound."""

    def __init__(self, name: str) -> None:
        super().__init__(
            f"model {name!r} is temporality=TIMELESS but its contract has "
            f"begin/end — a timeless model has no time axis to bound. "
            f"Drop the contract window (or pick temporality=TEMPORAL)."
        )


class ViewWithBatchingError(ModelDefinitionError):
    """A `view=True` model declared `batching` — a view isn't materialized
    per-window (it's one CREATE VIEW), so it can't be batched."""

    def __init__(self, name: str) -> None:
        super().__init__(
            f"model {name!r} is a view but has batching — a view isn't "
            f"materialized per-window (it's one CREATE VIEW). Drop "
            f"`batching`. A temporal view declares the range it covers "
            f"via its Contract begin/end instead."
        )


class ViewWithStagingError(ModelDefinitionError):
    """A `view=True` model declared `staging` — a view has nothing to
    stage."""

    def __init__(self, name: str) -> None:
        super().__init__(
            f"model {name!r} is a view but has staging — a view has "
            f"nothing to stage. Drop `staging`."
        )


class ViewWithRecreateOrTruncateError(ModelDefinitionError):
    """A `view=True` model set `recreate_table` / `truncate_table` — those are
    materialized-table operations that don't apply to views."""

    def __init__(self, name: str) -> None:
        super().__init__(
            f"model {name!r} is a view — recreate_table / "
            f"truncate_table don't apply to views."
        )


class GatedUpstreamWithoutStateError(ModelDefinitionError):
    """A model declared a gated upstream (a `Source` with a contract) but has
    no `state` — contracts are only checked for state-tracked models, so a
    gated upstream without state would silently never enforce."""

    def __init__(self, name: str) -> None:
        super().__init__(
            f"model {name!r} declares a gated upstream (a Source "
            f"with a contract) but has no state — contracts are only checked "
            f"for state-tracked models. Add state=State(...), or drop the "
            f"contract."
        )


class SourceContractWithoutModelError(ModelDefinitionError):
    """A `Source` carries a `contract` but its `type` isn't a `SourceModel` —
    only managed models can be state-gated (files / APIs / hardcoded data
    aren't state-tracked). `name` is the source name; `type_name` is the
    actual type."""

    def __init__(self, name: str, type_name: str) -> None:
        super().__init__(
            f"source {name!r} has a contract but type="
            f"{type_name} — only a SourceModel can be gated "
            f"(files / APIs / hardcoded data aren't state-tracked). Drop the "
            f"contract or make it a SourceModel."
        )


class FreshnessWithoutContractError(ModelDefinitionError):
    """A `Source` sets `freshness` but has no `contract` — freshness is a
    recency bound on a gated upstream's state, so it needs a contract."""

    def __init__(self, name: str) -> None:
        super().__init__(
            f"source {name!r} sets `freshness` but has no contract "
            f"— freshness is a recency bound on a gated upstream's state. "
            f"Add a contract (ENCAPSULATE / THROUGH / WHOLE) or drop freshness."
        )


class FreshnessWithExistsContractError(ModelDefinitionError):
    """A `Source` sets `freshness` with `contract=EXISTS` — EXISTS never
    inspects state (registration is the whole gate), so there's no
    `applied_at` to age."""

    def __init__(self, name: str) -> None:
        super().__init__(
            f"source {name!r} sets `freshness` with contract=EXISTS, "
            f"but EXISTS never inspects state (registration is the whole "
            f"gate) — there's no applied_at to age. Use ENCAPSULATE / "
            f"THROUGH / WHOLE, or drop freshness."
        )


class HardcodedSourceFormError(ModelDefinitionError):
    """A `SourceHardcoded` didn't set exactly one of `rows` (inline Python
    rows) or `sql` (an inline SQL literal). `rows_set` says whether `rows`
    was the one provided (used to phrase 'both' vs 'neither')."""

    def __init__(self, rows_set: bool) -> None:
        super().__init__(
            "SourceHardcoded needs exactly one of `rows` (inline Python "
            "rows) or `sql` (an inline SQL literal) — "
            + ("both were set." if rows_set else "neither was set.")
        )


class HardcodedSqlWithoutConnError(ModelDefinitionError):
    """A `SourceHardcoded(sql=...)` was materialized without a `conn` — the
    SQL literal needs the data connection to run against."""

    def __init__(self) -> None:
        super().__init__(
            "SourceHardcoded(sql=...) needs a `conn` to materialize "
            "(it runs the SQL); pass the data connection to to_dataframe()."
        )


class FrozenModelError(AttributeError):
    """A frozen `Model` (an immutable definition) was mutated after
    construction. Per-run state belongs on a `ModelRun`, and pipe/tag
    overrides build a new `Model` via `runtime.apply_runtime_overrides`.
    `attr` names the attribute that was being set.

    Subclasses `AttributeError` so existing `except AttributeError` handlers
    keep catching it unchanged."""

    def __init__(self, attr: str) -> None:
        super().__init__(
            f"Model is frozen (an immutable definition); cannot set {attr!r}. "
            f"Per-run state belongs on a ModelRun, and pipe/tag overrides "
            f"build a new Model via runtime.apply_runtime_overrides."
        )


class UndeclaredInputError(ModelDefinitionError):
    """`ref(name)` was called for a name that isn't a declared input of the
    model — it must be added to `upstream=[...]` before it can be referenced.
    `declared` lists the inputs that are declared."""

    def __init__(self, name: str, full_name: str, declared) -> None:
        super().__init__(
            f"{name!r} is not a declared input of "
            f"{full_name!r} — add it to upstream=[...] before "
            f"referencing it with ref() (declared: {declared or 'none'})"
        )


class NotSqlAddressableError(ModelDefinitionError):
    """`ref(name)` was called for an input that isn't SQL-addressable (a file
    / api / hardcoded source), so it can't go in a `FROM`. `kind` is the
    input's kind; read it in the read function instead."""

    def __init__(self, name: str, kind: str) -> None:
        super().__init__(
            f"input {name!r} is a {kind} — not SQL-addressable, so it "
            f"can't go in a FROM. Read it in your read function instead; "
            f"ref() is only for SourceModel inputs."
        )


class CircularDependencyError(ModelDiscoveryError):
    """The model set has a dependency cycle — topological sort couldn't order
    every model because some still depend on each other. `remaining` is the
    set of models left unordered."""

    def __init__(self, remaining) -> None:
        super().__init__(f"Circular dependency detected among: {remaining}")


class InvalidTagExpressionError(ModelDiscoveryError):
    """A tag expression didn't contain any `[group]` — the parser found no
    bracketed groups, so the expression is malformed. `expr` is the bad
    expression."""

    def __init__(self, expr: str) -> None:
        super().__init__(f"Invalid tag expression: {expr!r}. Must use [group] syntax.")


class CronSeedingInvariantError(RuntimeError):
    """The cron iterator returned a tick `>= now` within its first two steps,
    violating the seeding invariant that at least two ticks precede `now`.
    Signals an internal bug in window resolution, not bad config. `cron` is
    the resolved cron expression.

    Subclasses `RuntimeError` directly (not `ValueError`) so it keeps its
    original catch semantics."""

    def __init__(self, cron: str) -> None:
        super().__init__(
            f"cron seeding invariant violated for {cron!r}: the iterator "
            f"returned a tick >= now within the first two steps"
        )


class ReloadRequiresContractBeginError(ModelDefinitionError):
    """A `reload` window was requested but `contract.begin` isn't set — reload
    spans `contract.begin` .. (`contract.end` or latest tick), so a begin is
    required. `name` is the model name."""

    def __init__(self, name: str) -> None:
        super().__init__(f"reload requires contract.begin to be set on model {name!r}")


class BackfillRequiresSinceError(ModelDefinitionError):
    """A backfill window was requested with no `since` — backfill needs an
    explicit start. Set `contract.begin` on the model or pass `--since` at
    runtime. `name` is the model name."""

    def __init__(self, name: str) -> None:
        super().__init__(
            f"backfill requires a since value — set contract.begin on model "
            f"{name!r} or pass --since at runtime"
        )


__all__ = [
    "RuntimeConfigError",
    "LifecycleError",
    "MissingRunError",
    "MissingDataConnError",
    "MssqlStateRequiresPostgresConnError",
    "ModelDiscoveryError",
    "EmptyTagsError",
    "DuplicateModelError",
    "ConflictingRunModeError",
    "MissingSchemaSuffixError",
    "MissingTableSuffixError",
    "WindowOverrideWithoutLatestError",
    "NegativeLookbackError",
    "InvalidStateModeError",
    "InvalidTimezoneError",
    "RecreatePartitionWithoutWindowError",
    "RecreatePartitionWithoutIntervalError",
    "ModelDefinitionError",
    "NaiveIntervalBoundsError",
    "SinceAfterUntilError",
    "SinceEqualsUntilError",
    "NaiveContractBeginError",
    "NaiveContractEndError",
    "BatchSizeExceedsMaxError",
    "StagingWriteModeError",
    "RecreateAndTruncateError",
    "ColumnsWithoutDatabaseError",
    "DatabaseWithoutColumnsError",
    "MissingCatalogError",
    "MultiplePartitionColumnsError",
    "UpsertWithoutKeyError",
    "RecreatePartitionWithoutColumnError",
    "UnknownIndexColumnError",
    "TimelessModelWithBatchingError",
    "TimelessModelWithContractWindowError",
    "ViewWithBatchingError",
    "ViewWithStagingError",
    "ViewWithRecreateOrTruncateError",
    "GatedUpstreamWithoutStateError",
    "SourceContractWithoutModelError",
    "FreshnessWithoutContractError",
    "FreshnessWithExistsContractError",
    "HardcodedSourceFormError",
    "HardcodedSqlWithoutConnError",
    "FrozenModelError",
    "UndeclaredInputError",
    "NotSqlAddressableError",
    "CircularDependencyError",
    "InvalidTagExpressionError",
    "CronSeedingInvariantError",
    "ReloadRequiresContractBeginError",
    "BackfillRequiresSinceError",
]
