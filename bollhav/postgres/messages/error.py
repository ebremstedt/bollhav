"""Typed errors raised by the Postgres backend.

Sibling of `bollhav.model.messages.error`, but scoped to the Postgres package
(writes, staging, columns, and state coordination). Each subclass owns its own
message via `super().__init__`, so the call sites stay free of error prose —
they just `raise <SpecificError>(...)`.

Most errors share `PostgresError(ValueError)` as their base, so callers catching
`ValueError` (and the existing tests) keep working unchanged. A few can't:
the `create_indexes` guard is a `RuntimeError` (a misuse/bug, caught elsewhere
as such), and the write-mode dispatch fall-throughs are `NotImplementedError`
(an unsupported enum case) — those subclass their builtin directly so their
catch-semantics are preserved."""

from __future__ import annotations


class PostgresError(ValueError):
    """Base for a Postgres-backend error that callers catch as `ValueError`.

    Subclasses `ValueError`, so existing `except ValueError` handlers keep
    catching every backend error unchanged."""


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


class PrimaryKeyNotNullableError(PostgresError):
    """A column was declared both `primary_key=True` and `nullable=True`. A
    primary key can never be NULL, so the two flags conflict."""

    def __init__(self, name: str) -> None:
        super().__init__(f"Column {name!r}: primary_key=True cannot be nullable")


class CreateIndexesWithoutPartitionError(RuntimeError):
    """`create_indexes` ran for a target with `partitioned_by` None — there's no
    column to index. Signals a missing guard at the call site (a misuse/bug),
    so it's a `RuntimeError`, not a config `ValueError`."""

    def __init__(self, full_name: str) -> None:
        super().__init__(
            f"create_indexes ran for {full_name!r} but partitioned_by "
            f"is None — guard the call on `target.partitioned_by is not None`"
        )


class RecreatePartitionRequiresWindowError(PostgresError):
    """A `RECREATE_PARTITION` write was dispatched with no since/until — the
    mode overwrites a specific window, so it needs one. Run the model windowed."""

    def __init__(self) -> None:
        super().__init__("Since and until must be set for RECREATE_PARTITION")


class UnhandledWriteModeError(PostgresError):
    """The model's `write_mode` fell through every handled case in the write
    dispatch — an unknown/unsupported table write mode."""

    def __init__(self, write_mode) -> None:
        super().__init__(f"Unhandled write mode: {write_mode}")


class WriteOnViewError(PostgresError):
    """`write()` was called for a VIEW model. Views carry no data to write —
    they're created by `@model_lifecycle`, so a view's execute body has nothing
    to write."""

    def __init__(self, full_name: str) -> None:
        super().__init__(
            f"write() is for data, not views — {full_name!r} is "
            f"a VIEW. Views are created by @model_lifecycle "
            f"(PostgresData.create_or_replace_view); a view's execute body "
            f"has nothing to write."
        )


class MissingDataFrameError(PostgresError):
    """A table write mode (APPEND / RECREATE_PARTITION / UPSERT_NO_DELETE) was
    invoked with no DataFrame generator — those modes have nothing to write
    without one."""

    def __init__(self) -> None:
        super().__init__(
            "Modes APPEND, RECREATE_PARTITION, UPSERT_NO_DELETE need a dataframe"
        )


class UnsupportedStagingWriteModeError(NotImplementedError):
    """The staging write path hit a write mode it doesn't support (guarded
    upstream by `Staging.__post_init__`). Subclasses `NotImplementedError` so
    its catch-semantics are preserved."""

    def __init__(self, wm) -> None:
        super().__init__(f"unsupported staging.write_mode {wm!r}")


class RecreatePartitionRequiresAwareWindowError(PostgresError):
    """A staged `RECREATE_PARTITION` apply got a naive since/until. The DELETE
    window must be an unambiguous instant, so both bounds must be UTC-aware."""

    def __init__(self) -> None:
        super().__init__("RECREATE_PARTITION requires since/until to be UTC-aware")


class RecreatePartitionRequiresPartitionedByError(PostgresError):
    """A staged `RECREATE_PARTITION` apply ran on a target with no
    `partitioned_by` — there's no column to scope the window DELETE/INSERT to."""

    def __init__(self) -> None:
        super().__init__("RECREATE_PARTITION requires target.partitioned_by to be set")


class StagedRecreatePartitionRequiresWindowError(PostgresError):
    """A staged `RECREATE_PARTITION` apply was reached with no since/until — the
    mode overwrites a specific window, so it needs one. Run the model windowed."""

    def __init__(self) -> None:
        super().__init__(
            "RECREATE_PARTITION requires a window (since/until) — "
            "run the model windowed."
        )


class UnsupportedTargetWriteModeError(NotImplementedError):
    """The staging→target apply hit a target write mode it doesn't support
    (guarded upstream by `_assert_supported`). Subclasses `NotImplementedError`
    so its catch-semantics are preserved."""

    def __init__(self, wm) -> None:
        super().__init__(f"unsupported target.write_mode {wm!r}")


class MissingStateConnError(PostgresError):
    """`PostgresState` was used without an injected connection. It doesn't open
    its own — the caller owns it (opened in `main()`, threaded through the
    lifecycle hooks) — so a missing connection is a wiring error."""

    def __init__(self) -> None:
        super().__init__(
            "a state connection is required — construct "
            "PostgresState(model, conn=<state_conn>). PostgresState "
            "does not self-connect."
        )


class StateActivationRequiredError(PostgresError):
    """A state-only operation (e.g. `acquire_model_lock`) was called on a model
    with no `state=State(...)`. The lifecycle only invokes these on stateful
    models, so reaching here is a wiring bug in the caller."""

    def __init__(self, full_name: str) -> None:
        super().__init__(
            f"acquire_model_lock requires a state-activated model, but "
            f"{full_name!r} has model.state is None"
        )


class StateHashCollisionError(PostgresError):
    """The ~1e-12 case where two different models hash to the same state table.
    The table already holds rows for a different model, so sharing it would
    corrupt state — rename a model or widen the digest instead."""

    def __init__(self, schema: str, state_table: str, existing, full_name: str) -> None:
        super().__init__(
            f"state hash collision: {schema}.{state_table} already holds "
            f"state for {existing!r}, not {full_name!r}. "
            f"Rename one model or widen the digest in state_table_name."
        )


class PrefillRequiresStateError(PostgresError):
    """`prefill_intervals` was called on a model with no `state`. There's no
    state table to prefill, so the model must be state-enabled."""

    def __init__(self, full_name: str) -> None:
        super().__init__(
            "prefill_intervals requires a state-enabled model "
            f"({full_name!r} has no `state`)"
        )


class OneshotRequiresStateError(PostgresError):
    """`insert_oneshot` was called on a model with no `state`. There's no state
    table to write the oneshot row to, so the model must be state-enabled."""

    def __init__(self, full_name: str) -> None:
        super().__init__(
            "insert_oneshot requires a state-enabled model "
            f"({full_name!r} has no `state`)"
        )


class InvalidPrefillStatusError(PostgresError):
    """A prefill row carried a status other than `pending` or `blocked` — the
    only two statuses a prefill may set."""

    def __init__(self, status) -> None:
        super().__init__(
            f"prefill status must be 'pending' or 'blocked', got {status!r}"
        )


class BlockedRowRequiresReasonError(PostgresError):
    """A prefill row marked `blocked` carried no `blocked_reason`. A blocked row
    must say why it's blocked, so the reason is required."""

    def __init__(self) -> None:
        super().__init__("blocked rows require a non-empty blocked_reason")


class ClearStateRefusedError(PostgresError):
    """`clear_state` refuses to run on a model with no schema suffix — its state
    lives in prod (`z_bollhav`), and clearing prod state isn't offered. Set a
    schema suffix for an ephemeral environment, or delete rows by hand."""

    def __init__(self, full_name: str, library_schema: str) -> None:
        super().__init__(
            f"clear_state refuses to run on {full_name!r}: "
            f"it has no schema suffix, so its state lives in prod "
            f"({library_schema}). Clearing prod state isn't offered — set "
            f"SCHEMA_SUFFIX for an ephemeral environment, or delete the rows "
            f"by hand if you truly must."
        )


class UnregisteredUpstreamError(PostgresError):
    """A gated upstream contract names an upstream that isn't registered in the
    library — it has never run. The gate demands the upstream exists, so this is
    a real error (a typo, or the upstream was never deployed/run)."""

    def __init__(self, name: str, level: str, full_name: str) -> None:
        super().__init__(
            f"upstream contract {name!r} ({level}) on "
            f"{full_name!r} is not registered in "
            f"the library — it has never run. A gated upstream demands "
            f"the upstream exists; fix the name or run the upstream "
            f"first. (An ungated source would not block.)"
        )


class TimelessUpstreamContractError(PostgresError):
    """An EXACT / ENCAPSULATE / THROUGH contract gates on a per-window match,
    but its upstream is TIMELESS and has no window to match. Use WHOLE (loaded)
    or EXISTS (registered) instead."""

    def __init__(self, name: str, level: str, full_name: str) -> None:
        super().__init__(
            f"upstream contract {name!r} ({level}) on "
            f"{full_name!r} targets a TIMELESS "
            f"upstream, which has no window to match. Use WHOLE "
            f"(loaded) or EXISTS (registered) instead."
        )


class ExactContractOnFlexibleUpstreamError(PostgresError):
    """An EXACT contract gates on an applied row whose `(since, until)` equals
    the window exactly, but the upstream is flexible (`fixed_intervals=False`)
    — it coalesces its applied rows into maximal covered ranges, so no
    exact-grain row survives to match. The downstream would block forever. Use
    ENCAPSULATE (coverage) against a flexible upstream instead."""

    def __init__(self, name: str, full_name: str) -> None:
        super().__init__(
            f"upstream contract {name!r} (exact) on {full_name!r} targets a "
            f"flexible upstream (fixed_intervals=False), which coalesces away "
            f"its exact-grain rows — EXACT can never match and the model would "
            f"block forever. Use ENCAPSULATE instead."
        )


class DropEnvironmentRefusedError(PostgresError):
    """`drop_environment` refuses to run when no model carries a schema suffix —
    it would target prod schemas. Set `SCHEMA_SUFFIX` for an ephemeral
    environment, or drop prod schemas by hand."""

    def __init__(self, library_schema: str) -> None:
        super().__init__(
            "drop_environment refuses to run: no model carries a schema suffix, "
            f"so it would target prod schemas ({library_schema} + unsuffixed "
            "targets). Set SCHEMA_SUFFIX for an ephemeral environment, or drop "
            "prod schemas by hand if you must."
        )


__all__ = [
    "PostgresError",
    "NaiveDatetimeError",
    "RecreatePartitionRequiresPartitionColumnError",
    "CreateReplaceViewRequiresSourceModelError",
    "PrimaryKeyNotNullableError",
    "CreateIndexesWithoutPartitionError",
    "RecreatePartitionRequiresWindowError",
    "UnhandledWriteModeError",
    "WriteOnViewError",
    "MissingDataFrameError",
    "UnsupportedStagingWriteModeError",
    "RecreatePartitionRequiresAwareWindowError",
    "RecreatePartitionRequiresPartitionedByError",
    "StagedRecreatePartitionRequiresWindowError",
    "UnsupportedTargetWriteModeError",
    "MissingStateConnError",
    "StateActivationRequiredError",
    "StateHashCollisionError",
    "PrefillRequiresStateError",
    "OneshotRequiresStateError",
    "InvalidPrefillStatusError",
    "BlockedRowRequiresReasonError",
    "ClearStateRefusedError",
    "UnregisteredUpstreamError",
    "TimelessUpstreamContractError",
    "ExactContractOnFlexibleUpstreamError",
    "DropEnvironmentRefusedError",
]
