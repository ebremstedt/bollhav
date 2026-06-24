"""Typed errors raised by the MSSQL backend.

Each subclass owns its own message via `super().__init__`, so call sites stay
free of error prose — they just `raise <SpecificError>(...)`. Most subclass
`MssqlError` (a `ValueError`), so callers catching `ValueError` keep working
unchanged. A couple subclass `NotImplementedError` directly, since their
catch-semantics (an unimplemented branch) can't share the `ValueError` base.
Sibling-style module to `bollhav.model.messages.error`."""

from __future__ import annotations


class MssqlError(ValueError):
    """Base for an MSSQL backend error.

    Subclasses `ValueError`, so existing `except ValueError` handlers keep
    catching every MSSQL config/usage error unchanged."""


class NullablePrimaryKeyColumnError(MssqlError):
    """A `DatabaseColumn` was declared `primary_key=True` and `nullable=True`.
    A primary key can't hold NULLs, so the two flags are contradictory."""

    def __init__(self, name: str) -> None:
        super().__init__(f"Column {name!r}: primary_key=True cannot be nullable")


class EmptyIndexColumnsError(MssqlError):
    """An `Index` was declared with no key columns. An index needs at least
    one column to order/seek on, so an empty `columns` list is invalid."""

    def __init__(self, name: str) -> None:
        super().__init__(f"Index {name!r}: columns must be non-empty")


class OverlappingIndexColumnsError(MssqlError):
    """An `Index`'s key `columns` and `included` columns overlap. Included
    columns are stored at the leaf only and must be disjoint from the key,
    so the same column can't appear in both."""

    def __init__(self, name: str, overlap: list[str]) -> None:
        super().__init__(
            f"Index {name!r}: columns and included must be disjoint, "
            f"got overlap: {overlap}"
        )


class UnhandledMssqlTypeError(MssqlError):
    """`_input_size_for` was given a `MssqlType` it has no input-size mapping
    for — the type gained a value this function doesn't cover yet. Raised
    loudly rather than silently falling back to driver autodetect."""

    def __init__(self, t: object) -> None:
        super().__init__(f"_input_size_for: unhandled MssqlType {t!r}")


class MissingSourceModelQueryError(MssqlError):
    """`create_replace_view` found no upstream `Source` with a `SourceModel`
    type whose `.query` is set. A view is defined by that query, so without
    one there's nothing to create the view from."""

    def __init__(self, full_name: str) -> None:
        super().__init__(
            f"create_replace_view requires a Source with a SourceModel type "
            f"whose .query is set, in upstream=[...] on "
            f"{full_name!r}"
        )


class UnhandledWriteModeError(MssqlError):
    """The model's `write_mode` isn't one the MSSQL writer can handle. Raised
    when dispatching a write so an unsupported mode fails loudly."""

    def __init__(self, write_mode: object) -> None:
        super().__init__(f"Unhandled write mode for MSSQL: {write_mode}")


class WriteOnViewError(MssqlError):
    """`write()` was called for a VIEW model. Views are created by
    `@model_lifecycle`, not written to, so a view's execute body has nothing
    to write."""

    def __init__(self, full_name: str) -> None:
        super().__init__(
            f"{full_name!r} is a VIEW — created by "
            f"@model_lifecycle, not write()."
        )


class MissingDataframeGeneratorError(MssqlError):
    """`write()` was called without a DataFrame generator. The write mode
    needs rows to land, so a `df_gen` is required for non-view models."""

    def __init__(self, write_mode_value: str) -> None:
        super().__init__(f"{write_mode_value} requires a dataframe generator")


class RecreatePartitionRequiresTzAwareError(MssqlError):
    """A `RECREATE_PARTITION` apply was given naive `since`/`until`. The
    partition window is matched in UTC, so both bounds must be UTC-aware."""

    def __init__(self) -> None:
        super().__init__("RECREATE_PARTITION requires since/until to be UTC-aware")


class RecreatePartitionRequiresPartitionedByError(MssqlError):
    """A `RECREATE_PARTITION` apply ran on a target with no `partitioned_by`.
    The DELETE+INSERT keys on the partition column, so it must be set."""

    def __init__(self) -> None:
        super().__init__(
            "RECREATE_PARTITION requires target.partitioned_by to be set"
        )


class RecreatePartitionRequiresWindowError(MssqlError):
    """A `RECREATE_PARTITION` apply resolved no window (since/until). The write
    targets a specific partition window, so the model must be run windowed."""

    def __init__(self) -> None:
        super().__init__(
            "RECREATE_PARTITION requires a window (since/until) — "
            "run the model windowed."
        )


class UnsupportedStagingWriteModeError(NotImplementedError):
    """The staging write dispatch hit a `write_mode` it doesn't implement.
    Subclasses `NotImplementedError`: it marks an unimplemented branch (guarded
    upstream by `Staging.__post_init__`), not bad user config."""

    def __init__(self, wm: object) -> None:
        super().__init__(f"unsupported staging.write_mode {wm!r}")


__all__ = [
    "MssqlError",
    "NullablePrimaryKeyColumnError",
    "EmptyIndexColumnsError",
    "OverlappingIndexColumnsError",
    "UnhandledMssqlTypeError",
    "MissingSourceModelQueryError",
    "UnhandledWriteModeError",
    "WriteOnViewError",
    "MissingDataframeGeneratorError",
    "RecreatePartitionRequiresTzAwareError",
    "RecreatePartitionRequiresPartitionedByError",
    "RecreatePartitionRequiresWindowError",
    "UnsupportedStagingWriteModeError",
]
