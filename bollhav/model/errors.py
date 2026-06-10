"""Typed errors for invalid `@load_models` runtime/env configuration.

Each subclass owns its own message so the `_read_env` call sites stay free of
error prose — they just `raise <SpecificError>(...)`. All subclass
`RuntimeConfigError`, which subclasses `ValueError`, so callers catching
`ValueError` (and the existing tests) keep working unchanged."""

from __future__ import annotations


class RuntimeConfigError(ValueError):
    """Base for an invalid combination/value of `@load_models` env vars."""


class ConflictingRunModeError(RuntimeConfigError):
    def __init__(self) -> None:
        super().__init__("LATEST_ENABLED and BACKFILL_ENABLED cannot both be true")


class MissingSchemaSuffixError(RuntimeConfigError):
    def __init__(self) -> None:
        super().__init__("USE_SCHEMA_SUFFIX=True requires non-empty SCHEMA_SUFFIX")


class MissingTableSuffixError(RuntimeConfigError):
    def __init__(self) -> None:
        super().__init__("USE_TABLE_SUFFIX=True requires non-empty TABLE_SUFFIX")


class WindowOverrideWithoutLatestError(RuntimeConfigError):
    def __init__(self) -> None:
        super().__init__(
            "WINDOW_EXPRESSION_OVERRIDE only applies when LATEST_ENABLED=True — "
            "in backfill mode since/until are set explicitly and no window is inferred"
        )


class NegativeLookbackError(RuntimeConfigError):
    def __init__(self, value: int) -> None:
        super().__init__(f"LOOKBACK_OVERRIDE must be non-negative, got {value}")


class InvalidStateModeError(RuntimeConfigError):
    def __init__(self, value: str, valid: list[str]) -> None:
        super().__init__(f"STATE_MODE must be one of {valid}, got {value!r}")


class InvalidTimezoneError(RuntimeConfigError):
    def __init__(self, value: str) -> None:
        super().__init__(f"TIMEZONE_OVERRIDE is not a valid IANA timezone: {value!r}")


class InvalidUpstreamModeError(RuntimeConfigError):
    def __init__(self, value: str, valid: list[str]) -> None:
        super().__init__(f"UPSTREAM must be one of {valid}, got {value!r}")


__all__ = [
    "RuntimeConfigError",
    "ConflictingRunModeError",
    "MissingSchemaSuffixError",
    "MissingTableSuffixError",
    "WindowOverrideWithoutLatestError",
    "NegativeLookbackError",
    "InvalidStateModeError",
    "InvalidTimezoneError",
    "InvalidUpstreamModeError",
]
