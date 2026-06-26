from dataclasses import dataclass
from datetime import datetime

from bollhav.model.errors import ModelDefinitionError


# ── errors ──


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


@dataclass
class TZInterval:
    since: datetime
    until: datetime

    def __post_init__(self) -> None:
        if self.since.tzinfo is None or self.until.tzinfo is None:
            raise NaiveIntervalBoundsError()
        if self.since >= self.until:
            raise SinceAfterUntilError()
        if self.since == self.until:
            raise SinceEqualsUntilError()

    def __str__(self) -> str:
        return f"{self.since:%Y-%m-%d %H:%M} → {self.until:%Y-%m-%d %H:%M}"
