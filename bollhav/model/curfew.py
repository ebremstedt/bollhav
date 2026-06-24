from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, time, timezone, tzinfo


@dataclass
class Curfew:
    """Wall-clock windows and/or weekdays gating whether a model may run.

    The curfew is "in effect" at a moment when BOTH hold (read in `tz`):

      * the weekday is in `days` — or `days` is None, meaning every day; AND
      * the time-of-day is in one of `windows` — or `windows` is empty, meaning
        the whole day.

    By default an in-effect curfew is DENIED (don't run). Set `allowed=True` to
    flip it: run ONLY while in effect, blocked otherwise.

    Examples (deny unless noted):
      * `Curfew([(time(22), time(6))])`                      — don't run 22:00–06:00, any day
      * `Curfew(days={5, 6})`                                — don't run at all on Sat/Sun
      * `Curfew([(time(9), time(17))], days={0,1,2,3,4})`    — block weekday business hours
      * `Curfew([(time(9), time(17))], days={0,1,2,3,4}, allowed=True)` — run ONLY weekday 09:00–17:00

    Windows with `start > end` wrap midnight; multiple windows are unioned.
    `days` are weekday ints, Monday=0 … Sunday=6 (as `datetime.weekday()` /
    `calendar.MONDAY`…`calendar.SUNDAY`). `tz` anchors the wall clock and
    defaults to UTC. Checked once up front per model and again per interval, so
    a run that crosses into an in-effect curfew stops cleanly on the next unit.
    """

    windows: list[tuple[time, time]] = field(default_factory=list)
    days: set[int] | None = None
    tz: tzinfo = timezone.utc
    allowed: bool = False

    @staticmethod
    def _contains(window: tuple[time, time], t: time) -> bool:
        """Is `t` inside `[start, end)`? Handles overnight windows (`start >
        end`) that wrap past midnight."""
        start, end = window
        if start <= end:
            return start <= t < end
        return t >= start or t < end

    def _in_effect(self, now: datetime) -> bool:
        """Is the curfew in effect at `now` (an aware datetime)? True when the
        weekday matches `days` (or `days` is None) AND the time is in a window
        (or there are no windows → the whole day counts)."""
        local = now.astimezone(self.tz)
        if self.days is not None and local.weekday() not in self.days:
            return False
        if not self.windows:
            return True
        t = local.time()
        return any(self._contains(w, t) for w in self.windows)

    def blocks(self, now: datetime) -> bool:
        """True when the model must NOT run at `now` (an aware datetime). A deny
        curfew blocks while in effect; an `allowed` curfew blocks while NOT in
        effect."""
        in_effect = self._in_effect(now)
        return (not in_effect) if self.allowed else in_effect

    @classmethod
    def work_hours(
        cls, *, tz: tzinfo = timezone.utc, allowed: bool = False
    ) -> "Curfew":
        """09:00–17:00 every day — don't run during the 9-to-5."""
        return cls(windows=[(time(9), time(17))], tz=tz, allowed=allowed)

    @classmethod
    def business_hours(
        cls, *, tz: tzinfo = timezone.utc, allowed: bool = False
    ) -> "Curfew":
        """09:00–17:00 on weekdays (Mon–Fri) — work hours, business days only."""
        return cls(
            windows=[(time(9), time(17))], days={0, 1, 2, 3, 4}, tz=tz, allowed=allowed
        )

    @classmethod
    def after_work(
        cls, *, tz: tzinfo = timezone.utc, allowed: bool = False
    ) -> "Curfew":
        """17:00 until midnight."""
        return cls(windows=[(time(17), time(0))], tz=tz, allowed=allowed)

    @classmethod
    def overnight(cls, *, tz: tzinfo = timezone.utc, allowed: bool = False) -> "Curfew":
        """22:00–06:00, across midnight."""
        return cls(windows=[(time(22), time(6))], tz=tz, allowed=allowed)

    @classmethod
    def weekend(cls, *, tz: tzinfo = timezone.utc, allowed: bool = False) -> "Curfew":
        """All of Saturday and Sunday."""
        return cls(days={5, 6}, tz=tz, allowed=allowed)
