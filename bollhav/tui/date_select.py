"""A scrollable date-time picker: dropdowns for year / month / day / hour that
together produce an ISO 8601 datetime (UTC). Reads/writes a plain ISO string so
it drops into the config form like any other field.
"""

from __future__ import annotations

from datetime import datetime

from textual.app import ComposeResult
from textual.containers import Horizontal
from textual.widgets import Select

YEARS = list(range(2018, 2036))
MONTHS = list(range(1, 13))
DAYS = list(range(1, 32))
HOURS = list(range(0, 24))


def _is_set(value: object) -> bool:
    """True only for a real chosen option (an int, not the blank sentinel —
    which this Textual version represents as the bool False)."""
    return isinstance(value, int) and not isinstance(value, bool)


class DateSelect(Horizontal):
    """Year / month / day / hour dropdowns → an ISO 8601 datetime string."""

    DEFAULT_CSS = """
    DateSelect { height: auto; }
    DateSelect Select { width: 1fr; margin: 0 1 0 0; }
    """

    def compose(self) -> ComposeResult:
        yield Select([(str(y), y) for y in YEARS], prompt="YYYY", id="d-year")
        yield Select([(f"{m:02d}", m) for m in MONTHS], prompt="MM", id="d-month")
        yield Select([(f"{d:02d}", d) for d in DAYS], prompt="DD", id="d-day")
        yield Select([(f"{h:02d}h", h) for h in HOURS], prompt="HH", id="d-hour")

    def _sel(self, part: str) -> Select:
        return self.query_one(f"#d-{part}", Select)

    @property
    def value(self) -> str:
        """The ISO datetime, or "" if the date parts are unset. Hour defaults to 0."""
        y, mo, d = (
            self._sel("year").value,
            self._sel("month").value,
            self._sel("day").value,
        )
        if not (_is_set(y) and _is_set(mo) and _is_set(d)):
            return ""
        h = self._sel("hour").value
        hh = h if _is_set(h) else 0
        return f"{y:04d}-{mo:02d}-{d:02d}T{hh:02d}:00:00+00:00"

    def set_value(self, iso: str) -> None:
        """Populate the dropdowns from an ISO string (clears them if unparseable)."""
        try:
            dt: datetime | None = datetime.fromisoformat(iso) if iso else None
        except ValueError:
            dt = None
        parts = (
            {"year": dt.year, "month": dt.month, "day": dt.day, "hour": dt.hour}
            if dt
            else {}
        )
        for part in ("year", "month", "day", "hour"):
            sel = self._sel(part)
            if dt is not None:
                sel.value = parts[part]
            else:
                sel.clear()
