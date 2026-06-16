"""Tests for Curfew — the wall-clock gate on whether a model may run — and its
enforcement at both the model level (@model_lifecycle early-out) and per
interval (@execute_lifecycle)."""

from datetime import datetime, time, timezone
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

from bollhav.model import Batch, Curfew, Temporality, Model, Target
from bollhav.model.lifecycle import execute_lifecycle, model_lifecycle
from bollhav.model.modelrun import ModelRun


def _at(h: int, m: int = 0, tz=timezone.utc) -> datetime:
    return datetime(2024, 1, 1, h, m, tzinfo=tz)


class TestDeny:
    def test_inside_window_blocks(self):
        c = Curfew(windows=[(time(9), time(17))])
        assert c.blocks(_at(12)) is True

    def test_outside_window_runs(self):
        c = Curfew(windows=[(time(9), time(17))])
        assert c.blocks(_at(20)) is False

    def test_start_inclusive_end_exclusive(self):
        c = Curfew(windows=[(time(9), time(17))])
        assert c.blocks(_at(9)) is True  # start inclusive
        assert c.blocks(_at(17)) is False  # end exclusive


class TestOvernight:
    def test_wraps_midnight(self):
        c = Curfew(windows=[(time(22), time(6))])
        assert c.blocks(_at(23)) is True
        assert c.blocks(_at(3)) is True
        assert c.blocks(_at(12)) is False


class TestMultipleWindows:
    def test_union_of_windows_in_a_day(self):
        c = Curfew(windows=[(time(9), time(11)), (time(14), time(16))])
        assert c.blocks(_at(10)) is True
        assert c.blocks(_at(12)) is False  # the gap between windows
        assert c.blocks(_at(15)) is True


class TestAllowed:
    def test_allow_only_inside(self):
        c = Curfew(windows=[(time(9), time(17))], allowed=True)
        assert c.blocks(_at(12)) is False  # inside the allow-window → runs
        assert c.blocks(_at(20)) is True  # outside → blocked

    def test_allow_multiple_windows_blocks_the_gap(self):
        c = Curfew(windows=[(time(9), time(11)), (time(14), time(16))], allowed=True)
        assert c.blocks(_at(10)) is False
        assert c.blocks(_at(12)) is True  # the gap → blocked


class TestDays:
    # 2024-01-06 is a Saturday (weekday 5); 2024-01-09 a Tuesday (weekday 1).
    SAT = datetime(2024, 1, 6, 12, tzinfo=timezone.utc)
    TUE = datetime(2024, 1, 9, 12, tzinfo=timezone.utc)

    def test_ban_whole_weekend(self):
        c = Curfew(days={5, 6})  # no windows → whole day on Sat/Sun
        assert c.blocks(self.SAT) is True
        assert c.blocks(self.TUE) is False

    def test_days_and_hours_together(self):
        # block 09:00–17:00, but only on weekdays
        c = Curfew(windows=[(time(9), time(17))], days={0, 1, 2, 3, 4})
        assert c.blocks(self.TUE) is True  # Tue 12:00 → in window, a weekday
        assert (
            c.blocks(datetime(2024, 1, 9, 20, tzinfo=timezone.utc)) is False
        )  # Tue 20:00
        assert c.blocks(self.SAT) is False  # Sat 12:00 → not a curfew day

    def test_allow_only_weekday_business_hours(self):
        c = Curfew(windows=[(time(9), time(17))], days={0, 1, 2, 3, 4}, allowed=True)
        assert c.blocks(self.TUE) is False  # in effect → allowed → runs
        assert (
            c.blocks(datetime(2024, 1, 9, 20, tzinfo=timezone.utc)) is True
        )  # off-hours
        assert c.blocks(self.SAT) is True  # weekend → blocked

    def test_allow_only_on_weekend(self):
        # days only, allowed → run ONLY on Sat/Sun, any hour
        c = Curfew(days={5, 6}, allowed=True)
        assert c.blocks(self.SAT) is False  # weekend → runs
        assert c.blocks(self.TUE) is True  # weekday → blocked


class TestEmptyWindowsAndDays:
    def test_empty_windows_is_whole_day(self):
        # deny + no windows + no day filter → in effect all day, every day
        c = Curfew(windows=[])
        assert c.blocks(_at(3)) is True
        assert c.blocks(_at(15)) is True

    def test_empty_day_set_is_never_in_effect(self):
        # an empty day set matches no weekday → never in effect → never blocks
        c = Curfew(days=set())
        assert c.blocks(_at(3)) is False
        assert c.blocks(_at(15)) is False

    def test_days_only_bans_the_whole_day(self):
        c = Curfew(days={1})  # Tuesdays, no windows → all of Tuesday
        assert c.blocks(datetime(2024, 1, 9, 3, tzinfo=timezone.utc)) is True  # Tue
        assert c.blocks(datetime(2024, 1, 8, 3, tzinfo=timezone.utc)) is False  # Mon


class TestPresets:
    TUE_NOON = datetime(2024, 1, 9, 12, tzinfo=timezone.utc)
    TUE_NIGHT = datetime(2024, 1, 9, 23, tzinfo=timezone.utc)
    SAT_NOON = datetime(2024, 1, 6, 12, tzinfo=timezone.utc)

    def test_work_hours(self):
        c = Curfew.work_hours()
        assert c.blocks(self.TUE_NOON) is True  # 9–5, every day
        assert c.blocks(self.SAT_NOON) is True

    def test_business_hours_is_weekday_only(self):
        c = Curfew.business_hours()
        assert c.blocks(self.TUE_NOON) is True  # weekday 9–5
        assert c.blocks(self.SAT_NOON) is False  # weekend → runs

    def test_after_work(self):
        c = Curfew.after_work()
        assert c.blocks(self.TUE_NIGHT) is True  # 17:00–midnight
        assert c.blocks(self.TUE_NOON) is False

    def test_overnight(self):
        c = Curfew.overnight()  # 22:00–06:00, across midnight
        assert c.blocks(self.TUE_NIGHT) is True  # 23:00
        assert c.blocks(datetime(2024, 1, 9, 3, tzinfo=timezone.utc)) is True  # 03:00
        assert c.blocks(self.TUE_NOON) is False

    def test_weekend(self):
        c = Curfew.weekend()
        assert c.blocks(self.SAT_NOON) is True
        assert c.blocks(self.TUE_NOON) is False

    def test_allowed_flip_on_preset(self):
        # "run ONLY during work hours"
        c = Curfew.work_hours(allowed=True)
        assert c.blocks(self.TUE_NOON) is False
        assert c.blocks(self.TUE_NIGHT) is True


class TestTimezone:
    def test_window_is_in_curfew_tz_not_utc(self):
        # deny 09:00–10:00 Stockholm; 08:30 UTC == 09:30 CET (winter) → blocked.
        c = Curfew(windows=[(time(9), time(10))], tz=ZoneInfo("Europe/Stockholm"))
        assert c.blocks(datetime(2024, 1, 1, 8, 30, tzinfo=timezone.utc)) is True
        assert c.blocks(datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)) is False


class TestModelField:
    def test_defaults_to_none(self):
        m = Model(target=Target(name="x"), temporality=Temporality.TIMELESS)
        assert m.curfew is None

    def test_stored_on_model(self):
        c = Curfew(windows=[(time(22), time(6))])
        m = Model(
            target=Target(name="x"),
            temporality=Temporality.TEMPORAL,
            batching=Batch(),
            curfew=c,
        )
        assert m.curfew is c


class TestLifecycleGate:
    """A blocking curfew skips the interval entirely — the user's execute never
    runs and the call returns None, so a stateful interval is left pending."""

    def _run(self, curfew):
        m = MagicMock()
        m.target.full_name = "public.orders"
        m.target.database = None
        m.target.stage = False
        m.stateful = False
        m.curfew = curfew
        return ModelRun(model=m)

    def _interval(self):
        iv = MagicMock()
        iv.since, iv.until = _at(0), _at(1)
        return iv

    def test_blocking_curfew_skips_execute(self):
        ran = []

        @execute_lifecycle
        def execute(run, interval, data_conn, state_conn=None):
            ran.append(True)
            return "did work"

        # deny + no windows + no day filter → in effect all day, every day →
        # always blocks (deterministic regardless of the wall clock).
        result = execute(self._run(Curfew(windows=[])), self._interval(), "DATA")
        assert result is None
        assert ran == []  # the execute body never ran

    def test_open_curfew_runs(self):
        ran = []

        @execute_lifecycle
        def execute(run, interval, data_conn, state_conn=None):
            ran.append(True)
            return "did work"

        # empty day set → never an active weekday → never in effect → never
        # blocks (deterministic regardless of the wall clock).
        result = execute(self._run(Curfew(days=set())), self._interval(), "DATA")
        assert result == "did work"
        assert ran == [True]


class TestModelLevelGate:
    """A blocking curfew skips the whole model in @model_lifecycle — before any
    lock, asset DDL, or state bootstrap — so the body never runs and the setup
    backends are never even constructed."""

    def _run(self, curfew):
        m = MagicMock()
        m.target.full_name = "public.orders"
        m.target.database = None
        m.stateful = False
        m.curfew = curfew
        return ModelRun(model=m)

    def test_blocking_curfew_skips_whole_model(self):
        ran = []

        @model_lifecycle
        def run_model(run, data_conn, state_conn=None):
            ran.append(True)

        # Curfew(windows=[]) is in effect all day → always blocks.
        with (
            patch("bollhav.postgres.data.PostgresData") as pg_data,
            patch("bollhav.postgres.state.PostgresState") as pg_state,
        ):
            result = run_model(self._run(Curfew(windows=[])), "DATA")

        assert result is None
        assert ran == []  # the model body never ran
        pg_data.assert_not_called()  # no asset DDL
        pg_state.assert_not_called()  # no state bootstrap / lock
