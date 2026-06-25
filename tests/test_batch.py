from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pytest
from time_machine import travel

from bollhav.model.batch import Batch, TimeChunking
from bollhav.model.window import (
    _resolve_cron,
    _apply_lookback,
    compute_intervals,
    contract_intervals,
    latest_complete_interval,
    resolve_window,
    split_window,
)
from bollhav.model.contract import Contract
from bollhav.model.intervals import TZInterval
from bollhav.model.temporality import Temporality
from bollhav.model.model import Model
from bollhav.model.modelrun import ModelRun
from bollhav.model.target import Target


CET = ZoneInfo("Europe/Stockholm")
UTC = timezone.utc


def _model(
    interval_expression="@hourly",
    tz=UTC,
    lookback=None,
    retries=None,
    contract=None,
    interval_override=None,
    tz_override=None,
    since=None,
    until=None,
    latest=False,
) -> ModelRun:
    """Build a `ModelRun` with pipe overrides + the resolved window baked in —
    mirrors what `apply_runtime_overrides` produces. The window is resolved
    here (so an unsatisfiable backfill raises at construction, just like in
    runtime), and `compute_intervals` only splits it."""
    effective_expr = interval_override or interval_expression
    effective_tz = tz_override or tz
    batching = Batch(
        time=TimeChunking(
            chunk=effective_expr,
            tz=effective_tz,
            lookback=lookback,
        ),
        retries=retries,
    )
    contract = contract or Contract()
    window = resolve_window(
        batching, contract, latest=latest, since=since, until=until, name="test"
    )
    model = Model(
        target=Target(name="test"),
        batching=batching,
        contract=contract,
        temporality=Temporality.TEMPORAL,
    )
    return ModelRun(model=model, window=window)


class TestResolveCron:
    def test_hourly_alias(self) -> None:
        assert _resolve_cron("@hourly") == "0 * * * *"

    def test_daily_alias(self) -> None:
        assert _resolve_cron("@daily") == "0 0 * * *"

    def test_weekly_alias(self) -> None:
        assert _resolve_cron("@weekly") == "0 0 * * 0"

    def test_monthly_alias(self) -> None:
        assert _resolve_cron("@monthly") == "0 0 1 * *"

    def test_minutely_alias(self) -> None:
        assert _resolve_cron("@minutely") == "* * * * *"

    def test_short_form_aliases(self) -> None:
        # roskarl provides short-form synonyms alongside the -ly forms
        assert _resolve_cron("@hour") == _resolve_cron("@hourly")
        assert _resolve_cron("@day") == _resolve_cron("@daily")
        assert _resolve_cron("@week") == _resolve_cron("@weekly")
        assert _resolve_cron("@month") == _resolve_cron("@monthly")

    def test_raw_expression_passed_through(self) -> None:
        assert _resolve_cron("0 6 * * *") == "0 6 * * *"


class TestChunkInterval:
    def test_hourly_chunks_over_three_hours(self) -> None:
        since = datetime(2024, 6, 15, 10, 0, tzinfo=UTC)
        until = datetime(2024, 6, 15, 13, 0, tzinfo=UTC)
        intervals = split_window(TZInterval(since, until), "0 * * * *")
        assert len(intervals) == 3
        assert intervals[0].since == datetime(2024, 6, 15, 10, 0, tzinfo=UTC)
        assert intervals[0].until == datetime(2024, 6, 15, 11, 0, tzinfo=UTC)
        assert intervals[1].since == datetime(2024, 6, 15, 11, 0, tzinfo=UTC)
        assert intervals[1].until == datetime(2024, 6, 15, 12, 0, tzinfo=UTC)
        assert intervals[2].since == datetime(2024, 6, 15, 12, 0, tzinfo=UTC)
        assert intervals[2].until == datetime(2024, 6, 15, 13, 0, tzinfo=UTC)

    def test_single_chunk(self) -> None:
        since = datetime(2024, 6, 15, 10, 0, tzinfo=UTC)
        until = datetime(2024, 6, 15, 11, 0, tzinfo=UTC)
        intervals = split_window(TZInterval(since, until), "0 * * * *")
        assert len(intervals) == 1
        assert intervals[0].since == since
        assert intervals[0].until == until

    def test_partial_trailing_chunk(self) -> None:
        since = datetime(2024, 6, 15, 10, 0, tzinfo=UTC)
        until = datetime(2024, 6, 15, 11, 30, tzinfo=UTC)
        intervals = split_window(TZInterval(since, until), "0 * * * *")
        assert len(intervals) == 2
        assert intervals[0].until == datetime(2024, 6, 15, 11, 0, tzinfo=UTC)
        assert intervals[1].since == datetime(2024, 6, 15, 11, 0, tzinfo=UTC)
        assert intervals[1].until == datetime(2024, 6, 15, 11, 30, tzinfo=UTC)

    def test_daily_chunks(self) -> None:
        since = datetime(2024, 6, 15, 0, 0, tzinfo=UTC)
        until = datetime(2024, 6, 17, 0, 0, tzinfo=UTC)
        intervals = split_window(TZInterval(since, until), "0 0 * * *")
        assert len(intervals) == 2


class TestApplyLookback:
    def test_lookback_shifts_since_backwards(self) -> None:
        result = _apply_lookback(
            "0 * * * *", datetime(2024, 6, 15, 14, 0, tzinfo=UTC), 3
        )
        assert result == datetime(2024, 6, 15, 11, 0, tzinfo=UTC)

    def test_lookback_daily(self) -> None:
        result = _apply_lookback(
            "0 0 * * *", datetime(2024, 6, 15, 0, 0, tzinfo=UTC), 2
        )
        assert result == datetime(2024, 6, 13, 0, 0, tzinfo=UTC)


class TestLatestCompleteInterval:
    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_hourly(self) -> None:
        interval = latest_complete_interval("@hourly", UTC)
        assert interval.since == datetime(2024, 6, 15, 13, 0, tzinfo=UTC)
        assert interval.until == datetime(2024, 6, 15, 14, 0, tzinfo=UTC)

    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_daily(self) -> None:
        interval = latest_complete_interval("@daily", UTC)
        assert interval.since == datetime(2024, 6, 14, 0, 0, tzinfo=UTC)
        assert interval.until == datetime(2024, 6, 15, 0, 0, tzinfo=UTC)

    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_uses_given_tz_daily(self) -> None:
        interval = latest_complete_interval("@daily", CET)
        assert interval.since == datetime(2024, 6, 14, 0, 0, tzinfo=CET)
        assert interval.until == datetime(2024, 6, 15, 0, 0, tzinfo=CET)

    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_uses_given_tz_hourly(self) -> None:
        interval = latest_complete_interval("0 * * * *", CET)
        assert interval.since == datetime(2024, 6, 15, 15, 0, tzinfo=CET)
        assert interval.until == datetime(2024, 6, 15, 16, 0, tzinfo=CET)

    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_defaults_to_utc(self) -> None:
        interval = latest_complete_interval("0 * * * *")
        assert interval.since == datetime(2024, 6, 15, 13, 0, tzinfo=UTC)
        assert interval.until == datetime(2024, 6, 15, 14, 0, tzinfo=UTC)

    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_raw_cron_expression(self) -> None:
        interval = latest_complete_interval("0 * * * *", UTC)
        assert interval.since == datetime(2024, 6, 15, 13, 0, tzinfo=UTC)
        assert interval.until == datetime(2024, 6, 15, 14, 0, tzinfo=UTC)


class TestIntervalsTimezone:
    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_latest_uses_model_tz(self) -> None:
        model = _model(
            interval_expression="@hourly",
            tz=CET,
            interval_override="0 * * * *",
            latest=True,
        )
        intervals = list(compute_intervals(model))
        assert len(intervals) == 1
        assert intervals[0].since == datetime(2024, 6, 15, 15, 0, tzinfo=CET)
        assert intervals[0].until == datetime(2024, 6, 15, 16, 0, tzinfo=CET)

    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_latest_uses_utc_by_default(self) -> None:
        model = _model(
            interval_expression="@hourly",
            interval_override="0 * * * *",
            latest=True,
        )
        intervals = list(compute_intervals(model))
        assert len(intervals) == 1
        assert intervals[0].since == datetime(2024, 6, 15, 13, 0, tzinfo=UTC)
        assert intervals[0].until == datetime(2024, 6, 15, 14, 0, tzinfo=UTC)

    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_tz_override_takes_precedence_over_model_tz(self) -> None:
        model = _model(
            interval_expression="@hourly",
            tz=CET,
            interval_override="0 * * * *",
            tz_override=UTC,
            latest=True,
        )
        intervals = list(compute_intervals(model))
        assert len(intervals) == 1
        assert intervals[0].since == datetime(2024, 6, 15, 13, 0, tzinfo=UTC)
        assert intervals[0].until == datetime(2024, 6, 15, 14, 0, tzinfo=UTC)

    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_tz_override_none_falls_back_to_model(self) -> None:
        model = _model(
            interval_expression="@hourly",
            tz=CET,
            interval_override="0 * * * *",
            tz_override=None,
            latest=True,
        )
        intervals = list(compute_intervals(model))
        assert len(intervals) == 1
        assert intervals[0].since == datetime(2024, 6, 15, 15, 0, tzinfo=CET)
        assert intervals[0].until == datetime(2024, 6, 15, 16, 0, tzinfo=CET)


class TestIntervalsLookback:
    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_latest_with_lookback(self) -> None:
        model = _model(
            interval_expression="@hourly",
            lookback=3,
            interval_override="0 * * * *",
            latest=True,
        )
        intervals = list(compute_intervals(model))
        assert intervals[0].since == datetime(2024, 6, 15, 10, 0, tzinfo=UTC)
        assert intervals[-1].until == datetime(2024, 6, 15, 14, 0, tzinfo=UTC)
        assert len(intervals) == 4

    def test_backfill_with_lookback(self) -> None:
        model = _model(
            interval_expression="@hourly",
            lookback=2,
            interval_override="0 * * * *",
            since=datetime(2024, 6, 15, 12, 0, tzinfo=UTC),
            until=datetime(2024, 6, 15, 14, 0, tzinfo=UTC),
        )
        intervals = list(compute_intervals(model))
        assert intervals[0].since == datetime(2024, 6, 15, 10, 0, tzinfo=UTC)
        assert intervals[-1].until == datetime(2024, 6, 15, 14, 0, tzinfo=UTC)
        assert len(intervals) == 4

    def test_no_lookback(self) -> None:
        model = _model(
            interval_expression="@hourly",
            interval_override="0 * * * *",
            since=datetime(2024, 6, 15, 12, 0, tzinfo=UTC),
            until=datetime(2024, 6, 15, 14, 0, tzinfo=UTC),
        )
        intervals = list(compute_intervals(model))
        assert intervals[0].since == datetime(2024, 6, 15, 12, 0, tzinfo=UTC)
        assert len(intervals) == 2


class TestIntervalsNoneInputs:
    """Backfill bounds fall back to the contract: `since` → `contract.begin`,
    `until` → `contract.end` (or the latest complete tick for an open contract).
    A no-dates backfill therefore runs the declared range — the same window
    `reload` resolves. `since` still needs *a* source (explicit or
    `contract.begin`); with neither it raises."""

    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_until_none_falls_back_to_latest_tick(self) -> None:
        # No `contract.end` → `until` falls back to the latest complete tick.
        model = _model(
            interval_expression="@hourly",
            interval_override="0 * * * *",
            since=datetime(2024, 6, 15, 12, 0, tzinfo=UTC),
        )
        intervals = list(compute_intervals(model))
        assert intervals[0].since == datetime(2024, 6, 15, 12, 0, tzinfo=UTC)
        assert intervals[-1].until == datetime(2024, 6, 15, 14, 0, tzinfo=UTC)

    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_until_set_explicitly_in_backfill_is_honored(self) -> None:
        """An explicit `until` is now required — set it, get the intervals
        the caller asked for."""
        model = _model(
            interval_expression="@hourly",
            interval_override="0 * * * *",
            since=datetime(2024, 6, 15, 12, 0, tzinfo=UTC),
            until=datetime(2024, 6, 15, 14, 0, tzinfo=UTC),
        )
        intervals = list(compute_intervals(model))
        assert intervals[0].since == datetime(2024, 6, 15, 12, 0, tzinfo=UTC)
        assert intervals[-1].until == datetime(2024, 6, 15, 14, 0, tzinfo=UTC)
        assert len(intervals) == 2

    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_latest_mode_auto_derives_until(self) -> None:
        """If you want the latest-complete-tick semantic, use `latest`
        mode — which is what `LATEST_ENABLED=true` opts into."""
        model = _model(
            interval_expression="@hourly",
            interval_override="0 * * * *",
            latest=True,
        )
        intervals = list(compute_intervals(model))
        assert intervals[-1].until == datetime(2024, 6, 15, 14, 0, tzinfo=UTC)

    def test_since_none_without_latest_raises(self) -> None:
        with pytest.raises(ValueError, match="backfill requires a since value"):
            _model(
                interval_expression="@hourly",
                interval_override="0 * * * *",
                until=datetime(2024, 6, 15, 14, 0, tzinfo=UTC),
            )

    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_both_none_without_latest_raises(self) -> None:
        with pytest.raises(ValueError, match="backfill requires a since value"):
            _model(interval_expression="@hourly", interval_override="0 * * * *")

    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_both_none_with_latest_resolves(self) -> None:
        model = _model(
            interval_expression="@hourly",
            interval_override="0 * * * *",
            latest=True,
        )
        intervals = list(compute_intervals(model))
        assert len(intervals) == 1
        assert intervals[0].since == datetime(2024, 6, 15, 13, 0, tzinfo=UTC)
        assert intervals[0].until == datetime(2024, 6, 15, 14, 0, tzinfo=UTC)

    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_defaults_to_model_interval_expression(self) -> None:
        model = _model(
            interval_expression="@hourly",
            since=datetime(2024, 6, 15, 12, 0, tzinfo=UTC),
            until=datetime(2024, 6, 15, 14, 0, tzinfo=UTC),
        )
        intervals = list(compute_intervals(model))
        assert len(intervals) == 2
        assert intervals[0].since == datetime(2024, 6, 15, 12, 0, tzinfo=UTC)
        assert intervals[0].until == datetime(2024, 6, 15, 13, 0, tzinfo=UTC)


class TestContractIntervals:
    """`contract_intervals` returns the full contract range — what *prefill*
    materializes — independent of the run's (possibly narrower) window."""

    def test_fills_the_contract_not_the_window(self) -> None:
        # Contract spans three hours; the run targets only the last one.
        contract = Contract(
            begin=datetime(2024, 1, 1, 0, tzinfo=UTC),
            end=datetime(2024, 1, 1, 3, tzinfo=UTC),
        )
        run = _model(
            interval_expression="@hourly",
            contract=contract,
            since=datetime(2024, 1, 1, 2, tzinfo=UTC),
            until=datetime(2024, 1, 1, 3, tzinfo=UTC),
        )
        # The run's window is a single hour...
        assert len(compute_intervals(run)) == 1
        # ...but prefill fills every hour the contract declares.
        ivs = list(contract_intervals(run))
        assert [(i.since.hour, i.until.hour) for i in ivs] == [(0, 1), (1, 2), (2, 3)]

    def test_falls_back_to_window_without_contract_begin(self) -> None:
        # No contract.begin → nothing declared to fill against → the run window.
        run = _model(
            interval_expression="@hourly",
            since=datetime(2024, 1, 1, 0, tzinfo=UTC),
            until=datetime(2024, 1, 1, 2, tzinfo=UTC),
        )
        assert list(contract_intervals(run)) == list(compute_intervals(run))


# ── trailing edge: no_partial_below × future_data × contract.end ──

# 'now' for every case below; the latest-complete boundaries follow from it.
_NOW = datetime(2026, 6, 25, 12, 0, tzinfo=UTC)
_BEGIN = datetime(2024, 1, 1, tzinfo=UTC)
_PAST = datetime(2025, 6, 1, tzinfo=UTC)  # before _NOW
_FUTURE = datetime(2027, 6, 1, tzinfo=UTC)  # after _NOW
_LCY = datetime(2026, 1, 1, tzinfo=UTC)  # latest complete @yearly end
_LCM = datetime(2026, 6, 1, tzinfo=UTC)  # latest complete @monthly end
_LCD = datetime(2026, 6, 25, tzinfo=UTC)  # latest complete @daily end (00:00)


def _edge_batch(chunk="@yearly", no_partial_below=None, future_data=False) -> Batch:
    return Batch(
        time=TimeChunking(
            chunk=chunk,
            no_partial_below=no_partial_below,
            future_data=future_data,
            tz=UTC,
        )
    )


# (contract.end, future_data, no_partial_below) -> expected window.until, chunk=@yearly.
_EDGE_CASES = [
    # open contract → the completeness floor. future_data is a no-op here
    # (resolve_window falls through to the floor; the Model guard forbids the
    # combo upstream, but the resolver itself must stay well-defined).
    (None, False, None, _LCY),
    (None, False, "@daily", _LCD),
    (None, False, "@monthly", _LCM),
    (None, True, None, _LCY),
    (None, True, "@daily", _LCD),
    # past end → its own value either way (already before the floor).
    (_PAST, False, None, _PAST),
    (_PAST, False, "@daily", _PAST),
    (_PAST, True, None, _PAST),
    # future end → clamped to the floor, unless future_data honours it.
    (_FUTURE, False, None, _LCY),
    (_FUTURE, False, "@daily", _LCD),
    (_FUTURE, False, "@monthly", _LCM),
    (_FUTURE, True, None, _FUTURE),
    (_FUTURE, True, "@daily", _FUTURE),
]
_EDGE_IDS = [
    "open/fd0/chunk",
    "open/fd0/day",
    "open/fd0/month",
    "open/fd1/chunk",
    "open/fd1/day",
    "past/fd0/chunk",
    "past/fd0/day",
    "past/fd1/chunk",
    "future/fd0/chunk",
    "future/fd0/day",
    "future/fd0/month",
    "future/fd1/chunk",
    "future/fd1/day",
]


class TestTrailingEdge:
    """`no_partial_below` (inferred-edge completeness grain) × `future_data`
    (honour a future end) × `contract.end`, over the inferred-window modes
    (reload + no-dates backfill). chunk=`@yearly` so the default floor (year)
    differs visibly from a finer grain. 'now' = 2026-06-25 12:00 UTC."""

    @pytest.mark.parametrize("mode", ["reload", "backfill"])
    @pytest.mark.parametrize("end,future_data,npb,expected", _EDGE_CASES, ids=_EDGE_IDS)
    def test_edge_matrix(self, end, future_data, npb, expected, mode) -> None:
        with travel(_NOW, tick=False):
            batch = _edge_batch(no_partial_below=npb, future_data=future_data)
            w = resolve_window(
                batch, Contract(begin=_BEGIN, end=end), reload=(mode == "reload")
            )
        assert w.since == _BEGIN
        assert w.until == expected

    def test_default_edge_is_latest_complete_chunk(self) -> None:
        # npb unset + future_data False → exactly the legacy behavior.
        with travel(_NOW, tick=False):
            w = resolve_window(_edge_batch(), Contract(begin=_BEGIN), reload=True)
        assert w.until == _LCY

    def test_no_partial_below_coarser_than_chunk(self) -> None:
        # hourly chunks, but only release whole days: the edge is the last
        # complete day (00:00), not the latest complete hour (12:00).
        with travel(_NOW, tick=False):
            batch = _edge_batch(chunk="@hourly", no_partial_below="@daily")
            w = resolve_window(batch, Contract(begin=_BEGIN), reload=True)
        assert w.until == _LCD

    def test_latest_mode_ignores_no_partial_below(self) -> None:
        # latest mode uses `window` (the bite), not the inferred-edge knobs.
        with travel(_NOW, tick=False):
            batch = Batch(
                time=TimeChunking(
                    chunk="@yearly",
                    window="@monthly",
                    no_partial_below="@daily",
                    tz=UTC,
                )
            )
            w = resolve_window(batch, Contract(begin=_BEGIN), latest=True)
        assert (w.since, w.until) == (datetime(2026, 5, 1, tzinfo=UTC), _LCM)

    def test_explicit_until_overrides_the_inferred_edge(self) -> None:
        # An explicit BACKFILL_UNTIL wins over end / future_data / no_partial_below.
        until = datetime(2025, 3, 1, tzinfo=UTC)
        with travel(_NOW, tick=False):
            batch = _edge_batch(no_partial_below="@daily", future_data=True)
            w = resolve_window(
                batch, Contract(begin=_BEGIN, end=_FUTURE), since=_BEGIN, until=until
            )
        assert w.until == until

    def test_lookback_shifts_start_independently_of_the_edge(self) -> None:
        # lookback is the start knob; no_partial_below is the end knob — orthogonal.
        begin = datetime(2026, 6, 20, tzinfo=UTC)
        with travel(_NOW, tick=False):
            batch = Batch(
                time=TimeChunking(
                    chunk="@daily", no_partial_below="@daily", lookback=2, tz=UTC
                )
            )
            w = resolve_window(batch, Contract(begin=begin), reload=True)
        assert w.until == _LCD  # end snapped to the last complete day
        assert w.since == datetime(2026, 6, 18, tzinfo=UTC)  # start pulled back 2 days
