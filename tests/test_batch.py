from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pytest
from time_machine import travel

from bollhav.model.batch import Batch, TimeChunking, ChunkFix, ChunkFlex
from bollhav.model.window import (
    _resolve_cron,
    _apply_lookback,
    compute_intervals,
    contract_intervals,
    latest_complete_interval,
    resolve_window,
    split_at_times,
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


# ── trailing edge: floor_chunk × contract.end ──

# 'now' for every case below; the latest-complete boundaries follow from it.
_NOW = datetime(2026, 6, 25, 12, 0, tzinfo=UTC)
_BEGIN = datetime(2024, 1, 1, tzinfo=UTC)
_PAST = datetime(2025, 6, 1, tzinfo=UTC)  # before _NOW
_FUTURE = datetime(2027, 6, 1, tzinfo=UTC)  # after _NOW
_LCY = datetime(2026, 1, 1, tzinfo=UTC)  # latest complete @yearly end
_LCM = datetime(2026, 6, 1, tzinfo=UTC)  # latest complete @monthly end
_LCD = datetime(2026, 6, 25, tzinfo=UTC)  # latest complete @daily end (00:00)


def _edge_batch(chunk="@yearly", floor_chunk=None) -> Batch:
    # floor_chunk is flexible-only now; a bare edge model (no floor) is fixed.
    flexibility = (
        ChunkFlex(floor_chunk=floor_chunk) if floor_chunk is not None else ChunkFix()
    )
    return Batch(time=TimeChunking(chunk=chunk, flexibility=flexibility, tz=UTC))


# (contract.end, floor_chunk) -> expected window.until, with chunk=@yearly.
_EDGE_CASES = [
    # open contract → the completeness floor.
    (None, None, _LCY),
    (None, "@daily", _LCD),
    (None, "@monthly", _LCM),
    # past end → its own value (already before the floor).
    (_PAST, None, _PAST),
    (_PAST, "@daily", _PAST),
    # future end → clamped to the floor (no data past the clock to load).
    (_FUTURE, None, _LCY),
    (_FUTURE, "@daily", _LCD),
    (_FUTURE, "@monthly", _LCM),
]
_EDGE_IDS = [
    "open/chunk",
    "open/day",
    "open/month",
    "past/chunk",
    "past/day",
    "future/chunk",
    "future/day",
    "future/month",
]


class TestTrailingEdge:
    """`floor_chunk` (inferred-edge completeness grain) × `contract.end`,
    over the inferred-window modes (reload + no-dates backfill). chunk=`@yearly`
    so the default floor (year) differs visibly from a finer grain; a future end
    is always clamped to the floor. 'now' = 2026-06-25 12:00 UTC."""

    @pytest.mark.parametrize("mode", ["reload", "backfill"])
    @pytest.mark.parametrize("end,npb,expected", _EDGE_CASES, ids=_EDGE_IDS)
    def test_edge_matrix(self, end, npb, expected, mode) -> None:
        with travel(_NOW, tick=False):
            batch = _edge_batch(floor_chunk=npb)
            w = resolve_window(
                batch, Contract(begin=_BEGIN, end=end), reload=(mode == "reload")
            )
        assert w.since == _BEGIN
        assert w.until == expected

    def test_default_edge_is_latest_complete_chunk(self) -> None:
        # npb unset → exactly the legacy behavior (latest complete chunk).
        with travel(_NOW, tick=False):
            w = resolve_window(_edge_batch(), Contract(begin=_BEGIN), reload=True)
        assert w.until == _LCY

    def test_floor_chunk_coarser_than_chunk(self) -> None:
        # hourly chunks, but only release whole days: the edge is the last
        # complete day (00:00), not the latest complete hour (12:00).
        with travel(_NOW, tick=False):
            batch = _edge_batch(chunk="@hourly", floor_chunk="@daily")
            w = resolve_window(batch, Contract(begin=_BEGIN), reload=True)
        assert w.until == _LCD

    def test_latest_mode_ignores_floor_chunk(self) -> None:
        # latest mode uses `window` (the bite), not the inferred-edge knobs.
        with travel(_NOW, tick=False):
            batch = Batch(
                time=TimeChunking(
                    chunk="@yearly",
                    latest_window="@monthly",
                    flexibility=ChunkFlex(floor_chunk="@daily"),
                    tz=UTC,
                )
            )
            w = resolve_window(batch, Contract(begin=_BEGIN), latest=True)
        assert (w.since, w.until) == (datetime(2026, 5, 1, tzinfo=UTC), _LCM)

    def test_explicit_until_overrides_the_inferred_edge(self) -> None:
        # An explicit RUN_UNTIL wins over the contract end and floor_chunk.
        until = datetime(2025, 3, 1, tzinfo=UTC)
        with travel(_NOW, tick=False):
            batch = _edge_batch(floor_chunk="@daily")
            w = resolve_window(
                batch, Contract(begin=_BEGIN, end=_FUTURE), since=_BEGIN, until=until
            )
        assert w.until == until

    def test_lookback_shifts_start_independently_of_the_edge(self) -> None:
        # lookback is the start knob; floor_chunk is the end knob — orthogonal.
        begin = datetime(2026, 6, 20, tzinfo=UTC)
        with travel(_NOW, tick=False):
            batch = Batch(
                time=TimeChunking(
                    chunk="@daily",
                    flexibility=ChunkFlex(floor_chunk="@daily"),
                    lookback=2,
                    tz=UTC,
                )
            )
            w = resolve_window(batch, Contract(begin=begin), reload=True)
        assert w.until == _LCD  # end snapped to the last complete day
        assert w.since == datetime(2026, 6, 18, tzinfo=UTC)  # start pulled back 2 days

    def test_flexible_monthly_backfill_cuts_last_month_at_current_day(self) -> None:
        # A FLEXIBLE model chunked by month, backfilled with "now" halfway through
        # a month: the trailing edge floors on the latest complete DAY
        # (floor_chunk="@daily"), so the last month is a *partial*
        # [month-start, that day) — cut mid-month, not extended to the month end.
        now = datetime(2026, 6, 15, 12, tzinfo=UTC)  # halfway through June
        begin = datetime(2026, 3, 1, tzinfo=UTC)
        batch = Batch(
            time=TimeChunking(
                chunk="@monthly",
                flexibility=ChunkFlex(floor_chunk="@daily"),
                tz=UTC,
            )
        )
        with travel(now, tick=False):
            # no-dates backfill: since given, until inferred → the trailing edge.
            w = resolve_window(batch, Contract(begin=begin), since=begin)
            intervals = split_window(w, batch.time.chunk)
        # edge cut at the latest complete day, not the June month boundary.
        assert w.until == datetime(2026, 6, 15, tzinfo=UTC)
        # whole months up to June, then a partial June 1 → June 15.
        assert intervals[-2] == TZInterval(
            datetime(2026, 5, 1, tzinfo=UTC), datetime(2026, 6, 1, tzinfo=UTC)
        )
        assert intervals[-1] == TZInterval(
            datetime(2026, 6, 1, tzinfo=UTC), datetime(2026, 6, 15, tzinfo=UTC)
        )


class TestFlexibleBackfillSplits:
    """A flexible model has two independent knobs: `chunk` slices the span, and
    `floor_chunk` floors the *inferred* trailing edge. Backfill a multi-year
    range with no explicit `until`, chunked `@yearly`, and the whole years split
    cleanly while the final partial year snaps to `floor_chunk`'s latest
    complete unit. 'now' = _NOW (2026-06-25 12:00 UTC)."""

    _BEGIN = datetime(2021, 1, 1, tzinfo=UTC)
    _WHOLE_YEARS = [
        TZInterval(datetime(y, 1, 1, tzinfo=UTC), datetime(y + 1, 1, 1, tzinfo=UTC))
        for y in (2021, 2022, 2023, 2024, 2025)
    ]

    @pytest.mark.parametrize(
        "floor_chunk, tail",
        [
            (None, None),  # floor falls back to the chunk (year) → no partial tail
            ("@yearly", None),  # explicit year floor → same as above
            ("@monthly", TZInterval(_LCY, _LCM)),  # partial year → last complete month
            ("@daily", TZInterval(_LCY, _LCD)),  # partial year → last complete day
        ],
        ids=["floor=chunk", "floor=year", "floor=month", "floor=day"],
    )
    def test_yearly_backfill_tail_snaps_to_floor_chunk(self, floor_chunk, tail) -> None:
        batch = Batch(
            time=TimeChunking(
                chunk="@yearly",
                flexibility=ChunkFlex(floor_chunk=floor_chunk),
                tz=UTC,
            )
        )
        with travel(_NOW, tick=False):
            # no-dates backfill: since given, until inferred → the trailing edge
            w = resolve_window(batch, Contract(begin=self._BEGIN), since=self._BEGIN)
            intervals = split_window(w, batch.time.chunk)
        assert intervals == self._WHOLE_YEARS + ([tail] if tail else [])

    # Full cross-product: chunk × floor_chunk. The edge floors on `floor_chunk or
    # chunk` (so it depends only on the floor grain, never the slicing chunk); the
    # last interval is whole when the edge lands on a chunk boundary, partial
    # otherwise. 'now' = 2026-06-25 12:00 → _LCY/_LCM/_LCD.
    _MATRIX = [
        # (chunk, floor_chunk, edge, last-interval `since`)
        ("@yearly", None, _LCY, datetime(2025, 1, 1, tzinfo=UTC)),  # whole year
        ("@yearly", "@monthly", _LCM, datetime(2026, 1, 1, tzinfo=UTC)),  # partial
        ("@yearly", "@daily", _LCD, datetime(2026, 1, 1, tzinfo=UTC)),  # partial
        ("@monthly", None, _LCM, datetime(2026, 5, 1, tzinfo=UTC)),  # whole month
        ("@monthly", "@daily", _LCD, datetime(2026, 6, 1, tzinfo=UTC)),  # partial
        (
            "@monthly",
            "@yearly",
            _LCY,
            datetime(2025, 12, 1, tzinfo=UTC),
        ),  # coarser floor
        ("@daily", None, _LCD, datetime(2026, 6, 24, tzinfo=UTC)),  # whole day
        (
            "@daily",
            "@monthly",
            _LCM,
            datetime(2026, 5, 31, tzinfo=UTC),
        ),  # coarser floor
        (
            "@daily",
            "@yearly",
            _LCY,
            datetime(2025, 12, 31, tzinfo=UTC),
        ),  # coarser floor
    ]

    @pytest.mark.parametrize(
        "chunk, floor_chunk, edge, last_since",
        _MATRIX,
        ids=[
            f"{c.strip('@')}-floor-{(f or 'none').strip('@')}" for c, f, *_ in _MATRIX
        ],
    )
    def test_edge_and_tail_across_chunk_x_floor(
        self, chunk, floor_chunk, edge, last_since
    ) -> None:
        batch = Batch(
            time=TimeChunking(
                chunk=chunk,
                flexibility=ChunkFlex(floor_chunk=floor_chunk),
                tz=UTC,
            )
        )
        with travel(_NOW, tick=False):
            w = resolve_window(batch, Contract(begin=self._BEGIN), since=self._BEGIN)
            intervals = split_window(w, batch.time.chunk)
        assert w.until == edge  # edge floors on `floor_chunk or chunk`
        assert intervals[-1] == TZInterval(last_since, edge)  # whole or partial tail

    def test_fine_chunk_coarse_floor_releases_only_settled_units(self) -> None:
        # The floor can be COARSER than the chunk: chunk daily but only release
        # whole months (floor_chunk="@monthly"). The edge stops at the last
        # complete month — daily slices up to it, never a partial day beyond.
        batch = Batch(
            time=TimeChunking(
                chunk="@daily",
                flexibility=ChunkFlex(floor_chunk="@monthly"),
                tz=UTC,
            )
        )
        begin = datetime(2026, 5, 28, tzinfo=UTC)
        with travel(_NOW, tick=False):
            w = resolve_window(batch, Contract(begin=begin), since=begin)
            intervals = split_window(w, batch.time.chunk)
        assert w.until == _LCM  # last complete MONTH, not the last complete day
        days = [datetime(2026, 5, d, tzinfo=UTC) for d in (28, 29, 30, 31)] + [_LCM]
        assert intervals == [TZInterval(a, b) for a, b in zip(days, days[1:])]


class TestSplitAtTimes:
    """`split_at_times` — force extra boundaries (a run window's edges) into an interval
    so no piece straddles them."""

    _IV = TZInterval(
        datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 1, 11, tzinfo=UTC)
    )

    def test_interior_points_split_left_to_right(self) -> None:
        d3 = datetime(2026, 1, 4, tzinfo=UTC)
        d7 = datetime(2026, 1, 8, tzinfo=UTC)
        assert split_at_times(
            self._IV, [d7, d3]
        ) == [  # unordered input, ordered output
            TZInterval(self._IV.since, d3),
            TZInterval(d3, d7),
            TZInterval(d7, self._IV.until),
        ]

    def test_boundary_and_outside_points_are_ignored(self) -> None:
        before = datetime(2025, 12, 1, tzinfo=UTC)
        after = datetime(2026, 2, 1, tzinfo=UTC)
        # points on the bounds or outside are no-ops → interval unchanged
        assert split_at_times(
            self._IV, [self._IV.since, self._IV.until, before, after]
        ) == [self._IV]

    def test_duplicate_points_collapse(self) -> None:
        mid = datetime(2026, 1, 6, tzinfo=UTC)
        assert split_at_times(self._IV, [mid, mid]) == [
            TZInterval(self._IV.since, mid),
            TZInterval(mid, self._IV.until),
        ]


class TestUnbatchedWindow:
    """`resolve_window` with `batching=None` — the single-row span an unbatched
    model records. The whole point: an open temporal contract must still resolve
    to a real `[begin, edge]` window so it registers a non-NULL state row.
    `uncovered_gaps` only counts `since IS NOT NULL` rows, so an unbatched
    temporal oneshot that fell through to `None` (a NULL-window row) reported its
    entire contract as an uncovered gap. 'now' = 2026-06-25 12:00 UTC."""

    def test_temporal_closed_contract_spans_begin_to_end(self) -> None:
        # Both bounds set → the literal [begin, end] span (no clock read).
        w = resolve_window(
            None, Contract(begin=_BEGIN, end=_PAST), temporality=Temporality.TEMPORAL
        )
        assert w == TZInterval(_BEGIN, _PAST)

    def test_temporal_open_contract_closes_at_latest_complete_day(self) -> None:
        # The fix: open end (no contract.end) → [begin, latest complete day],
        # not None. An unbatched model has no chunk, so the edge defaults to
        # whole days.
        with travel(_NOW, tick=False):
            w = resolve_window(
                None, Contract(begin=_BEGIN), temporality=Temporality.TEMPORAL
            )
        assert w == TZInterval(_BEGIN, _LCD)

    def test_open_contract_edge_uses_the_begin_timezone(self) -> None:
        # The daily floor is taken in the contract's own tz, so the day boundary
        # is local midnight (CET), not UTC midnight.
        begin = datetime(2024, 1, 1, tzinfo=CET)
        with travel(_NOW, tick=False):
            w = resolve_window(
                None, Contract(begin=begin), temporality=Temporality.TEMPORAL
            )
        assert w.since == begin
        assert w.until == datetime(2026, 6, 25, tzinfo=CET)  # last complete CET day

    def test_temporal_open_contract_is_the_default_temporality(self) -> None:
        # temporality defaults to TEMPORAL, so omitting it resolves a span too.
        with travel(_NOW, tick=False):
            w = resolve_window(None, Contract(begin=_BEGIN))
        assert w == TZInterval(_BEGIN, _LCD)

    def test_timeless_open_contract_is_a_null_window_oneshot(self) -> None:
        # A TIMELESS model never spans, even with a begin — it gets None (→ the
        # NULL-window one-shot row). This is the gate the temporality arg adds.
        w = resolve_window(
            None, Contract(begin=_BEGIN), temporality=Temporality.TIMELESS
        )
        assert w is None

    def test_temporal_rangeless_contract_is_a_null_window_oneshot(self) -> None:
        # No begin → nothing to span → None (NULL-window one-shot row), regardless
        # of temporality.
        assert (
            resolve_window(None, Contract(), temporality=Temporality.TEMPORAL) is None
        )
