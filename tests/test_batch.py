from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pytest
from time_machine import travel

from bollhav.model.batch import (
    Batch,
    IntervalChunks,
    _resolve_cron,
    _resolve_cron_interval,
    _chunk_interval,
)
from bollhav.model.intervals import TZInterval
from bollhav.model.model import Model
from bollhav.model.target import Target


CET = ZoneInfo("Europe/Stockholm")
UTC = timezone.utc


def _model(
    interval_expression="@hourly",
    tz=UTC,
    lookback=None,
    retries=None,
    bounds=None,
    interval_expression_override=None,
    tz_override=None,
    since=None,
    until=None,
    latest=False,
) -> Model:
    """Build a Model with pipe overrides already baked in — mirrors what
    `apply_pipe` would produce. `interval_expression_override` and
    `tz_override` are applied on top of the base interval config so
    model.intervals tests can exercise both layers."""
    effective_expr = interval_expression_override or interval_expression
    effective_tz = tz_override or tz
    m = Model(
        target=Target(name="test"),
        batching=Batch(
            interval=IntervalChunks(
                expression=effective_expr,
                tz=effective_tz,
                lookback=lookback,
            ),
            retries=retries,
        ),
        bounds=bounds,
    )
    m.directives.since = since
    m.directives.until = until
    m.directives.latest = latest
    return m


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


class TestResolveCronInterval:
    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_utc_hourly(self) -> None:
        since, until = _resolve_cron_interval("0 * * * *", tz=UTC)
        assert since == datetime(2024, 6, 15, 13, 0, tzinfo=UTC)
        assert until == datetime(2024, 6, 15, 14, 0, tzinfo=UTC)

    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_cet_hourly(self) -> None:
        since, until = _resolve_cron_interval("0 * * * *", tz=CET)
        assert since == datetime(2024, 6, 15, 15, 0, tzinfo=CET)
        assert until == datetime(2024, 6, 15, 16, 0, tzinfo=CET)

    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_utc_daily(self) -> None:
        since, until = _resolve_cron_interval("0 0 * * *", tz=UTC)
        assert since == datetime(2024, 6, 14, 0, 0, tzinfo=UTC)
        assert until == datetime(2024, 6, 15, 0, 0, tzinfo=UTC)

    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_defaults_to_utc(self) -> None:
        since, until = _resolve_cron_interval("0 * * * *")
        assert since == datetime(2024, 6, 15, 13, 0, tzinfo=UTC)
        assert until == datetime(2024, 6, 15, 14, 0, tzinfo=UTC)


class TestChunkInterval:
    def test_hourly_chunks_over_three_hours(self) -> None:
        since = datetime(2024, 6, 15, 10, 0, tzinfo=UTC)
        until = datetime(2024, 6, 15, 13, 0, tzinfo=UTC)
        intervals = _chunk_interval("0 * * * *", TZInterval(since, until))
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
        intervals = _chunk_interval("0 * * * *", TZInterval(since, until))
        assert len(intervals) == 1
        assert intervals[0].since == since
        assert intervals[0].until == until

    def test_partial_trailing_chunk(self) -> None:
        since = datetime(2024, 6, 15, 10, 0, tzinfo=UTC)
        until = datetime(2024, 6, 15, 11, 30, tzinfo=UTC)
        intervals = _chunk_interval("0 * * * *", TZInterval(since, until))
        assert len(intervals) == 2
        assert intervals[0].until == datetime(2024, 6, 15, 11, 0, tzinfo=UTC)
        assert intervals[1].since == datetime(2024, 6, 15, 11, 0, tzinfo=UTC)
        assert intervals[1].until == datetime(2024, 6, 15, 11, 30, tzinfo=UTC)

    def test_daily_chunks(self) -> None:
        since = datetime(2024, 6, 15, 0, 0, tzinfo=UTC)
        until = datetime(2024, 6, 17, 0, 0, tzinfo=UTC)
        intervals = _chunk_interval("0 0 * * *", TZInterval(since, until))
        assert len(intervals) == 2


class TestApplyLookback:
    def test_lookback_shifts_since_backwards(self) -> None:
        model = _model(interval_expression="@hourly", lookback=3)
        result = model._apply_lookback(
            "0 * * * *", datetime(2024, 6, 15, 14, 0, tzinfo=UTC)
        )
        assert result == datetime(2024, 6, 15, 11, 0, tzinfo=UTC)

    def test_lookback_daily(self) -> None:
        model = _model(interval_expression="@daily", lookback=2)
        result = model._apply_lookback(
            "0 0 * * *", datetime(2024, 6, 15, 0, 0, tzinfo=UTC)
        )
        assert result == datetime(2024, 6, 13, 0, 0, tzinfo=UTC)


class TestLatestCompleteInterval:
    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_hourly(self) -> None:
        model = _model(interval_expression="@hourly", tz=UTC)
        interval = model.latest_complete_interval()
        assert interval.since == datetime(2024, 6, 15, 13, 0, tzinfo=UTC)
        assert interval.until == datetime(2024, 6, 15, 14, 0, tzinfo=UTC)

    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_daily(self) -> None:
        model = _model(interval_expression="@daily", tz=UTC)
        interval = model.latest_complete_interval()
        assert interval.since == datetime(2024, 6, 14, 0, 0, tzinfo=UTC)
        assert interval.until == datetime(2024, 6, 15, 0, 0, tzinfo=UTC)

    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_uses_model_tz(self) -> None:
        model = _model(interval_expression="@daily", tz=CET)
        interval = model.latest_complete_interval()
        assert interval.since == datetime(2024, 6, 14, 0, 0, tzinfo=CET)
        assert interval.until == datetime(2024, 6, 15, 0, 0, tzinfo=CET)

    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_tz_override(self) -> None:
        model = _model(interval_expression="@hourly", tz=CET)
        interval = model.latest_complete_interval(tz_override=UTC)
        assert interval.since == datetime(2024, 6, 15, 13, 0, tzinfo=UTC)
        assert interval.until == datetime(2024, 6, 15, 14, 0, tzinfo=UTC)

    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_interval_expression_override(self) -> None:
        model = _model(interval_expression="@daily", tz=UTC)
        interval = model.latest_complete_interval(
            interval_expression_override="0 * * * *"
        )
        assert interval.since == datetime(2024, 6, 15, 13, 0, tzinfo=UTC)
        assert interval.until == datetime(2024, 6, 15, 14, 0, tzinfo=UTC)


class TestIntervalsTimezone:
    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_latest_uses_model_tz(self) -> None:
        model = _model(
            interval_expression="@hourly",
            tz=CET,
            interval_expression_override="0 * * * *",
            latest=True,
        )
        intervals = list(model.compute_intervals())
        assert len(intervals) == 1
        assert intervals[0].since == datetime(2024, 6, 15, 15, 0, tzinfo=CET)
        assert intervals[0].until == datetime(2024, 6, 15, 16, 0, tzinfo=CET)

    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_latest_uses_utc_by_default(self) -> None:
        model = _model(
            interval_expression="@hourly",
            interval_expression_override="0 * * * *",
            latest=True,
        )
        intervals = list(model.compute_intervals())
        assert len(intervals) == 1
        assert intervals[0].since == datetime(2024, 6, 15, 13, 0, tzinfo=UTC)
        assert intervals[0].until == datetime(2024, 6, 15, 14, 0, tzinfo=UTC)

    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_tz_override_takes_precedence_over_model_tz(self) -> None:
        model = _model(
            interval_expression="@hourly",
            tz=CET,
            interval_expression_override="0 * * * *",
            tz_override=UTC,
            latest=True,
        )
        intervals = list(model.compute_intervals())
        assert len(intervals) == 1
        assert intervals[0].since == datetime(2024, 6, 15, 13, 0, tzinfo=UTC)
        assert intervals[0].until == datetime(2024, 6, 15, 14, 0, tzinfo=UTC)

    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_tz_override_none_falls_back_to_model(self) -> None:
        model = _model(
            interval_expression="@hourly",
            tz=CET,
            interval_expression_override="0 * * * *",
            tz_override=None,
            latest=True,
        )
        intervals = list(model.compute_intervals())
        assert len(intervals) == 1
        assert intervals[0].since == datetime(2024, 6, 15, 15, 0, tzinfo=CET)
        assert intervals[0].until == datetime(2024, 6, 15, 16, 0, tzinfo=CET)


class TestIntervalsLookback:
    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_latest_with_lookback(self) -> None:
        model = _model(
            interval_expression="@hourly",
            lookback=3,
            interval_expression_override="0 * * * *",
            latest=True,
        )
        intervals = list(model.compute_intervals())
        assert intervals[0].since == datetime(2024, 6, 15, 10, 0, tzinfo=UTC)
        assert intervals[-1].until == datetime(2024, 6, 15, 14, 0, tzinfo=UTC)
        assert len(intervals) == 4

    def test_backfill_with_lookback(self) -> None:
        model = _model(
            interval_expression="@hourly",
            lookback=2,
            interval_expression_override="0 * * * *",
            since=datetime(2024, 6, 15, 12, 0, tzinfo=UTC),
            until=datetime(2024, 6, 15, 14, 0, tzinfo=UTC),
        )
        intervals = list(model.compute_intervals())
        assert intervals[0].since == datetime(2024, 6, 15, 10, 0, tzinfo=UTC)
        assert intervals[-1].until == datetime(2024, 6, 15, 14, 0, tzinfo=UTC)
        assert len(intervals) == 4

    def test_no_lookback(self) -> None:
        model = _model(
            interval_expression="@hourly",
            interval_expression_override="0 * * * *",
            since=datetime(2024, 6, 15, 12, 0, tzinfo=UTC),
            until=datetime(2024, 6, 15, 14, 0, tzinfo=UTC),
        )
        intervals = list(model.compute_intervals())
        assert intervals[0].since == datetime(2024, 6, 15, 12, 0, tzinfo=UTC)
        assert len(intervals) == 2


class TestIntervalsNoneInputs:
    """Backfill mode is now strict: `directives.until` must be set,
    no silent fallback to `latest_complete_interval()`. These tests
    pin that behaviour so a future refactor can't bring the implicit
    fallback back."""

    def test_until_none_in_backfill_raises(self) -> None:
        model = _model(
            interval_expression="@hourly",
            interval_expression_override="0 * * * *",
            since=datetime(2024, 6, 15, 12, 0, tzinfo=UTC),
        )
        with pytest.raises(ValueError, match="backfill requires an explicit until"):
            _ = model.compute_intervals()

    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_until_set_explicitly_in_backfill_is_honored(self) -> None:
        """`directives.until` is now required — set it explicitly,
        get the intervals the caller asked for."""
        model = _model(
            interval_expression="@hourly",
            interval_expression_override="0 * * * *",
            since=datetime(2024, 6, 15, 12, 0, tzinfo=UTC),
            until=datetime(2024, 6, 15, 14, 0, tzinfo=UTC),
        )
        intervals = list(model.compute_intervals())
        assert intervals[0].since == datetime(2024, 6, 15, 12, 0, tzinfo=UTC)
        assert intervals[-1].until == datetime(2024, 6, 15, 14, 0, tzinfo=UTC)
        assert len(intervals) == 2

    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_latest_mode_auto_derives_until(self) -> None:
        """If you want the latest-complete-tick semantic, use `latest`
        mode — which is what `LATEST_ENABLED=true` opts into."""
        model = _model(
            interval_expression="@hourly",
            interval_expression_override="0 * * * *",
            latest=True,
        )
        intervals = list(model.compute_intervals())
        assert intervals[-1].until == datetime(2024, 6, 15, 14, 0, tzinfo=UTC)

    def test_since_none_without_latest_raises(self) -> None:
        model = _model(
            interval_expression="@hourly",
            interval_expression_override="0 * * * *",
            until=datetime(2024, 6, 15, 14, 0, tzinfo=UTC),
        )
        with pytest.raises(ValueError, match="backfill requires a since value"):
            model.compute_intervals()

    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_both_none_without_latest_raises(self) -> None:
        model = _model(
            interval_expression="@hourly", interval_expression_override="0 * * * *"
        )
        with pytest.raises(ValueError, match="backfill requires a since value"):
            model.compute_intervals()

    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_both_none_with_latest_resolves(self) -> None:
        model = _model(
            interval_expression="@hourly",
            interval_expression_override="0 * * * *",
            latest=True,
        )
        intervals = list(model.compute_intervals())
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
        intervals = list(model.compute_intervals())
        assert len(intervals) == 2
        assert intervals[0].since == datetime(2024, 6, 15, 12, 0, tzinfo=UTC)
        assert intervals[0].until == datetime(2024, 6, 15, 13, 0, tzinfo=UTC)
