from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

import pytest
from time_machine import travel

from bollhav.model.batch import (
    Batch,
    _resolve_cron,
    _resolve_cron_interval,
    _chunk_interval,
)
from bollhav.model.intervals import TZInterval
from bollhav.model.model import Model
from bollhav.model.target import Target
from bollhav.model.bounds import Bounds


CET = ZoneInfo("Europe/Stockholm")
UTC = timezone.utc


def _model(
    batch_expression="@hourly",
    tz=UTC,
    lookback=None,
    retries=None,
    bounds=None,
    batch_expression_override=None,
    tz_override=None,
    since=None,
    until=None,
    latest=False,
) -> Model:
    m = Model(
        target=Target(name="test"),
        batching=Batch(
            batch_expression=batch_expression,
            tz=tz,
            lookback=lookback,
            retries=retries,
        ),
        bounds=bounds,
    )
    m.runtime_override.batch_expression = batch_expression_override
    m.runtime_override.tz = tz_override
    m.runtime_override.since = since
    m.runtime_override.until = until
    m.runtime_override.latest = latest
    return m


class TestResolveCron:
    def test_hourly_alias(self) -> None:
        assert _resolve_cron("@hourly") == "0 * * * *"

    def test_daily_alias(self) -> None:
        assert _resolve_cron("@daily") == "0 0 * * *"

    def test_midnight_alias(self) -> None:
        assert _resolve_cron("@midnight") == "0 0 * * *"

    def test_weekly_alias(self) -> None:
        assert _resolve_cron("@weekly") == "0 0 * * 0"

    def test_monthly_alias(self) -> None:
        assert _resolve_cron("@monthly") == "0 0 1 * *"

    def test_yearly_alias(self) -> None:
        assert _resolve_cron("@yearly") == "0 0 1 1 *"

    def test_annually_alias(self) -> None:
        assert _resolve_cron("@annually") == "0 0 1 1 *"

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
        model = _model(batch_expression="@hourly", lookback=3)
        result = model._apply_lookback(
            "0 * * * *", datetime(2024, 6, 15, 14, 0, tzinfo=UTC)
        )
        assert result == datetime(2024, 6, 15, 11, 0, tzinfo=UTC)

    def test_lookback_daily(self) -> None:
        model = _model(batch_expression="@daily", lookback=2)
        result = model._apply_lookback(
            "0 0 * * *", datetime(2024, 6, 15, 0, 0, tzinfo=UTC)
        )
        assert result == datetime(2024, 6, 13, 0, 0, tzinfo=UTC)


class TestLatestCompleteInterval:
    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_hourly(self) -> None:
        model = _model(batch_expression="@hourly", tz=UTC)
        interval = model.latest_complete_interval()
        assert interval.since == datetime(2024, 6, 15, 13, 0, tzinfo=UTC)
        assert interval.until == datetime(2024, 6, 15, 14, 0, tzinfo=UTC)

    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_daily(self) -> None:
        model = _model(batch_expression="@daily", tz=UTC)
        interval = model.latest_complete_interval()
        assert interval.since == datetime(2024, 6, 14, 0, 0, tzinfo=UTC)
        assert interval.until == datetime(2024, 6, 15, 0, 0, tzinfo=UTC)

    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_uses_model_tz(self) -> None:
        model = _model(batch_expression="@daily", tz=CET)
        interval = model.latest_complete_interval()
        assert interval.since == datetime(2024, 6, 14, 0, 0, tzinfo=CET)
        assert interval.until == datetime(2024, 6, 15, 0, 0, tzinfo=CET)

    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_tz_override(self) -> None:
        model = _model(batch_expression="@hourly", tz=CET)
        interval = model.latest_complete_interval(tz_override=UTC)
        assert interval.since == datetime(2024, 6, 15, 13, 0, tzinfo=UTC)
        assert interval.until == datetime(2024, 6, 15, 14, 0, tzinfo=UTC)

    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_batch_expression_override(self) -> None:
        model = _model(batch_expression="@daily", tz=UTC)
        interval = model.latest_complete_interval(batch_expression_override="0 * * * *")
        assert interval.since == datetime(2024, 6, 15, 13, 0, tzinfo=UTC)
        assert interval.until == datetime(2024, 6, 15, 14, 0, tzinfo=UTC)


class TestInferIntervalsTimezone:
    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_latest_uses_model_tz(self) -> None:
        model = _model(
            batch_expression="@hourly",
            tz=CET,
            batch_expression_override="0 * * * *",
            latest=True,
        )
        intervals = model.infer_intervals()
        assert len(intervals) == 1
        assert intervals[0].since == datetime(2024, 6, 15, 15, 0, tzinfo=CET)
        assert intervals[0].until == datetime(2024, 6, 15, 16, 0, tzinfo=CET)

    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_latest_uses_utc_by_default(self) -> None:
        model = _model(
            batch_expression="@hourly",
            batch_expression_override="0 * * * *",
            latest=True,
        )
        intervals = model.infer_intervals()
        assert len(intervals) == 1
        assert intervals[0].since == datetime(2024, 6, 15, 13, 0, tzinfo=UTC)
        assert intervals[0].until == datetime(2024, 6, 15, 14, 0, tzinfo=UTC)

    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_tz_override_takes_precedence_over_model_tz(self) -> None:
        model = _model(
            batch_expression="@hourly",
            tz=CET,
            batch_expression_override="0 * * * *",
            tz_override=UTC,
            latest=True,
        )
        intervals = model.infer_intervals()
        assert len(intervals) == 1
        assert intervals[0].since == datetime(2024, 6, 15, 13, 0, tzinfo=UTC)
        assert intervals[0].until == datetime(2024, 6, 15, 14, 0, tzinfo=UTC)

    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_tz_override_none_falls_back_to_model(self) -> None:
        model = _model(
            batch_expression="@hourly",
            tz=CET,
            batch_expression_override="0 * * * *",
            tz_override=None,
            latest=True,
        )
        intervals = model.infer_intervals()
        assert len(intervals) == 1
        assert intervals[0].since == datetime(2024, 6, 15, 15, 0, tzinfo=CET)
        assert intervals[0].until == datetime(2024, 6, 15, 16, 0, tzinfo=CET)


class TestInferIntervalsLookback:
    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_latest_with_lookback(self) -> None:
        model = _model(
            batch_expression="@hourly",
            lookback=3,
            batch_expression_override="0 * * * *",
            latest=True,
        )
        intervals = model.infer_intervals()
        assert intervals[0].since == datetime(2024, 6, 15, 10, 0, tzinfo=UTC)
        assert intervals[-1].until == datetime(2024, 6, 15, 14, 0, tzinfo=UTC)
        assert len(intervals) == 4

    def test_backfill_with_lookback(self) -> None:
        model = _model(
            batch_expression="@hourly",
            lookback=2,
            batch_expression_override="0 * * * *",
            since=datetime(2024, 6, 15, 12, 0, tzinfo=UTC),
            until=datetime(2024, 6, 15, 14, 0, tzinfo=UTC),
        )
        intervals = model.infer_intervals()
        assert intervals[0].since == datetime(2024, 6, 15, 10, 0, tzinfo=UTC)
        assert intervals[-1].until == datetime(2024, 6, 15, 14, 0, tzinfo=UTC)
        assert len(intervals) == 4

    def test_no_lookback(self) -> None:
        model = _model(
            batch_expression="@hourly",
            batch_expression_override="0 * * * *",
            since=datetime(2024, 6, 15, 12, 0, tzinfo=UTC),
            until=datetime(2024, 6, 15, 14, 0, tzinfo=UTC),
        )
        intervals = model.infer_intervals()
        assert intervals[0].since == datetime(2024, 6, 15, 12, 0, tzinfo=UTC)
        assert len(intervals) == 2


class TestInferIntervalsNoneInputs:
    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_until_none_defaults_to_last_complete_hourly(self) -> None:
        model = _model(
            batch_expression="@hourly",
            batch_expression_override="0 * * * *",
            since=datetime(2024, 6, 15, 12, 0, tzinfo=UTC),
        )
        intervals = model.infer_intervals()
        assert intervals[0].since == datetime(2024, 6, 15, 12, 0, tzinfo=UTC)
        assert intervals[-1].until == datetime(2024, 6, 15, 14, 0, tzinfo=UTC)
        assert len(intervals) == 2

    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_until_none_defaults_to_last_complete_daily(self) -> None:
        model = _model(
            batch_expression="@daily",
            batch_expression_override="0 0 * * *",
            since=datetime(2024, 6, 14, 0, 0, tzinfo=UTC),
        )
        intervals = model.infer_intervals()
        assert intervals[0].since == datetime(2024, 6, 14, 0, 0, tzinfo=UTC)
        assert intervals[-1].until == datetime(2024, 6, 15, 0, 0, tzinfo=UTC)
        assert len(intervals) == 1

    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_until_none_uses_model_tz_not_override(self) -> None:
        model = _model(
            batch_expression="@daily",
            tz=CET,
            batch_expression_override="0 0 * * *",
            tz_override=UTC,
            since=datetime(2024, 6, 14, 0, 0, tzinfo=CET),
        )
        intervals = model.infer_intervals()
        assert intervals[-1].until == datetime(2024, 6, 15, 0, 0, tzinfo=CET)

    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_until_none_uses_provided_batch_expression_first(self) -> None:
        model = _model(
            batch_expression="@daily",
            tz=UTC,
            batch_expression_override="0 * * * *",
            since=datetime(2024, 6, 15, 12, 0, tzinfo=UTC),
        )
        intervals = model.infer_intervals()
        assert intervals[-1].until == datetime(2024, 6, 15, 14, 0, tzinfo=UTC)

    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_until_none_falls_back_to_model_batch_expression(self) -> None:
        model = _model(
            batch_expression="@daily",
            tz=UTC,
            since=datetime(2024, 6, 14, 0, 0, tzinfo=UTC),
        )
        intervals = model.infer_intervals()
        assert intervals[-1].until == datetime(2024, 6, 15, 0, 0, tzinfo=UTC)

    def test_since_none_without_latest_raises(self) -> None:
        model = _model(
            batch_expression="@hourly",
            batch_expression_override="0 * * * *",
            until=datetime(2024, 6, 15, 14, 0, tzinfo=UTC),
        )
        with pytest.raises(ValueError, match="backfill requires a since value"):
            model.infer_intervals()

    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_both_none_without_latest_raises(self) -> None:
        model = _model(
            batch_expression="@hourly", batch_expression_override="0 * * * *"
        )
        with pytest.raises(ValueError, match="backfill requires a since value"):
            model.infer_intervals()

    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_both_none_with_latest_resolves(self) -> None:
        model = _model(
            batch_expression="@hourly",
            batch_expression_override="0 * * * *",
            latest=True,
        )
        intervals = model.infer_intervals()
        assert len(intervals) == 1
        assert intervals[0].since == datetime(2024, 6, 15, 13, 0, tzinfo=UTC)
        assert intervals[0].until == datetime(2024, 6, 15, 14, 0, tzinfo=UTC)

    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_defaults_to_model_batch_expression(self) -> None:
        model = _model(
            batch_expression="@hourly",
            since=datetime(2024, 6, 15, 12, 0, tzinfo=UTC),
            until=datetime(2024, 6, 15, 14, 0, tzinfo=UTC),
        )
        intervals = model.infer_intervals()
        assert len(intervals) == 2
        assert intervals[0].since == datetime(2024, 6, 15, 12, 0, tzinfo=UTC)
        assert intervals[0].until == datetime(2024, 6, 15, 13, 0, tzinfo=UTC)
