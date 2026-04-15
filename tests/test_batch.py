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


CET = ZoneInfo("Europe/Stockholm")
UTC = timezone.utc


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
        intervals = _chunk_interval("0 * * * *", since, until)
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
        intervals = _chunk_interval("0 * * * *", since, until)
        assert len(intervals) == 1
        assert intervals[0].since == since
        assert intervals[0].until == until

    def test_partial_trailing_chunk(self) -> None:
        since = datetime(2024, 6, 15, 10, 0, tzinfo=UTC)
        until = datetime(2024, 6, 15, 11, 30, tzinfo=UTC)
        intervals = _chunk_interval("0 * * * *", since, until)
        assert len(intervals) == 2
        assert intervals[0].until == datetime(2024, 6, 15, 11, 0, tzinfo=UTC)
        assert intervals[1].since == datetime(2024, 6, 15, 11, 0, tzinfo=UTC)
        assert intervals[1].until == datetime(2024, 6, 15, 11, 30, tzinfo=UTC)

    def test_daily_chunks(self) -> None:
        since = datetime(2024, 6, 15, 0, 0, tzinfo=UTC)
        until = datetime(2024, 6, 17, 0, 0, tzinfo=UTC)
        intervals = _chunk_interval("0 0 * * *", since, until)
        assert len(intervals) == 2


class TestApplyLookback:
    def test_lookback_shifts_since_backwards(self) -> None:
        batch = Batch(batch_expression="@hourly", lookback=3)
        result = batch._apply_lookback(
            "0 * * * *", datetime(2024, 6, 15, 14, 0, tzinfo=UTC)
        )
        assert result == datetime(2024, 6, 15, 11, 0, tzinfo=UTC)

    def test_lookback_daily(self) -> None:
        batch = Batch(batch_expression="@daily", lookback=2)
        result = batch._apply_lookback(
            "0 0 * * *", datetime(2024, 6, 15, 0, 0, tzinfo=UTC)
        )
        assert result == datetime(2024, 6, 13, 0, 0, tzinfo=UTC)


class TestInferIntervalsTimezone:
    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_latest_uses_model_tz(self) -> None:
        batch = Batch(batch_expression="@hourly", tz=CET)
        intervals = batch.infer_intervals(
            since=None, until=None, batch_expression="0 * * * *", latest=True
        )
        assert len(intervals) == 1
        assert intervals[0].since == datetime(2024, 6, 15, 15, 0, tzinfo=CET)
        assert intervals[0].until == datetime(2024, 6, 15, 16, 0, tzinfo=CET)

    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_latest_uses_utc_by_default(self) -> None:
        batch = Batch(batch_expression="@hourly")
        intervals = batch.infer_intervals(
            since=None, until=None, batch_expression="0 * * * *", latest=True
        )
        assert len(intervals) == 1
        assert intervals[0].since == datetime(2024, 6, 15, 13, 0, tzinfo=UTC)
        assert intervals[0].until == datetime(2024, 6, 15, 14, 0, tzinfo=UTC)

    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_tz_override_takes_precedence_over_model_tz(self) -> None:
        batch = Batch(batch_expression="@hourly", tz=CET)
        intervals = batch.infer_intervals(
            since=None,
            until=None,
            batch_expression="0 * * * *",
            latest=True,
            tz_override=UTC,
        )
        assert len(intervals) == 1
        assert intervals[0].since == datetime(2024, 6, 15, 13, 0, tzinfo=UTC)
        assert intervals[0].until == datetime(2024, 6, 15, 14, 0, tzinfo=UTC)

    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_tz_override_none_falls_back_to_model(self) -> None:
        batch = Batch(batch_expression="@hourly", tz=CET)
        intervals = batch.infer_intervals(
            since=None,
            until=None,
            batch_expression="0 * * * *",
            latest=True,
            tz_override=None,
        )
        assert len(intervals) == 1
        assert intervals[0].since == datetime(2024, 6, 15, 15, 0, tzinfo=CET)
        assert intervals[0].until == datetime(2024, 6, 15, 16, 0, tzinfo=CET)


class TestInferIntervalsLookback:
    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_latest_with_lookback(self) -> None:
        batch = Batch(batch_expression="@hourly", lookback=3)
        intervals = batch.infer_intervals(
            since=None, until=None, batch_expression="0 * * * *", latest=True
        )
        # Latest interval is 13:00-14:00, lookback=3 shifts since to 10:00
        assert intervals[0].since == datetime(2024, 6, 15, 10, 0, tzinfo=UTC)
        assert intervals[-1].until == datetime(2024, 6, 15, 14, 0, tzinfo=UTC)
        assert len(intervals) == 4

    def test_backfill_with_lookback(self) -> None:
        batch = Batch(batch_expression="@hourly", lookback=2)
        intervals = batch.infer_intervals(
            since=datetime(2024, 6, 15, 12, 0, tzinfo=UTC),
            until=datetime(2024, 6, 15, 14, 0, tzinfo=UTC),
            batch_expression="0 * * * *",
        )
        # since=12:00, lookback=2 shifts to 10:00, until stays 14:00
        assert intervals[0].since == datetime(2024, 6, 15, 10, 0, tzinfo=UTC)
        assert intervals[-1].until == datetime(2024, 6, 15, 14, 0, tzinfo=UTC)
        assert len(intervals) == 4

    def test_no_lookback(self) -> None:
        batch = Batch(batch_expression="@hourly")
        intervals = batch.infer_intervals(
            since=datetime(2024, 6, 15, 12, 0, tzinfo=UTC),
            until=datetime(2024, 6, 15, 14, 0, tzinfo=UTC),
            batch_expression="0 * * * *",
        )
        assert intervals[0].since == datetime(2024, 6, 15, 12, 0, tzinfo=UTC)
        assert len(intervals) == 2


class TestInferIntervalsNoneInputs:
    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_until_none_defaults_to_now(self) -> None:
        batch = Batch(batch_expression="@hourly")
        intervals = batch.infer_intervals(
            since=datetime(2024, 6, 15, 12, 0, tzinfo=UTC),
            until=None,
            batch_expression="0 * * * *",
        )
        assert intervals[0].since == datetime(2024, 6, 15, 12, 0, tzinfo=UTC)
        assert intervals[-1].until == datetime(2024, 6, 15, 14, 35, tzinfo=UTC)

    def test_since_none_without_latest_raises(self) -> None:
        batch = Batch(batch_expression="@hourly")
        with pytest.raises(TypeError):
            batch.infer_intervals(
                since=None,
                until=datetime(2024, 6, 15, 14, 0, tzinfo=UTC),
                batch_expression="0 * * * *",
            )

    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_both_none_without_latest_raises(self) -> None:
        batch = Batch(batch_expression="@hourly")
        with pytest.raises(TypeError):
            batch.infer_intervals(
                since=None,
                until=None,
                batch_expression="0 * * * *",
            )

    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_both_none_with_latest_resolves(self) -> None:
        batch = Batch(batch_expression="@hourly")
        intervals = batch.infer_intervals(
            since=None,
            until=None,
            batch_expression="0 * * * *",
            latest=True,
        )
        assert len(intervals) == 1
        assert intervals[0].since == datetime(2024, 6, 15, 13, 0, tzinfo=UTC)
        assert intervals[0].until == datetime(2024, 6, 15, 14, 0, tzinfo=UTC)
