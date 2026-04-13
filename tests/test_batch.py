from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from unittest.mock import patch

import pytest
from time_machine import travel

from bollhav.model.batch import Batch, _resolve_cron_interval


CET = ZoneInfo("Europe/Stockholm")
UTC = timezone.utc


class TestResolveCronInterval:
    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_utc_hourly(self) -> None:
        since, until = _resolve_cron_interval("0 * * * *", tz=UTC)
        assert since == datetime(2024, 6, 15, 13, 0, tzinfo=UTC)
        assert until == datetime(2024, 6, 15, 14, 0, tzinfo=UTC)

    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_cet_hourly(self) -> None:
        since, until = _resolve_cron_interval("0 * * * *", tz=CET)
        # 14:35 UTC = 16:35 CET, so last complete CET hour is 15:00-16:00 CET
        assert since.tzinfo is not None
        assert until.tzinfo is not None
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
        # Override to UTC, so should get UTC intervals despite model being CET
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
