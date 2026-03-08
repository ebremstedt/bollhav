from datetime import datetime, timezone
from unittest.mock import MagicMock, patch, call

import pytest

from bollhav.model import Model


UTC = timezone.utc


def make_model(cron: str = "0 * * * *") -> Model:
    config = MagicMock()
    config.cron = cron
    fn = MagicMock()
    fn.__name__ = "execute"
    return Model(model_config=config, execute=fn)


def make_interval(since: datetime, until: datetime) -> MagicMock:
    interval = MagicMock()
    interval.since = since
    interval.until = until
    return interval


def dt(*args) -> datetime:
    return datetime(*args, tzinfo=UTC)


def mock_croniter(ticks: list[datetime]):
    it = MagicMock()
    it.get_next.side_effect = ticks
    return it


# --- dataclass structure ---


def test_model_is_dataclass():
    from dataclasses import fields

    field_names = {f.name for f in fields(Model)}
    assert "model_config" in field_names
    assert "execute" in field_names


def test_model_stores_config_and_execute():
    config = MagicMock()
    fn = MagicMock()
    fn.__name__ = "execute"
    m = Model(model_config=config, execute=fn)
    assert m.model_config is config
    assert m.execute is fn


# --- get_batch_intervals ---


@patch("bollhav.model.croniter")
def test_single_tick_within_interval(mock_cron_cls):
    since = dt(2024, 1, 1, 0, 0)
    tick1 = dt(2024, 1, 1, 1, 0)
    until = dt(2024, 1, 1, 2, 0)

    mock_cron_cls.return_value = mock_croniter([tick1, until])

    model = make_model("0 * * * *")
    result = model.get_batch_intervals(make_interval(since, until))

    assert len(result) == 2
    assert result[0].since == since
    assert result[0].until == tick1
    assert result[1].since == tick1
    assert result[1].until == until


@patch("bollhav.model.croniter")
def test_multiple_ticks_within_interval(mock_cron_cls):
    since = dt(2024, 1, 1, 0, 0)
    tick1 = dt(2024, 1, 1, 1, 0)
    tick2 = dt(2024, 1, 1, 2, 0)
    until = dt(2024, 1, 1, 3, 0)

    mock_cron_cls.return_value = mock_croniter([tick1, tick2, until])

    model = make_model()
    result = model.get_batch_intervals(make_interval(since, until))

    assert len(result) == 3
    assert result[0].since == since and result[0].until == tick1
    assert result[1].since == tick1 and result[1].until == tick2
    assert result[2].since == tick2 and result[2].until == until


@patch("bollhav.model.croniter")
def test_no_ticks_returns_single_interval(mock_cron_cls):
    since = dt(2024, 1, 1, 0, 0)
    until = dt(2024, 1, 1, 1, 0)

    mock_cron_cls.return_value = mock_croniter([until])

    model = make_model()
    result = model.get_batch_intervals(make_interval(since, until))

    assert len(result) == 1
    assert result[0].since == since
    assert result[0].until == until


@patch("bollhav.model.croniter")
def test_tick_exactly_at_until_is_excluded(mock_cron_cls):
    since = dt(2024, 1, 1, 0, 0)
    until = dt(2024, 1, 1, 1, 0)

    mock_cron_cls.return_value = mock_croniter([until])

    model = make_model()
    result = model.get_batch_intervals(make_interval(since, until))

    # tick == until triggers break, so only trailing interval is appended
    assert len(result) == 1
    assert result[0].since == since
    assert result[0].until == until


@patch("bollhav.model.croniter")
def test_cron_override_used_over_config_cron(mock_cron_cls):
    since = dt(2024, 1, 1, 0, 0)
    until = dt(2024, 1, 1, 1, 0)

    mock_cron_cls.return_value = mock_croniter([until])

    model = make_model(cron="0 * * * *")
    model.get_batch_intervals(make_interval(since, until), cron_override="*/15 * * * *")

    mock_cron_cls.assert_called_once_with("*/15 * * * *", since)


@patch("bollhav.model.croniter")
def test_config_cron_used_when_no_override(mock_cron_cls):
    since = dt(2024, 1, 1, 0, 0)
    until = dt(2024, 1, 1, 1, 0)

    mock_cron_cls.return_value = mock_croniter([until])

    model = make_model(cron="0 6 * * *")
    model.get_batch_intervals(make_interval(since, until))

    mock_cron_cls.assert_called_once_with("0 6 * * *", since)


@patch("bollhav.model.croniter")
def test_croniter_initialized_with_interval_since(mock_cron_cls):
    since = dt(2024, 3, 15, 8, 30)
    until = dt(2024, 3, 15, 10, 0)

    mock_cron_cls.return_value = mock_croniter([until])

    model = make_model()
    model.get_batch_intervals(make_interval(since, until))

    mock_cron_cls.assert_called_once_with(model.model_config.cron, since)


@patch("bollhav.model.croniter")
def test_trailing_interval_appended_when_current_lt_until(mock_cron_cls):
    since = dt(2024, 1, 1, 0, 0)
    tick1 = dt(2024, 1, 1, 0, 30)
    until = dt(2024, 1, 1, 1, 0)

    mock_cron_cls.return_value = mock_croniter([tick1, until])

    model = make_model()
    result = model.get_batch_intervals(make_interval(since, until))

    last = result[-1]
    assert last.since == tick1
    assert last.until == until


@patch("bollhav.model.croniter")
def test_returns_empty_when_since_equals_until(mock_cron_cls):
    since = dt(2024, 1, 1, 0, 0)
    until = since

    mock_cron_cls.return_value = mock_croniter([until])

    model = make_model()
    result = model.get_batch_intervals(make_interval(since, until))

    # tick >= until immediately breaks, current == until so no trailing append
    assert result == []


@patch("bollhav.model.croniter")
def test_interval_boundaries_are_tzinterval_instances(mock_cron_cls):
    from bollhav.intervals import TZInterval

    since = dt(2024, 1, 1, 0, 0)
    tick1 = dt(2024, 1, 1, 1, 0)
    until = dt(2024, 1, 1, 2, 0)

    real_it = MagicMock()
    real_it.get_next.side_effect = [tick1, until]
    mock_cron_cls.return_value = real_it

    model = make_model()
    result = model.get_batch_intervals(make_interval(since, until))

    for item in result:
        assert isinstance(item, TZInterval)
