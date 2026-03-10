from datetime import datetime, timezone
from bollhav.model.model import Model
from bollhav.model.intervals import TZInterval
from unittest.mock import MagicMock

UTC = timezone.utc


def dt(*args) -> datetime:
    return datetime(*args, tzinfo=UTC)


def make_model(cron: str = "0 * * * *") -> Model:
    config = MagicMock()
    config.cron = cron
    fn = MagicMock()
    fn.__name__ = "execute"
    return Model(model_config=config, execute=fn)


def make_interval(since: datetime, until: datetime) -> TZInterval:
    return TZInterval(since=since, until=until)


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


def test_single_tick_within_interval():
    since = dt(2024, 1, 1, 0, 0)
    until = dt(2024, 1, 1, 2, 0)
    result = make_model("0 * * * *").get_batch_intervals(make_interval(since, until))
    assert len(result) == 2
    assert result[0].since == since
    assert result[0].until == dt(2024, 1, 1, 1, 0)
    assert result[1].since == dt(2024, 1, 1, 1, 0)
    assert result[1].until == until


def test_multiple_ticks_within_interval():
    since = dt(2024, 1, 1, 0, 0)
    until = dt(2024, 1, 1, 3, 0)
    result = make_model("0 * * * *").get_batch_intervals(make_interval(since, until))
    assert len(result) == 3


def test_no_ticks_returns_single_interval():
    since = dt(2024, 1, 1, 0, 0)
    until = dt(2024, 1, 1, 0, 30)
    result = make_model("0 * * * *").get_batch_intervals(make_interval(since, until))
    assert len(result) == 1
    assert result[0].since == since
    assert result[0].until == until


def test_cron_override_used_over_config_cron():
    since = dt(2024, 1, 1, 0, 0)
    until = dt(2024, 1, 1, 1, 0)
    result_default = make_model("0 * * * *").get_batch_intervals(
        make_interval(since, until)
    )
    result_override = make_model("0 * * * *").get_batch_intervals(
        make_interval(since, until), cron_override="*/15 * * * *"
    )
    assert len(result_override) > len(result_default)


def test_interval_boundaries_are_tzinterval_instances():
    since = dt(2024, 1, 1, 0, 0)
    until = dt(2024, 1, 1, 2, 0)
    result = make_model("0 * * * *").get_batch_intervals(make_interval(since, until))
    for item in result:
        assert isinstance(item, TZInterval)


def test_trailing_interval_appended_when_current_lt_until():
    since = dt(2024, 1, 1, 0, 0)
    until = dt(2024, 1, 1, 0, 30)
    result = make_model("0 * * * *").get_batch_intervals(make_interval(since, until))
    assert result[-1].until == until
