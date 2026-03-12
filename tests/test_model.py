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
