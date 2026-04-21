from datetime import datetime, timezone

import pytest

from bollhav.model.batch import Batch
from bollhav.model.bounds import Bounds
from bollhav.model.model import Model
from bollhav.model.target import Target


def make_model(**overrides) -> Model:
    # Default to an explicit Batch() so infer_intervals tests exercise
    # the time-chunking path (None would short-circuit to [None]).
    overrides.setdefault("batching", Batch())
    return Model(
        target=overrides.pop("target", Target(name="orders")),
        source=overrides.pop("source", None),
        **overrides,
    )


def test_model_stores_fields():
    m = make_model()
    assert m.target.name == "orders"
    assert m.source is None


def test_model_exposes_sub_configs():
    from bollhav.model.schema import Schema
    from bollhav.model.target import Target
    from bollhav.model.bounds import Bounds

    m = make_model()
    assert isinstance(m.target.schema, Schema)
    assert isinstance(m.target, Target)
    assert isinstance(m.batching, Batch)
    assert isinstance(m.bounds, Bounds)
    assert isinstance(m.tags, set)


def test_batching_defaults_to_none_when_unspecified():
    """No batching kwarg = no chunking. infer_intervals returns [None]."""
    m = Model(target=Target(name="orders"))
    assert m.batching is None
    assert m.infer_intervals() == [None]


def test_infer_intervals_reload_without_bounds_raises():
    m = make_model()
    m.runtime_override.reload = True
    with pytest.raises(ValueError, match="reload requires bounds.begin"):
        m.infer_intervals()


def test_infer_intervals_backfill_without_since_or_bounds_raises():
    m = make_model()
    with pytest.raises(ValueError, match="backfill requires a since value"):
        m.infer_intervals()


def test_infer_intervals_backfill_without_runtime_since_falls_back_to_bounds():
    m = make_model(bounds=Bounds(begin=datetime(2026, 1, 1, tzinfo=timezone.utc)))
    intervals = m.infer_intervals()
    assert len(intervals) > 0
    assert intervals[0].since == datetime(2026, 1, 1, tzinfo=timezone.utc)
