from bollhav.model.model import Model
from bollhav.model.target import Target


def make_model(**overrides) -> Model:
    return Model(
        name=overrides.pop("name", "orders"),
        target=overrides.pop("target", Target(name="orders")),
        source=overrides.pop("source", None),
        **overrides,
    )


def test_model_stores_fields():
    m = make_model()
    assert m.name == "orders"
    assert m.source is None


def test_model_exposes_sub_configs():
    from bollhav.model.schema import Schema
    from bollhav.model.target import Target
    from bollhav.model.batch import Batch
    from bollhav.model.bounds import Bounds

    m = make_model()
    assert isinstance(m.target.schema, Schema)
    assert isinstance(m.target, Target)
    assert isinstance(m.batching, Batch)
    assert isinstance(m.bounds, Bounds)
    assert isinstance(m.tags, set)
