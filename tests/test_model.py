import pytest
from bollhav.model.model import Model
from bollhav.model.source import Source
from bollhav.model.target import Target


def execute():
    pass


def make_model(**overrides) -> Model:
    return Model(
        name=overrides.pop("name", "orders"),
        execute=overrides.pop("execute", execute),
        target=overrides.pop("target", Target(name="orders")),
        source=overrides.pop("source", None),
        **overrides,
    )


def test_model_stores_fields():
    m = make_model()
    assert m.name == "orders"
    assert m.source is None
    assert m.execute is execute


def test_execute_name_validated():
    def not_execute():
        pass

    with pytest.raises(ValueError, match="Expected a function named 'execute'"):
        make_model(execute=not_execute)


def test_model_exposes_sub_configs():
    from bollhav.model.schema import Schema
    from bollhav.model.target import Target
    from bollhav.model.batch import Batch
    from bollhav.model.bounds import Bounds
    from bollhav.model.tags import Tags

    m = make_model()
    assert isinstance(m.target.schema, Schema)
    assert isinstance(m.target, Target)
    assert isinstance(m.batching, Batch)
    assert isinstance(m.bounds, Bounds)
    assert isinstance(m.tags, set)
