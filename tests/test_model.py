import pytest
from unittest.mock import patch
from bollhav import Model, ModelType, WriteMode, Database, PostgresColumn, PostgresType


def make_columns() -> list[PostgresColumn]:
    return [
        PostgresColumn(
            name="id",
            data_type=PostgresType.BIGINT,
            primary_key=True,
            nullable=False,
            order=0,
        ),
    ]


def test_defaults():
    m = Model(name="test", source_entity="raw.orders")
    assert m.name == "test"
    assert m.source_entity == "raw.orders"
    assert m.table == ""
    assert m.schema == ""
    assert m.database is None
    assert m.columns is None
    assert m.model_type == ModelType.TABLE
    assert m.write_mode == WriteMode.APPEND
    assert m.tags is None
    assert m.cron is None
    assert m.enabled is True
    assert m.debug is False
    assert m.batch_size is None
    assert m.extra == {}


def test_view_must_use_write_mode_view():
    with pytest.raises(ValueError, match="ModelType.VIEW must use WriteMode.VIEW"):
        Model(
            name="test",
            source_entity="raw.orders",
            model_type=ModelType.VIEW,
            write_mode=WriteMode.APPEND,
        )


def test_table_cannot_use_write_mode_view():
    with pytest.raises(ValueError, match="ModelType.TABLE cannot use WriteMode.VIEW"):
        Model(
            name="test",
            source_entity="raw.orders",
            model_type=ModelType.TABLE,
            write_mode=WriteMode.VIEW,
        )


def test_database_without_columns_raises():
    with pytest.raises(
        ValueError, match="columns must be set when database is provided"
    ):
        Model(name="test", source_entity="raw.orders", database=Database.POSTGRES)


def test_columns_without_database_raises():
    with pytest.raises(
        ValueError, match="database must be set when columns is provided"
    ):
        Model(name="test", source_entity="raw.orders", columns=make_columns())


def test_database_and_columns_together():
    m = Model(
        name="test",
        source_entity="raw.orders",
        database=Database.POSTGRES,
        columns=make_columns(),
    )
    assert m.database == Database.POSTGRES
    assert len(m.columns) == 1


def test_cron_infers_batch_size():
    with patch("bollhav.model.infer_batch_size", return_value=7) as mock:
        m = Model(name="test", source_entity="raw.orders", cron="0 3 * * *")
        mock.assert_called_once_with("0 3 * * *")
        assert m.batch_size == 7


def test_no_cron_no_batch_size():
    m = Model(name="test", source_entity="raw.orders")
    assert m.batch_size is None


def test_extra_kwargs_stored():
    m = Model(name="test", source_entity="raw.orders", custom_field="hello")
    assert m.extra == {"custom_field": "hello"}


def test_extra_callable_kwargs_resolved():
    m = Model(
        name="test",
        source_entity="raw.orders",
        static="world",
        dynamic=lambda static: f"hello {static}",
    )
    assert m.extra["dynamic"] == "hello world"


def test_repr():
    m = Model(name="test", source_entity="raw.orders")
    assert "source_entity='raw.orders'" in repr(m)


def test_equality():
    a = Model(name="test", source_entity="raw.orders")
    b = Model(name="test", source_entity="raw.orders")
    assert a == b


def test_inequality():
    a = Model(name="test", source_entity="raw.orders")
    b = Model(name="other", source_entity="raw.orders")
    assert a != b


def test_not_equal_to_other_type():
    m = Model(name="test", source_entity="raw.orders")
    assert m.__eq__("not a model") is NotImplemented
