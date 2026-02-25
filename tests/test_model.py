import pytest
import polars as pl
from unittest.mock import MagicMock
from bollhav.modes import WriteMode, ModelType
from bollhav.model import Model


@pytest.fixture
def mock_database():
    return MagicMock()


@pytest.fixture
def mock_columns():
    return [MagicMock()]


def test_basic_model():
    m = Model(name="test", source_table="src")
    assert m.name == "test"
    assert m.source_table == "src"
    assert m.enabled is True
    assert m.debug is False
    assert m.batch_size is None
    assert m.cron is None


def test_defaults():
    m = Model(name="test", source_table="src")
    assert m.table == ""
    assert m.schema == ""
    assert m.database is None
    assert m.columns is None
    assert m.model_type == ModelType.TABLE
    assert m.write_mode == WriteMode.APPEND
    assert m.tags is None


def test_view_mode_valid():
    m = Model(
        name="test",
        source_table="src",
        model_type=ModelType.VIEW,
        write_mode=WriteMode.VIEW,
    )
    assert m.model_type == ModelType.VIEW
    assert m.write_mode == WriteMode.VIEW


def test_view_mode_invalid():
    with pytest.raises(ValueError, match="ModelType.VIEW must use WriteMode.VIEW"):
        Model(
            name="test",
            source_table="src",
            model_type=ModelType.VIEW,
            write_mode=WriteMode.APPEND,
        )


def test_table_mode_cannot_use_view_write_mode():
    with pytest.raises(ValueError, match="ModelType.TABLE cannot use WriteMode.VIEW"):
        Model(
            name="test",
            source_table="src",
            model_type=ModelType.TABLE,
            write_mode=WriteMode.VIEW,
        )


def test_database_without_columns_raises(mock_database):
    with pytest.raises(
        ValueError, match="columns must be set when database is provided"
    ):
        Model(name="test", source_table="src", database=mock_database)


def test_columns_without_database_raises(mock_columns):
    with pytest.raises(
        ValueError, match="database must be set when columns is provided"
    ):
        Model(name="test", source_table="src", columns=mock_columns)


def test_database_with_columns(mock_database, mock_columns):
    m = Model(
        name="test", source_table="src", database=mock_database, columns=mock_columns
    )
    assert m.database == mock_database
    assert m.columns == mock_columns


def test_cron_sets_batch_size():
    m = Model(name="test", source_table="src", cron="0 * * * *")
    assert m.batch_size is not None


def test_invalid_cron_raises():
    with pytest.raises(ValueError, match="Invalid cron expression"):
        Model(name="test", source_table="src", cron="not_a_cron")


def test_debug_flag():
    m = Model(name="test", source_table="src", debug=True)
    assert m.debug is True


def test_enabled_flag():
    m = Model(name="test", source_table="src", enabled=False)
    assert m.enabled is False


def test_kwargs_callable_resolved():
    m = Model(
        name="test", source_table="src", my_val=10, derived=lambda my_val: my_val * 2
    )
    assert m.extra["derived"] == 20


def test_eq():
    m1 = Model(name="test", source_table="src")
    m2 = Model(name="test", source_table="src")
    assert m1 == m2


def test_not_eq():
    m1 = Model(name="test", source_table="src")
    m2 = Model(name="other", source_table="src")
    assert m1 != m2


def test_eq_non_model():
    m = Model(name="test", source_table="src")
    assert m.__eq__("not a model") == NotImplemented


def test_repr():
    m = Model(name="test", source_table="src")
    assert "Model(" in repr(m)
    assert "test" in repr(m)
