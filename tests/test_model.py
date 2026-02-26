import pytest
from unittest.mock import patch
from bollhav.database import Database
from bollhav.implementations.postgres import PostgresColumn, PostgresType
from bollhav.implementations.parquet import ParquetColumn
from bollhav.modes import WriteMode, ModelType
from bollhav.model import Model
from bollhav.implementations.parquet import ParquetColumn, ParquetType


def make_postgres_columns(sensitive: bool = False) -> list[PostgresColumn]:
    return [
        PostgresColumn(
            name="id",
            data_type=PostgresType.BIGINT,
            primary_key=True,
            nullable=False,
            order=0,
        ),
        PostgresColumn(
            name="email",
            data_type=PostgresType.TEXT,
            nullable=True,
            order=1,
            sensitive=sensitive,
        ),
    ]


def make_parquet_columns(sensitive: bool = False) -> list[ParquetColumn]:
    return [
        ParquetColumn(name="id", data_type=ParquetType.INT64, nullable=False, order=0),
        ParquetColumn(
            name="email",
            data_type=ParquetType.BYTE_ARRAY,
            nullable=True,
            order=1,
            sensitive=sensitive,
        ),
    ]


# --- defaults ---


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
    assert m.partitioned_by is None
    assert m.sensitive is False
    assert m.extra == {}


# --- model_type / write_mode validation ---


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


# --- database / columns validation ---


def test_database_without_columns_raises():
    with pytest.raises(
        ValueError, match="columns must be set when database is provided"
    ):
        Model(name="test", source_entity="raw.orders", database=Database.POSTGRES)


def test_columns_without_database_raises():
    with pytest.raises(
        ValueError, match="database must be set when columns is provided"
    ):
        Model(name="test", source_entity="raw.orders", columns=make_postgres_columns())


def test_database_and_postgres_columns_accepted():
    m = Model(
        name="test",
        source_entity="raw.orders",
        database=Database.POSTGRES,
        columns=make_postgres_columns(),
    )
    assert m.database == Database.POSTGRES
    assert len(m.columns) == 2


def test_database_and_parquet_columns_accepted():
    m = Model(
        name="test",
        source_entity="raw.orders",
        database=Database.PARQUET,
        columns=make_parquet_columns(),
    )
    assert m.database == Database.PARQUET
    assert len(m.columns) == 2


# --- partitioned_by ---


def test_partitioned_by_valid_column():
    m = Model(
        name="test",
        source_entity="raw.orders",
        database=Database.POSTGRES,
        columns=make_postgres_columns(),
        partitioned_by=["id"],
    )
    assert m.partitioned_by == ["id"]


def test_partitioned_by_multiple_valid_columns():
    m = Model(
        name="test",
        source_entity="raw.orders",
        database=Database.POSTGRES,
        columns=make_postgres_columns(),
        partitioned_by=["id", "email"],
    )
    assert m.partitioned_by == ["id", "email"]


def test_partitioned_by_unknown_column_raises():
    with pytest.raises(ValueError, match="partitioned_by references unknown columns"):
        Model(
            name="test",
            source_entity="raw.orders",
            database=Database.POSTGRES,
            columns=make_postgres_columns(),
            partitioned_by=["nonexistent"],
        )


def test_partitioned_by_mix_of_valid_and_invalid_raises():
    with pytest.raises(ValueError, match="partitioned_by references unknown columns"):
        Model(
            name="test",
            source_entity="raw.orders",
            database=Database.POSTGRES,
            columns=make_postgres_columns(),
            partitioned_by=["id", "nonexistent"],
        )


def test_partitioned_by_none_without_columns_is_fine():
    m = Model(name="test", source_entity="raw.orders")
    assert m.partitioned_by is None


def test_partitioned_by_none_with_columns_is_fine():
    m = Model(
        name="test",
        source_entity="raw.orders",
        database=Database.POSTGRES,
        columns=make_postgres_columns(),
        partitioned_by=None,
    )
    assert m.partitioned_by is None


# --- sensitive ---


def test_sensitive_false_when_no_columns():
    m = Model(name="test", source_entity="raw.orders")
    assert m.sensitive is False


def test_sensitive_false_when_no_sensitive_postgres_columns():
    m = Model(
        name="test",
        source_entity="raw.orders",
        database=Database.POSTGRES,
        columns=make_postgres_columns(sensitive=False),
    )
    assert m.sensitive is False


def test_sensitive_true_when_sensitive_postgres_column():
    m = Model(
        name="test",
        source_entity="raw.orders",
        database=Database.POSTGRES,
        columns=make_postgres_columns(sensitive=True),
    )
    assert m.sensitive is True


def test_sensitive_false_when_no_sensitive_parquet_columns():
    m = Model(
        name="test",
        source_entity="raw.orders",
        database=Database.PARQUET,
        columns=make_parquet_columns(sensitive=False),
    )
    assert m.sensitive is False


def test_sensitive_true_when_sensitive_parquet_column():
    m = Model(
        name="test",
        source_entity="raw.orders",
        database=Database.PARQUET,
        columns=make_parquet_columns(sensitive=True),
    )
    assert m.sensitive is True


# --- cron / batch_size ---


def test_cron_infers_batch_size():
    with patch("bollhav.model.infer_batch_size", return_value=7) as mock:
        m = Model(name="test", source_entity="raw.orders", cron="0 3 * * *")
        mock.assert_called_once_with("0 3 * * *")
        assert m.batch_size == 7


def test_no_cron_no_batch_size():
    m = Model(name="test", source_entity="raw.orders")
    assert m.batch_size is None


# --- extra kwargs ---


def test_extra_kwargs_stored():
    m = Model(name="test", source_entity="raw.orders", custom_field="hello")
    assert m.extra == {"custom_field": "hello"}


def test_extra_callable_resolved():
    m = Model(
        name="test",
        source_entity="raw.orders",
        static="world",
        dynamic=lambda static: f"hello {static}",
    )
    assert m.extra["dynamic"] == "hello world"


# --- repr ---


def test_repr_contains_name():
    m = Model(name="test", source_entity="raw.orders")
    assert "name='test'" in repr(m)


def test_repr_contains_partitioned_by():
    m = Model(name="test", source_entity="raw.orders")
    assert "partitioned_by=None" in repr(m)


def test_repr_contains_sensitive():
    m = Model(name="test", source_entity="raw.orders")
    assert "sensitive=False" in repr(m)


# --- eq ---


def test_equality():
    a = Model(name="test", source_entity="raw.orders")
    b = Model(name="test", source_entity="raw.orders")
    assert a == b


def test_inequality_by_name():
    a = Model(name="test", source_entity="raw.orders")
    b = Model(name="other", source_entity="raw.orders")
    assert a != b


def test_inequality_by_partitioned_by():
    a = Model(
        name="test",
        source_entity="raw.orders",
        database=Database.POSTGRES,
        columns=make_postgres_columns(),
        partitioned_by=["id"],
    )
    b = Model(
        name="test",
        source_entity="raw.orders",
        database=Database.POSTGRES,
        columns=make_postgres_columns(),
        partitioned_by=None,
    )
    assert a != b


def test_not_equal_to_other_type():
    m = Model(name="test", source_entity="raw.orders")
    assert m.__eq__("not a model") is NotImplemented
