import pytest
import polars as pl

from bollhav import Model, ModelType, WriteMode, BatchSize
from bollhav.batching import infer_batch_size
from bollhav.database import Database, DatabaseColumn
from bollhav.postgres import PostgresType, PostgresColumn


COLUMNS: dict[str, pl.DataType] = {"id": pl.Int64, "amount": pl.Float64}


class TestModelDefaults:
    def test_defaults(self):
        m = Model(name="orders", source_table="raw.orders", columns=COLUMNS)
        assert m.model_type == ModelType.TABLE
        assert m.write_mode == WriteMode.APPEND
        assert m.tags is None
        assert m.cron is None
        assert m.batch_size is None
        assert m.extra == {}

    def test_destination_defaults_to_empty(self):
        m = Model(name="orders", source_table="raw.orders", columns=COLUMNS)
        assert m.destination_table == ""
        assert m.destination_schema == ""

    def test_destination_db_defaults_to_none(self):
        m = Model(name="orders", source_table="raw.orders", columns=COLUMNS)
        assert m.destination_db is None
        assert m.destination_columns is None


class TestModelValidation:
    def test_view_type_requires_view_write_mode(self):
        with pytest.raises(ValueError):
            Model(
                name="v",
                source_table="raw.orders",
                columns=COLUMNS,
                model_type=ModelType.VIEW,
                write_mode=WriteMode.APPEND,
            )

    def test_table_type_cannot_use_view_write_mode(self):
        with pytest.raises(ValueError):
            Model(
                name="t",
                source_table="raw.orders",
                columns=COLUMNS,
                model_type=ModelType.TABLE,
                write_mode=WriteMode.VIEW,
            )

    def test_view_type_with_view_write_mode_is_valid(self):
        m = Model(
            name="v",
            source_table="raw.orders",
            columns=COLUMNS,
            model_type=ModelType.VIEW,
            write_mode=WriteMode.VIEW,
        )
        assert m.model_type == ModelType.VIEW
        assert m.write_mode == WriteMode.VIEW

    def test_destination_db_without_columns_raises(self):
        with pytest.raises(ValueError):
            Model(
                name="orders",
                source_table="raw.orders",
                columns=COLUMNS,
                destination_db=Database.POSTGRES,
            )

    def test_destination_columns_without_db_raises(self):
        with pytest.raises(ValueError):
            Model(
                name="orders",
                source_table="raw.orders",
                columns=COLUMNS,
                destination_columns=[
                    PostgresColumn(name="id", data_type=PostgresType.INTEGER)
                ],
            )

    def test_destination_db_and_columns_valid(self):
        m = Model(
            name="orders",
            source_table="raw.orders",
            columns=COLUMNS,
            destination_db=Database.POSTGRES,
            destination_columns=[
                PostgresColumn(name="id", data_type=PostgresType.INTEGER)
            ],
        )
        assert m.destination_db == Database.POSTGRES
        assert len(m.destination_columns) == 1


class TestBatchSizeInference:
    @pytest.mark.parametrize(
        "cron,expected",
        [
            ("0 0 1 1 *", BatchSize.YEARLY),
            ("0 0 1 * *", BatchSize.MONTHLY),
            ("0 0 * * 0", BatchSize.WEEKLY),
            ("0 3 * * *", BatchSize.DAILY),
            ("0 * * * *", BatchSize.HOURLY),
            ("*/15 * * * *", None),
        ],
    )
    def test_infer_batch_size(self, cron: str, expected: BatchSize):
        assert infer_batch_size(cron) == expected

    def test_invalid_cron_raises(self):
        with pytest.raises(ValueError):
            infer_batch_size("not a cron")

    def test_batch_size_set_on_model(self):
        m = Model(
            name="orders", source_table="raw.orders", columns=COLUMNS, cron="0 3 * * *"
        )
        assert m.batch_size == BatchSize.DAILY

    def test_no_cron_gives_none_batch_size(self):
        m = Model(name="orders", source_table="raw.orders", columns=COLUMNS)
        assert m.batch_size is None


class TestPostgresColumn:
    def test_defaults(self):
        col = PostgresColumn(name="id", data_type=PostgresType.INTEGER)
        assert col.nullable is True
        assert col.order is None
        assert col.primary_key is False
        assert col.unique is False

    def test_primary_key_nullable_raises(self):
        with pytest.raises(ValueError, match="primary_key=True cannot be nullable"):
            PostgresColumn(
                name="id",
                data_type=PostgresType.INTEGER,
                primary_key=True,
                nullable=True,
            )

    def test_primary_key_not_nullable_valid(self):
        col = PostgresColumn(
            name="id", data_type=PostgresType.INTEGER, primary_key=True, nullable=False
        )
        assert col.primary_key is True
        assert col.nullable is False

    def test_unique(self):
        col = PostgresColumn(name="email", data_type=PostgresType.TEXT, unique=True)
        assert col.unique is True

    def test_order(self):
        col = PostgresColumn(
            name="created_at", data_type=PostgresType.TIMESTAMPTZ, order=2
        )
        assert col.order == 2

    def test_inherits_database_column(self):
        col = PostgresColumn(name="id", data_type=PostgresType.INTEGER)
        assert isinstance(col, DatabaseColumn)

    def test_repr(self):
        col = PostgresColumn(
            name="id", data_type=PostgresType.INTEGER, nullable=False, primary_key=True
        )
        assert "id" in repr(col)
        assert "INTEGER" in repr(col)
        assert "primary_key=True" in repr(col)

    def test_equality(self):
        a = PostgresColumn(name="id", data_type=PostgresType.INTEGER)
        b = PostgresColumn(name="id", data_type=PostgresType.INTEGER)
        assert a == b

    def test_inequality(self):
        a = PostgresColumn(name="id", data_type=PostgresType.INTEGER)
        b = PostgresColumn(name="id", data_type=PostgresType.BIGINT)
        assert a != b


class TestExtra:
    def test_plain_kwargs_stored_in_extra(self):
        m = Model(name="orders", source_table="raw.orders", columns=COLUMNS, foo="bar")
        assert m.extra["foo"] == "bar"

    def test_callable_kwargs_resolved(self):
        def my_ddl(table_name: str, **kwargs) -> str:
            return f"CREATE TABLE {table_name};"

        m = Model(
            name="orders",
            source_table="raw.orders",
            columns=COLUMNS,
            destination_ddl=my_ddl,
            table_name="orders",
        )
        assert m.extra["destination_ddl"] == "CREATE TABLE orders;"

    def test_callable_receives_non_callable_kwargs(self):
        def my_indexes(table_name: str, schema: str, **kwargs) -> list[str]:
            return [f"CREATE INDEX ON {schema}.{table_name} (id)"]

        m = Model(
            name="orders",
            source_table="raw.orders",
            columns=COLUMNS,
            destination_indexes=my_indexes,
            table_name="orders",
            schema="public",
        )
        assert m.extra["destination_indexes"] == ["CREATE INDEX ON public.orders (id)"]


class TestEquality:
    def test_equal_models(self):
        a = Model(name="orders", source_table="raw.orders", columns=COLUMNS)
        b = Model(name="orders", source_table="raw.orders", columns=COLUMNS)
        assert a == b

    def test_unequal_models(self):
        a = Model(name="orders", source_table="raw.orders", columns=COLUMNS)
        b = Model(name="other", source_table="raw.orders", columns=COLUMNS)
        assert a != b

    def test_not_equal_to_non_model(self):
        m = Model(name="orders", source_table="raw.orders", columns=COLUMNS)
        assert m.__eq__("not a model") == NotImplemented


class TestRepr:
    def test_repr_contains_name(self):
        m = Model(name="orders", source_table="raw.orders", columns=COLUMNS)
        assert "orders" in repr(m)

    def test_repr_contains_destination_db(self):
        m = Model(
            name="orders",
            source_table="raw.orders",
            columns=COLUMNS,
            destination_db=Database.POSTGRES,
            destination_columns=[
                PostgresColumn(name="id", data_type=PostgresType.INTEGER)
            ],
        )
        assert "POSTGRES" in repr(m)
