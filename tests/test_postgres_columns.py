import pytest
from bollhav.postgres.columns import PostgresColumn, PostgresType


class TestPostgresColumnDefaults:
    def test_default_data_type(self) -> None:
        col = PostgresColumn(name="col")
        assert col.data_type == PostgresType.TEXT

    def test_default_primary_key(self) -> None:
        col = PostgresColumn(name="col")
        assert col.primary_key is False

    def test_default_unique(self) -> None:
        col = PostgresColumn(name="col")
        assert col.unique is False

    def test_default_precision_none(self) -> None:
        col = PostgresColumn(name="col")
        assert col.precision is None

    def test_default_scale_none(self) -> None:
        col = PostgresColumn(name="col")
        assert col.scale is None

    def test_default_length_none(self) -> None:
        col = PostgresColumn(name="col")
        assert col.length is None


class TestPostgresColumnValidation:
    def test_primary_key_nullable_raises(self) -> None:
        with pytest.raises(ValueError, match="primary_key=True cannot be nullable"):
            PostgresColumn(name="col", primary_key=True, nullable=True)

    def test_primary_key_not_nullable_ok(self) -> None:
        col = PostgresColumn(name="col", primary_key=True, nullable=False)
        assert col.primary_key is True

    def test_nullable_without_primary_key_ok(self) -> None:
        col = PostgresColumn(name="col", nullable=True)
        assert col.nullable is True


class TestPostgresColumnFields:
    def test_data_type_assigned(self) -> None:
        col = PostgresColumn(name="col", data_type=PostgresType.JSONB)
        assert col.data_type == PostgresType.JSONB

    def test_unique_assigned(self) -> None:
        col = PostgresColumn(name="col", unique=True)
        assert col.unique is True

    def test_precision_assigned(self) -> None:
        col = PostgresColumn(name="col", precision=10)
        assert col.precision == 10

    def test_scale_assigned(self) -> None:
        col = PostgresColumn(name="col", scale=2)
        assert col.scale == 2

    def test_length_assigned(self) -> None:
        col = PostgresColumn(name="col", length=255)
        assert col.length == 255


class TestPostgresColumnRepr:
    def test_repr_contains_name(self) -> None:
        col = PostgresColumn(name="my_col")
        assert "my_col" in repr(col)

    def test_repr_contains_data_type(self) -> None:
        col = PostgresColumn(name="col", data_type=PostgresType.BIGINT)
        assert "BIGINT" in repr(col)

    def test_repr_contains_precision_when_set(self) -> None:
        col = PostgresColumn(name="col", precision=5)
        assert "precision=5" in repr(col)

    def test_repr_omits_precision_when_none(self) -> None:
        col = PostgresColumn(name="col")
        assert "precision" not in repr(col)

    def test_repr_contains_scale_when_set(self) -> None:
        col = PostgresColumn(name="col", scale=3)
        assert "scale=3" in repr(col)

    def test_repr_omits_scale_when_none(self) -> None:
        col = PostgresColumn(name="col")
        assert "scale" not in repr(col)

    def test_repr_contains_length_when_set(self) -> None:
        col = PostgresColumn(name="col", length=100)
        assert "length=100" in repr(col)

    def test_repr_omits_length_when_none(self) -> None:
        col = PostgresColumn(name="col")
        assert "length" not in repr(col)


class TestPostgresType:
    def test_text_array_value(self) -> None:
        assert PostgresType.TEXT_ARRAY.value == "TEXT[]"

    def test_character_varying_value(self) -> None:
        assert PostgresType.CHARACTER_VARYING.value == "CHARACTER VARYING"

    def test_double_precision_value(self) -> None:
        assert PostgresType.DOUBLE_PRECISION.value == "DOUBLE PRECISION"
