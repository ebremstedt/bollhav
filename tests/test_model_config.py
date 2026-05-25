from datetime import datetime, timezone
from unittest.mock import MagicMock
import pytest
from bollhav.model.model_type import ModelType
from bollhav.model.model import Model
from bollhav.model.target_schema import TargetSchema
from bollhav.model.target import Target
from bollhav.model.bounds import Bounds
from bollhav.model.tags import Tags
from bollhav.model.write_modes import WriteMode


UTC = timezone.utc


def make_column(
    name: str,
    unique: bool = False,
    sensitive: bool = False,
    partition_on: bool = False,
    primary_key: bool = False,
) -> MagicMock:
    col = MagicMock()
    col.name = name
    col.unique = unique
    col.sensitive = sensitive
    col.partition_on = partition_on
    col.primary_key = primary_key
    return col


def make_db() -> MagicMock:
    return MagicMock()


def make_model(**overrides) -> Model:
    return Model(
        target=overrides.pop("target", Target(name="test_table")),
        source=overrides.pop("source", None),
        **overrides,
    )


# --- Target validation ---


def test_view_model_type_requires_view_write_mode():
    with pytest.raises(ValueError, match="ModelType.VIEW must use WriteMode.VIEW"):
        Target(
            name="test_table", model_type=ModelType.VIEW, write_mode=WriteMode.APPEND
        )


def test_table_model_type_cannot_use_view_write_mode():
    with pytest.raises(ValueError, match="ModelType.TABLE cannot use WriteMode.VIEW"):
        Target(name="test_table", model_type=ModelType.TABLE, write_mode=WriteMode.VIEW)


def test_database_without_columns_raises():
    with pytest.raises(ValueError, match="columns must be set"):
        Target(name="test_table", database=make_db())


def test_columns_without_database_raises():
    with pytest.raises(ValueError, match="database must be set"):
        Target(name="test_table", columns=[make_column("id")])


def test_multiple_partition_columns_raises():
    with pytest.raises(
        ValueError, match="At most one column can have partition_on=True"
    ):
        Target(
            name="test_table",
            database=make_db(),
            columns=[
                make_column("id", partition_on=True),
                make_column("ts", partition_on=True),
            ],
        )


def test_upsert_no_delete_without_unique_or_pk_raises():
    with pytest.raises(ValueError, match="primary_key=True or unique=True"):
        Target(
            name="test_table",
            database=make_db(),
            columns=[make_column("id"), make_column("ts")],
            write_mode=WriteMode.UPSERT_NO_DELETE,
        )


def test_upsert_no_delete_with_primary_key_only_is_ok():
    t = Target(
        name="test_table",
        database=make_db(),
        columns=[make_column("id", primary_key=True), make_column("ts")],
        write_mode=WriteMode.UPSERT_NO_DELETE,
    )
    assert [c.name for c in t.primary_key_columns] == ["id"]
    assert [c.name for c in t.merge_key_columns] == ["id"]


def test_merge_key_columns_prefers_primary_key_over_unique():
    t = Target(
        name="test_table",
        database=make_db(),
        columns=[
            make_column("id", primary_key=True),
            make_column("alt", unique=True),
        ],
        write_mode=WriteMode.UPSERT_NO_DELETE,
    )
    assert [c.name for c in t.merge_key_columns] == ["id"]


def test_merge_key_columns_falls_back_to_unique():
    t = Target(
        name="test_table",
        database=make_db(),
        columns=[make_column("id", unique=True), make_column("v")],
        write_mode=WriteMode.UPSERT_NO_DELETE,
    )
    assert [c.name for c in t.merge_key_columns] == ["id"]


# --- Bounds ---


def test_bounds_stores_begin_end():
    begin = datetime(2024, 1, 1, tzinfo=UTC)
    end = datetime(2024, 12, 31, tzinfo=UTC)
    b = Bounds(begin=begin, end=end)
    assert b.begin == begin
    assert b.end == end


# --- Model defaults ---


def test_defaults():
    m = make_model()
    assert m.source is None
    assert m.target.name == "test_table"
    assert m.target.schema.resolved == ""
    assert m.target.database is None
    assert m.target.columns == []
    assert m.target.model_type == ModelType.TABLE
    assert m.target.write_mode == WriteMode.APPEND
    assert m.enabled is True
    assert m.debug is False
    # With no batching kwarg passed, batching stays None ("don't chunk, run once").
    assert m.batching is None
    assert m.target.sensitive is False
    assert m.target.unique_columns == []


# --- Tags ---


def test_name_added_to_tags_by_default():
    m = make_model()
    assert "test_table" in m.tags


def test_schema_added_to_tags_by_default():
    m = make_model(
        target=Target(name="test_table", schema=TargetSchema(name="my_schema"))
    )
    assert "my_schema" in m.tags


def test_all_tag_added_by_default():
    m = make_model()
    assert "all" in m.tags


def test_name_not_added_to_tags_when_disabled():
    m = make_model(tagging=Tags(name_add_to_tags=False))
    assert "test_table" not in m.tags


def test_schema_not_added_to_tags_when_disabled():
    m = make_model(
        target=Target(name="test_table", schema=TargetSchema(name="my_schema")),
        tagging=Tags(schema_add_to_tags=False),
    )
    assert "my_schema" not in m.tags


def test_all_tag_not_added_when_disabled():
    m = make_model(tagging=Tags(model_gets_all_tag=False))
    assert "all" not in m.tags


def test_unsnake_name_splits_on_underscore():
    m = make_model(target=Target(name="my_cool_table"))
    assert "my" in m.tags
    assert "cool" in m.tags
    assert "table" in m.tags


def test_unsnake_schema_splits_on_underscore():
    m = make_model(
        target=Target(name="test_table", schema=TargetSchema(name="my_schema"))
    )
    assert "my" in m.tags
    assert "schema" in m.tags


def test_unsnake_name_disabled():
    m = make_model(
        target=Target(name="my_cool_table"), tagging=Tags(unsnake_name_for_tags=False)
    )
    assert "my" not in m.tags
    assert "cool" not in m.tags


def test_unsnake_schema_disabled():
    m = make_model(
        target=Target(name="test_table", schema=TargetSchema(name="my_schema")),
        tagging=Tags(unsnake_schema_for_tags=False),
    )
    assert "my" not in m.tags


def test_unpascal_name_off_by_default():
    m = make_model(target=Target(name="MyTable"))
    # unpascal is off by default, so "my" and "table" should not be added
    # (unsnake splits on _, which won't split PascalCase)
    assert "my" not in m.tags
    assert "MyTable" in m.tags


def test_unpascal_name_splits_pascal_case():
    m = make_model(
        target=Target(name="MyCoolTable"), tagging=Tags(unpascal_name_for_tags=True)
    )
    assert "my" in m.tags
    assert "cool" in m.tags
    assert "table" in m.tags


def test_unpascal_schema_splits_pascal_case():
    m = make_model(
        target=Target(name="test_table", schema=TargetSchema(name="MySchema")),
        tagging=Tags(unpascal_schema_for_tags=True),
    )
    assert "my" in m.tags
    assert "schema" in m.tags


def test_unpascal_schema_off_by_default():
    m = make_model(
        target=Target(name="test_table", schema=TargetSchema(name="MySchema")),
    )
    assert "my" not in m.tags


def test_full_name_added_to_tags_by_default():
    m = make_model(
        target=Target(name="poop", schema=TargetSchema(name="cha_clean")),
    )
    assert "cha_clean.poop" in m.tags


def test_full_name_not_added_when_no_schema():
    m = make_model(target=Target(name="poop"))
    assert ".poop" not in m.tags


def test_full_name_not_added_when_disabled():
    m = make_model(
        target=Target(name="poop", schema=TargetSchema(name="cha_clean")),
        tagging=Tags(full_name_add_to_tags=False),
    )
    assert "cha_clean.poop" not in m.tags


# --- Tags: catalog ---


def test_catalog_added_to_tags_when_set():
    m = make_model(
        target=Target(
            name="poop", schema=TargetSchema(name="cha_clean"), catalog="prod_dwh"
        )
    )
    assert "prod_dwh" in m.tags


def test_catalog_not_added_when_none():
    m = make_model(target=Target(name="poop", schema=TargetSchema(name="cha_clean")))
    # None should not contaminate the tag set
    assert None not in m.tags
    assert "" not in m.tags


def test_catalog_not_added_when_disabled():
    m = make_model(
        target=Target(
            name="poop", schema=TargetSchema(name="cha_clean"), catalog="prod_dwh"
        ),
        tagging=Tags(catalog_add_to_tags=False),
    )
    assert "prod_dwh" not in m.tags


def test_fully_qualified_name_added_when_catalog_and_schema():
    m = make_model(
        target=Target(
            name="poop", schema=TargetSchema(name="cha_clean"), catalog="prod_dwh"
        )
    )
    assert "prod_dwh.cha_clean.poop" in m.tags


def test_fully_qualified_name_not_added_without_catalog():
    m = make_model(target=Target(name="poop", schema=TargetSchema(name="cha_clean")))
    # No `None.cha_clean.poop` or `.cha_clean.poop` leaks in
    assert not any("None" in t for t in m.tags)


def test_fully_qualified_name_disabled():
    m = make_model(
        target=Target(
            name="poop", schema=TargetSchema(name="cha_clean"), catalog="prod_dwh"
        ),
        tagging=Tags(fully_qualified_name_add_to_tags=False),
    )
    assert "prod_dwh.cha_clean.poop" not in m.tags
    # Catalog-as-single-tag still present (separate flag)
    assert "prod_dwh" in m.tags


def test_unsnake_catalog_splits_on_underscore():
    m = make_model(
        target=Target(
            name="poop", schema=TargetSchema(name="cha_clean"), catalog="prod_dwh"
        )
    )
    assert "prod" in m.tags
    assert "dwh" in m.tags


def test_unsnake_catalog_disabled():
    m = make_model(
        target=Target(
            name="poop", schema=TargetSchema(name="cha_clean"), catalog="prod_dwh"
        ),
        tagging=Tags(unsnake_catalog_for_tags=False),
    )
    assert "prod" not in m.tags


def test_unpascal_catalog_off_by_default():
    m = make_model(
        target=Target(name="poop", schema=TargetSchema(name="cha"), catalog="ProdDwh")
    )
    # unpascal off → "prod" not added; raw "ProdDwh" still is
    assert "prod" not in m.tags
    assert "ProdDwh" in m.tags


def test_unpascal_catalog_splits_pascal_case():
    m = make_model(
        target=Target(name="poop", schema=TargetSchema(name="cha"), catalog="ProdDwh"),
        tagging=Tags(unpascal_catalog_for_tags=True),
    )
    assert "prod" in m.tags
    assert "dwh" in m.tags


# --- Target.full_name with catalog ---


def test_full_name_includes_catalog_when_set():
    t = Target(name="poop", schema=TargetSchema(name="cha_clean"), catalog="prod_dwh")
    assert t.full_name == "prod_dwh.cha_clean.poop"


def test_full_name_skips_catalog_when_unset():
    t = Target(name="poop", schema=TargetSchema(name="cha_clean"))
    assert t.full_name == "cha_clean.poop"


# --- Target columns ---


def test_sensitive_flag_set_from_columns():
    t = Target(
        name="test_table",
        database=make_db(),
        columns=[make_column("id"), make_column("secret", sensitive=True)],
    )
    assert t.sensitive is True


def test_sensitive_false_when_no_sensitive_columns():
    t = Target(
        name="test_table",
        database=make_db(),
        columns=[make_column("id"), make_column("name")],
    )
    assert t.sensitive is False


def test_unique_columns_extracted():
    t = Target(
        name="test_table",
        database=make_db(),
        columns=[make_column("id", unique=True), make_column("name")],
    )
    assert len(t.unique_columns) == 1
    assert t.unique_columns[0].name == "id"


def test_upsert_no_delete_with_unique_column_ok():
    t = Target(
        name="test_table",
        database=make_db(),
        columns=[make_column("id", unique=True), make_column("name")],
        write_mode=WriteMode.UPSERT_NO_DELETE,
    )
    assert t.write_mode == WriteMode.UPSERT_NO_DELETE


# --- Column sorting ---


def test_column_sorting_applied():
    def custom_sort(names: list[str]) -> list[str]:
        return sorted(names, reverse=True)

    t = Target(
        name="test_table",
        database=make_db(),
        columns=[make_column("b"), make_column("a")],
        column_sorting=custom_sort,
    )
    assert [c.name for c in t.columns] == ["b", "a"]


def test_column_sorting_none_skips_sort():
    t = Target(
        name="test_table",
        database=make_db(),
        columns=[make_column("b"), make_column("a")],
        column_sorting=None,
    )
    assert [c.name for c in t.columns] == ["b", "a"]


# --- Partitioned by ---


def test_partition_on_sets_partitioned_by_and_index():
    t = Target(
        name="test_table",
        database=make_db(),
        columns=[make_column("id"), make_column("ts", partition_on=True)],
        column_sorting=None,
    )
    assert t.partitioned_by == "ts"
    assert t.partitioned_by_index is True


def test_no_partition_column_sets_index_false():
    t = Target(name="test_table")
    assert t.partitioned_by is None
    assert t.partitioned_by_index is False


# --- Extra kwargs ---


def test_extra_kwargs_stored():
    m = make_model(custom_key="custom_value")
    assert m.extra["custom_key"] == "custom_value"


# --- __repr__ ---


def test_repr_contains_name():
    m = make_model()
    assert "test_table" in repr(m)


# --- __eq__ ---


def test_eq_same_config():
    m1 = make_model()
    m2 = make_model()
    assert m1 == m2


def test_eq_different_config():
    m1 = make_model(target=Target(name="a"))
    m2 = make_model(target=Target(name="b"))
    assert m1 != m2


def test_eq_non_model():
    m = make_model()
    assert m.__eq__("not a model") == NotImplemented
