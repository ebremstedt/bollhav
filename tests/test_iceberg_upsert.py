from datetime import datetime, timezone
import pytest

pytest.importorskip("pyiceberg", reason="needs the optional iceberg extra")
pytest.importorskip("sqlalchemy", reason="needs pyiceberg[sql-sqlite]")

import pyarrow as pa  # noqa: E402
from pyiceberg.catalog.sql import SqlCatalog  # noqa: E402
from bollhav.iceberg import IcebergColumn, IcebergType  # noqa: E402
from bollhav.iceberg.columns import IcebergPrimaryKeyNotNullableError  # noqa: E402
from bollhav.iceberg.data import IcebergData  # noqa: E402
from bollhav.iceberg.schema import iceberg_schema  # noqa: E402
from bollhav.iceberg.write_modes import write_dataframes  # noqa: E402
from bollhav.model import Database, Model, Target, Temporality, WriteMode  # noqa: E402

UTC = timezone.utc


def make_catalog(*, tmp_path) -> SqlCatalog:
    return SqlCatalog(
        "test",
        uri=f"sqlite:///{tmp_path / 'catalog.db'}",
        warehouse=f"file://{tmp_path}",
    )


def make_model(*, key_columns: list[str], partitioned: bool = False) -> Model:
    columns = [
        IcebergColumn(
            name="_data_modified",
            data_type=IcebergType.TIMESTAMPTZ,
            nullable=False,
            partition_on=partitioned,
        ),
        IcebergColumn(
            name="id",
            data_type=IcebergType.LONG,
            nullable=False,
            primary_key="id" in key_columns,
        ),
        IcebergColumn(
            name="version",
            data_type=IcebergType.INT,
            nullable=False,
            primary_key="version" in key_columns,
        ),
        IcebergColumn(name="name", data_type=IcebergType.STRING),
    ]
    return Model(
        target=Target(
            name="t",
            schema="ns",
            catalog="test",
            database=Database.ICEBERG,
            write_mode=WriteMode.UPSERT_NO_DELETE,
            columns=columns,
        ),
        temporality=Temporality.TIMELESS,
    )


def rows(
    *, model: Model, data: list[tuple[datetime, int, int, str | None]]
) -> pa.Table:
    modified, ids, versions, names = zip(*data)
    return pa.table(
        {
            "_data_modified": pa.array(modified, pa.timestamp("us", "UTC")),
            "id": pa.array(ids, pa.int64()),
            "version": pa.array(versions, pa.int32()),
            "name": pa.array(names, pa.string()),
        }
    )


def chunks(*, table: pa.Table):
    yield table


def test_primary_key_cannot_be_nullable():
    with pytest.raises(IcebergPrimaryKeyNotNullableError, match="cannot be nullable"):
        IcebergColumn(name="id", data_type=IcebergType.LONG, primary_key=True)


def test_upsert_target_builds_with_iceberg_primary_key():
    model = make_model(key_columns=["id"])
    assert [c.name for c in model.target.merge_key_columns] == ["id"]


def test_primary_keys_become_identifier_fields():
    schema = iceberg_schema(make_model(key_columns=["id", "version"]))
    assert sorted(schema.identifier_field_names()) == ["id", "version"]


def test_time_type_round_trips_to_a_table(tmp_path):
    model = Model(
        target=Target(
            name="t",
            schema="ns",
            catalog="test",
            database=Database.ICEBERG,
            write_mode=WriteMode.APPEND,
            columns=[IcebergColumn(name="at", data_type=IcebergType.TIME)],
        ),
        temporality=Temporality.TIMELESS,
    )
    catalog = make_catalog(tmp_path=tmp_path)
    data = IcebergData(model=model, conn=catalog)
    data.create_schema()
    data.create_table()
    assert (
        str(catalog.load_table("ns.t").schema().find_field("at").field_type) == "time"
    )


def test_upsert_updates_changed_rows_and_inserts_new_ones(tmp_path):
    model = make_model(key_columns=["id"])
    catalog = make_catalog(tmp_path=tmp_path)
    data = IcebergData(model=model, conn=catalog)
    data.create_schema()
    data.create_table()

    day1 = datetime(2026, 6, 1, 8, tzinfo=UTC)
    day2 = datetime(2026, 6, 2, 8, tzinfo=UTC)
    first = rows(
        model=model, data=[(day1, 1, 1, "a"), (day1, 2, 1, "b"), (day1, 3, 1, "c")]
    )
    write_dataframes(catalog=catalog, model=model, df_gen=chunks(table=first))

    second = rows(
        model=model,
        data=[(day2, 2, 2, "b changed"), (day1, 3, 1, "c"), (day2, 4, 1, "d")],
    )
    write_dataframes(catalog=catalog, model=model, df_gen=chunks(table=second))

    got = catalog.load_table("ns.t").scan().to_arrow().sort_by("id").to_pylist()
    assert [(r["id"], r["version"], r["name"]) for r in got] == [
        (1, 1, "a"),
        (2, 2, "b changed"),
        (3, 1, "c"),
        (4, 1, "d"),
    ]


def test_upsert_is_idempotent(tmp_path):
    model = make_model(key_columns=["id"])
    catalog = make_catalog(tmp_path=tmp_path)
    data = IcebergData(model=model, conn=catalog)
    data.create_schema()
    data.create_table()

    day1 = datetime(2026, 6, 1, 8, tzinfo=UTC)
    batch = rows(model=model, data=[(day1, 1, 1, "a"), (day1, 2, 1, "b")])
    write_dataframes(catalog=catalog, model=model, df_gen=chunks(table=batch))
    write_dataframes(catalog=catalog, model=model, df_gen=chunks(table=batch))

    assert catalog.load_table("ns.t").scan().to_arrow().num_rows == 2


def test_upsert_on_a_composite_key(tmp_path):
    model = make_model(key_columns=["id", "version"])
    catalog = make_catalog(tmp_path=tmp_path)
    data = IcebergData(model=model, conn=catalog)
    data.create_schema()
    data.create_table()

    day1 = datetime(2026, 6, 1, 8, tzinfo=UTC)
    first = rows(model=model, data=[(day1, 1, 1, "v1")])
    second = rows(model=model, data=[(day1, 1, 1, "v1 fixed"), (day1, 1, 2, "v2")])
    write_dataframes(catalog=catalog, model=model, df_gen=chunks(table=first))
    write_dataframes(catalog=catalog, model=model, df_gen=chunks(table=second))

    got = catalog.load_table("ns.t").scan().to_arrow().sort_by("version").to_pylist()
    assert [(r["id"], r["version"], r["name"]) for r in got] == [
        (1, 1, "v1 fixed"),
        (1, 2, "v2"),
    ]


def test_upsert_into_a_day_partitioned_table(tmp_path):
    pytest.importorskip(
        "pyiceberg_core", reason="partitioned writes need pyiceberg-core"
    )
    model = make_model(key_columns=["id"], partitioned=True)
    catalog = make_catalog(tmp_path=tmp_path)
    data = IcebergData(model=model, conn=catalog)
    data.create_schema()
    data.create_table()

    day1 = datetime(2026, 6, 1, 8, tzinfo=UTC)
    day2 = datetime(2026, 6, 2, 8, tzinfo=UTC)
    first = rows(model=model, data=[(day1, 1, 1, "a"), (day1, 2, 1, "b")])
    second = rows(model=model, data=[(day2, 2, 2, "b moved a day"), (day2, 3, 1, "c")])
    write_dataframes(catalog=catalog, model=model, df_gen=chunks(table=first))
    write_dataframes(catalog=catalog, model=model, df_gen=chunks(table=second))

    got = catalog.load_table("ns.t").scan().to_arrow().sort_by("id").to_pylist()
    assert [(r["id"], r["name"]) for r in got] == [
        (1, "a"),
        (2, "b moved a day"),
        (3, "c"),
    ]


def test_composite_upsert_takes_a_full_chunk(tmp_path):
    """pyiceberg's own upsert segfaults on a composite key at this size."""
    model = make_model(key_columns=["id", "version"])
    catalog = make_catalog(tmp_path=tmp_path)
    data = IcebergData(model=model, conn=catalog)
    data.create_schema()
    data.create_table()

    day1 = datetime(2026, 6, 1, 8, tzinfo=UTC)
    day2 = datetime(2026, 6, 2, 8, tzinfo=UTC)
    first = rows(model=model, data=[(day1, i // 4, i % 4, "a") for i in range(20_000)])
    second = rows(
        model=model,
        data=[(day2, i // 4, i % 4, "b") for i in range(10_000, 30_000)],
    )
    write_dataframes(catalog=catalog, model=model, df_gen=chunks(table=first))
    write_dataframes(catalog=catalog, model=model, df_gen=chunks(table=second))

    got = catalog.load_table("ns.t").scan().to_arrow()
    assert got.num_rows == 30_000
    names = got.group_by("name").aggregate([([], "count_all")]).to_pylist()
    assert sorted((r["name"], r["count_all"]) for r in names) == [
        ("a", 10_000),
        ("b", 20_000),
    ]


def test_composite_upsert_leaves_rows_that_share_only_part_of_the_key(tmp_path):
    model = make_model(key_columns=["id", "version"])
    catalog = make_catalog(tmp_path=tmp_path)
    data = IcebergData(model=model, conn=catalog)
    data.create_schema()
    data.create_table()

    day1 = datetime(2026, 6, 1, 8, tzinfo=UTC)
    first = rows(
        model=model,
        data=[(day1, 1, 1, "1-1"), (day1, 1, 2, "1-2"), (day1, 2, 1, "2-1")],
    )
    # (1, 2) and (2, 1) pair an incoming id with an incoming version, but
    # neither is an incoming key
    second = rows(model=model, data=[(day1, 1, 1, "1-1 new"), (day1, 2, 2, "2-2")])
    write_dataframes(catalog=catalog, model=model, df_gen=chunks(table=first))
    write_dataframes(catalog=catalog, model=model, df_gen=chunks(table=second))

    got = catalog.load_table("ns.t").scan().to_arrow().to_pylist()
    assert sorted((r["id"], r["version"], r["name"]) for r in got) == [
        (1, 1, "1-1 new"),
        (1, 2, "1-2"),
        (2, 1, "2-1"),
        (2, 2, "2-2"),
    ]


def test_composite_upsert_is_one_commit(tmp_path):
    model = make_model(key_columns=["id", "version"])
    catalog = make_catalog(tmp_path=tmp_path)
    data = IcebergData(model=model, conn=catalog)
    data.create_schema()
    data.create_table()

    day1 = datetime(2026, 6, 1, 8, tzinfo=UTC)
    first = rows(model=model, data=[(day1, 1, 1, "a"), (day1, 1, 2, "b")])
    second = rows(model=model, data=[(day1, 1, 1, "a changed"), (day1, 1, 3, "c")])
    write_dataframes(catalog=catalog, model=model, df_gen=chunks(table=first))
    commits = len(catalog.load_table("ns.t").metadata.metadata_log)
    write_dataframes(catalog=catalog, model=model, df_gen=chunks(table=second))

    # the delete and the append land in one commit, so a reader never sees
    # the matched rows missing
    table = catalog.load_table("ns.t")
    assert len(table.metadata.metadata_log) == commits + 1
    assert table.scan().to_arrow().num_rows == 3


def test_composite_upsert_rejects_duplicate_keys(tmp_path):
    from bollhav.iceberg.modes import UpsertDuplicateKeysError

    model = make_model(key_columns=["id", "version"])
    catalog = make_catalog(tmp_path=tmp_path)
    data = IcebergData(model=model, conn=catalog)
    data.create_schema()
    data.create_table()

    day1 = datetime(2026, 6, 1, 8, tzinfo=UTC)
    twice = rows(model=model, data=[(day1, 1, 1, "a"), (day1, 1, 1, "b")])
    with pytest.raises(UpsertDuplicateKeysError, match="duplicate rows"):
        write_dataframes(catalog=catalog, model=model, df_gen=chunks(table=twice))


def test_composite_upsert_into_a_day_partitioned_table(tmp_path):
    pytest.importorskip(
        "pyiceberg_core", reason="partitioned writes need pyiceberg-core"
    )
    model = make_model(key_columns=["id", "version"], partitioned=True)
    catalog = make_catalog(tmp_path=tmp_path)
    data = IcebergData(model=model, conn=catalog)
    data.create_schema()
    data.create_table()

    day1 = datetime(2026, 6, 1, 8, tzinfo=UTC)
    day2 = datetime(2026, 6, 2, 8, tzinfo=UTC)
    first = rows(model=model, data=[(day1, 1, 1, "a"), (day1, 2, 1, "b")])
    second = rows(model=model, data=[(day2, 2, 1, "b moved a day"), (day2, 3, 1, "c")])
    write_dataframes(catalog=catalog, model=model, df_gen=chunks(table=first))
    write_dataframes(catalog=catalog, model=model, df_gen=chunks(table=second))

    got = catalog.load_table("ns.t").scan().to_arrow().sort_by("id").to_pylist()
    assert [(r["id"], r["name"]) for r in got] == [
        (1, "a"),
        (2, "b moved a day"),
        (3, "c"),
    ]


def test_descriptions_become_table_comment_and_column_docs(tmp_path):
    model = Model(
        description="Health issues",
        target=Target(
            name="t",
            schema="ns",
            catalog="test",
            database=Database.ICEBERG,
            columns=[
                IcebergColumn(
                    name="id", data_type=IcebergType.LONG, description="The id"
                ),
                IcebergColumn(name="name", data_type=IcebergType.STRING),
            ],
        ),
        temporality=Temporality.TIMELESS,
    )
    catalog = make_catalog(tmp_path=tmp_path)
    data = IcebergData(model=model, conn=catalog)
    data.create_schema()
    data.create_table()

    table = catalog.load_table("ns.t")
    assert table.properties["comment"] == "Health issues"
    assert table.schema().find_field("id").doc == "The id"
    assert table.schema().find_field("name").doc is None
