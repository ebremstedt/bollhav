import pytest

pytest.importorskip("pyiceberg", reason="needs the optional iceberg extra")
pytest.importorskip("sqlalchemy", reason="needs pyiceberg[sql-sqlite]")

from pyiceberg.catalog.sql import SqlCatalog  # noqa: E402
from pyiceberg.transforms import DayTransform, IdentityTransform  # noqa: E402
from bollhav.iceberg import IcebergColumn, IcebergType  # noqa: E402
from bollhav.iceberg.data import IcebergData  # noqa: E402
from bollhav.model import Database, Model, Target, Temporality, WriteMode  # noqa: E402


def make_catalog(*, tmp_path) -> SqlCatalog:
    return SqlCatalog(
        "test",
        uri=f"sqlite:///{tmp_path / 'catalog.db'}",
        warehouse=f"file://{tmp_path}",
    )


def make_model(*, columns) -> Model:
    return Model(
        target=Target(
            name="t",
            schema="ns",
            catalog="test",
            database=Database.ICEBERG,
            write_mode=WriteMode.APPEND,
            columns=columns,
        ),
        temporality=Temporality.TIMELESS,
    )


def create(*, tmp_path, columns):
    catalog = make_catalog(tmp_path=tmp_path)
    data = IcebergData(model=make_model(columns=columns), conn=catalog)
    data.create_schema()
    data.create_table()
    return catalog.load_table("ns.t")


def test_timestamptz_partition_on_becomes_day_partition(tmp_path):
    table = create(
        tmp_path=tmp_path,
        columns=[
            IcebergColumn(
                name="_data_modified",
                data_type=IcebergType.TIMESTAMPTZ,
                partition_on=True,
            ),
            IcebergColumn(name="payload", data_type=IcebergType.STRING),
        ],
    )
    (field,) = table.spec().fields
    assert field.name == "_data_modified_day"
    assert isinstance(field.transform, DayTransform)
    assert table.schema().find_field(field.source_id).name == "_data_modified"


def test_string_partition_on_becomes_identity_partition(tmp_path):
    table = create(
        tmp_path=tmp_path,
        columns=[
            IcebergColumn(
                name="hospital", data_type=IcebergType.STRING, partition_on=True
            ),
            IcebergColumn(name="payload", data_type=IcebergType.STRING),
        ],
    )
    (field,) = table.spec().fields
    assert field.name == "hospital"
    assert isinstance(field.transform, IdentityTransform)


def test_no_partition_on_is_unpartitioned(tmp_path):
    table = create(
        tmp_path=tmp_path,
        columns=[IcebergColumn(name="payload", data_type=IcebergType.STRING)],
    )
    assert table.spec().is_unpartitioned()
