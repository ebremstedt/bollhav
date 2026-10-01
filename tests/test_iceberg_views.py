import pytest

pytest.importorskip("pyiceberg", reason="needs the optional iceberg extra")
pytest.importorskip("sqlalchemy", reason="needs pyiceberg[sql-sqlite]")

from pyiceberg.catalog.sql import SqlCatalog  # noqa: E402
from bollhav.iceberg import IcebergColumn, IcebergType  # noqa: E402
from bollhav.iceberg.data import (  # noqa: E402
    IcebergData,
    IcebergWriterConnectionError,
)
from bollhav.model import (  # noqa: E402
    Database,
    Materialization,
    Model,
    ModelRun,
    Source,
    SourceModel,
    Target,
    Temporality,
    Writer,
)
from bollhav.model.model import (  # noqa: E402
    IcebergViewRequiresTrinoWriterError,
    TrinoWritesViewsOnlyError,
)
from bollhav.model.runtime import _target_with_suffix  # noqa: E402
from bollhav.model.target import (  # noqa: E402
    DatabaseWithoutColumnsError,
    WriterRequiresIcebergError,
)


class FakeCursor:
    def __init__(self, statements: list[str]) -> None:
        self.statements = statements

    def execute(self, statement: str) -> None:
        self.statements.append(statement)

    def fetchall(self) -> list:
        return []

    def close(self) -> None:
        pass


class FakeTrino:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def cursor(self) -> FakeCursor:
        return FakeCursor(self.statements)


def make_view(
    *,
    description: str | None = "Contacts, Active = 1",
    writer: Writer | None = Writer.TRINO,
) -> Model:
    return Model(
        description=description,
        target=Target(
            name="f_contact_latest",
            schema="cint_clean_rst",
            catalog="lake",
            database=Database.ICEBERG,
            writer=writer,
        ),
        materialization=Materialization.VIEW,
        query_builder="SELECT * FROM lake.cint_clean_rst.f_contact_original WHERE Active = 1",
        upstream=[
            Source(
                name="lake.cint_clean_rst.f_contact_original",
                type=SourceModel(catalog="lake", schema="cint_clean_rst"),
            )
        ],
        temporality=Temporality.TIMELESS,
    )


def test_a_view_declares_no_columns():
    assert make_view().target.columns == []


def test_a_table_still_needs_columns():
    with pytest.raises(DatabaseWithoutColumnsError):
        Model(
            target=Target(
                name="t", schema="ns", catalog="lake", database=Database.ICEBERG
            ),
            temporality=Temporality.TIMELESS,
        )


def test_view_is_created_through_trino_with_its_comment():
    trino = FakeTrino()
    model = make_view()
    data = IcebergData(model=model, conn=trino)
    data.create_schema()
    data.create_or_replace_view(ModelRun(model=model).resolve_query())
    assert trino.statements == [
        'CREATE SCHEMA IF NOT EXISTS "lake"."cint_clean_rst"',
        'CREATE OR REPLACE VIEW "lake"."cint_clean_rst"."f_contact_latest" '
        "COMMENT 'Contacts, Active = 1' AS "
        "SELECT * FROM lake.cint_clean_rst.f_contact_original WHERE Active = 1",
    ]


def test_view_comment_quotes_are_doubled_and_absent_without_description():
    trino = FakeTrino()
    model = make_view(description="Patient's contacts")
    IcebergData(model=model, conn=trino).create_or_replace_view("SELECT 1")
    assert "COMMENT 'Patient''s contacts'" in trino.statements[0]

    trino = FakeTrino()
    IcebergData(model=make_view(description=None), conn=trino).create_or_replace_view(
        "SELECT 1"
    )
    assert "COMMENT" not in trino.statements[0]


def make_catalog(*, tmp_path) -> SqlCatalog:
    return SqlCatalog(
        "test",
        uri=f"sqlite:///{tmp_path / 'catalog.db'}",
        warehouse=f"file://{tmp_path}",
    )


def test_an_iceberg_target_writes_with_pyiceberg_unless_told_otherwise():
    table = Target(
        name="t",
        schema="ns",
        catalog="lake",
        database=Database.ICEBERG,
        columns=[IcebergColumn(name="id", data_type=IcebergType.LONG)],
    )
    assert table.writer is Writer.PYICEBERG
    assert make_view().target.writer is Writer.TRINO


def test_writer_is_only_for_iceberg():
    with pytest.raises(WriterRequiresIcebergError, match="only set for"):
        Target(name="t", schema="ns", catalog="lake", writer=Writer.TRINO)


def test_trino_writes_views_only():
    with pytest.raises(TrinoWritesViewsOnlyError, match="is for views"):
        Model(
            target=Target(
                name="t",
                schema="ns",
                catalog="lake",
                database=Database.ICEBERG,
                writer=Writer.TRINO,
                columns=[IcebergColumn(name="id", data_type=IcebergType.LONG)],
            ),
            temporality=Temporality.TIMELESS,
        )


def test_an_iceberg_view_must_declare_trino_as_its_writer():
    with pytest.raises(
        IcebergViewRequiresTrinoWriterError, match="writer=Writer.TRINO"
    ):
        make_view(writer=None)


def test_a_run_keeps_every_target_field():
    """_target_with_suffix rebuilds the Target per run, field by field; a field
    added to Target and forgotten there would silently fall back to its
    default, as `writer` once did."""
    from dataclasses import fields

    target = make_view().target
    rebuilt = _target_with_suffix(target, schema_suffix="dev")
    for field in fields(Target):
        if field.init and field.name not in ("schema_suffix",):
            assert getattr(rebuilt, field.name) == getattr(target, field.name), (
                field.name
            )
    assert rebuilt.writer is Writer.TRINO


def test_the_connection_must_match_the_writer(tmp_path):
    with pytest.raises(IcebergWriterConnectionError, match="needs a Trino"):
        IcebergData(model=make_view(), conn=make_catalog(tmp_path=tmp_path))
    table = Model(
        target=Target(
            name="t",
            schema="ns",
            catalog="lake",
            database=Database.ICEBERG,
            columns=[IcebergColumn(name="id", data_type=IcebergType.LONG)],
        ),
        temporality=Temporality.TIMELESS,
    )
    with pytest.raises(IcebergWriterConnectionError, match="needs the pyiceberg"):
        IcebergData(model=table, conn=FakeTrino())
