from unittest.mock import MagicMock
import sys

sys.modules.setdefault("pyodbc", MagicMock())

import pytest  # noqa: E402

from bollhav.model.database import Database  # noqa: E402
from bollhav.model.model import Model  # noqa: E402
from bollhav.model.target_schema import TargetSchema  # noqa: E402
from bollhav.model.source_table import SourceTable  # noqa: E402
from bollhav.model.target import Target  # noqa: E402
from bollhav.model.write_modes import WriteMode  # noqa: E402
from bollhav.mssql.columns import MssqlColumn, MssqlType  # noqa: E402
from bollhav.mssql.indexes import MssqlIndex  # noqa: E402
from bollhav.mssql.schema import (  # noqa: E402
    _index_ddl,
    ensure_indexes,
    ensure_primary_key,
    ensure_schema_table_and_indexes,
    ensure_table,
)


def _model(
    columns: list[MssqlColumn] | None = None,
    indexes: list[MssqlIndex] | None = None,
) -> Model:
    cols = columns or [
        MssqlColumn(name="id", data_type=MssqlType.INT, nullable=False),
        MssqlColumn(name="val", data_type=MssqlType.NVARCHAR, length=50),
    ]
    return Model(
        source=SourceTable(name="src"),
        target=Target(
            name="t",
            schema=TargetSchema(name="s"),
            database=Database.MSSQL,
            columns=cols,
            indexes=indexes or [],
            write_mode=WriteMode.APPEND,
            column_sorting=None,
        ),
    )


def _conn() -> MagicMock:
    return MagicMock()


class TestMssqlIndexConstruction:
    def test_basic(self) -> None:
        idx = MssqlIndex(name="ix", columns=["a"])
        assert idx.name == "ix"
        assert idx.columns == ["a"]
        assert idx.unique is False
        assert idx.filter is None
        assert idx.included == []

    def test_empty_columns_raises(self) -> None:
        with pytest.raises(ValueError, match="columns must be non-empty"):
            MssqlIndex(name="ix", columns=[])

    def test_columns_included_overlap_raises(self) -> None:
        with pytest.raises(ValueError, match="columns and included must be disjoint"):
            MssqlIndex(name="ix", columns=["a"], included=["a"])


class TestIndexDdl:
    def test_basic(self) -> None:
        ddl = _index_ddl("s", "t", MssqlIndex(name="ix", columns=["a"]))
        assert "CREATE NONCLUSTERED INDEX [ix] ON [s].[t] ([a])" in ddl
        assert "UNIQUE" not in ddl
        assert "INCLUDE" not in ddl
        assert "WHERE" not in ddl

    def test_unique(self) -> None:
        ddl = _index_ddl("s", "t", MssqlIndex(name="ix", columns=["a"], unique=True))
        assert "CREATE UNIQUE NONCLUSTERED INDEX" in ddl

    def test_multi_column(self) -> None:
        ddl = _index_ddl("s", "t", MssqlIndex(name="ix", columns=["a", "b"]))
        assert "([a], [b])" in ddl

    def test_filter(self) -> None:
        ddl = _index_ddl("s", "t", MssqlIndex(name="ix", columns=["a"], filter="b = 1"))
        assert ddl.endswith(" WHERE b = 1")

    def test_included(self) -> None:
        ddl = _index_ddl(
            "s", "t", MssqlIndex(name="ix", columns=["a"], included=["b", "c"])
        )
        assert "INCLUDE ([b], [c])" in ddl

    def test_filter_after_include(self) -> None:
        ddl = _index_ddl(
            "s",
            "t",
            MssqlIndex(name="ix", columns=["a"], included=["b"], filter="c = 1"),
        )
        assert ddl.index("INCLUDE") < ddl.index("WHERE")


class TestEnsureTable:
    def test_does_not_create_indexes(self) -> None:
        conn = _conn()
        model = _model(indexes=[MssqlIndex(name="ix_val", columns=["val"])])
        ensure_table(conn=conn, model=model)
        calls = conn.cursor.return_value.execute.call_args_list
        joined = "\n".join(c.args[0] for c in calls)
        assert "CREATE TABLE" in joined
        assert "CREATE NONCLUSTERED INDEX" not in joined


class TestEnsureIndexes:
    def test_no_indexes_is_noop(self) -> None:
        conn = _conn()
        ensure_indexes(conn=conn, model=_model())
        conn.cursor.assert_not_called()

    def test_single_index(self) -> None:
        conn = _conn()
        model = _model(indexes=[MssqlIndex(name="ix_val", columns=["val"])])
        ensure_indexes(conn=conn, model=model)
        assert conn.cursor.return_value.execute.call_count == 1

    def test_multiple_indexes(self) -> None:
        conn = _conn()
        model = _model(
            indexes=[
                MssqlIndex(name="ix_a", columns=["id"]),
                MssqlIndex(name="ix_b", columns=["val"]),
            ]
        )
        ensure_indexes(conn=conn, model=model)
        assert conn.cursor.return_value.execute.call_count == 2

    def test_filtered_index_ddl_passed_to_cursor(self) -> None:
        conn = _conn()
        model = _model(
            indexes=[MssqlIndex(name="ix_filtered", columns=["id"], filter="val = 'x'")]
        )
        ensure_indexes(conn=conn, model=model)
        call = conn.cursor.return_value.execute.call_args_list[0]
        sql = call.args[0]
        assert "CREATE NONCLUSTERED INDEX [ix_filtered]" in sql
        assert "WHERE val = 'x'" in sql
        assert "IF NOT EXISTS" in sql


class TestEnsureSchemaTableAndIndexes:
    def test_runs_all_three_steps(self) -> None:
        conn = _conn()
        model = _model(indexes=[MssqlIndex(name="ix_val", columns=["val"])])
        ensure_schema_table_and_indexes(conn=conn, model=model)
        joined = "\n".join(
            c.args[0] for c in conn.cursor.return_value.execute.call_args_list
        )
        assert "CREATE SCHEMA" in joined
        assert "CREATE TABLE" in joined
        assert "CREATE NONCLUSTERED INDEX" in joined


class TestEnsurePrimaryKey:
    def test_no_pk_columns_is_noop(self) -> None:
        conn = _conn()
        ensure_primary_key(conn=conn, model=_model())
        conn.cursor.assert_not_called()

    def test_pk_emits_clustered_alter(self) -> None:
        conn = _conn()
        model = _model(
            columns=[
                MssqlColumn(
                    name="id", data_type=MssqlType.INT, nullable=False, primary_key=True
                ),
                MssqlColumn(name="val", data_type=MssqlType.NVARCHAR, length=50),
            ]
        )
        ensure_primary_key(conn=conn, model=model)
        sql = conn.cursor.return_value.execute.call_args_list[0].args[0]
        assert "PRIMARY KEY CLUSTERED ([id])" in sql
        assert "[t_pk]" in sql
        assert "IF NOT EXISTS" in sql

    def test_composite_pk(self) -> None:
        conn = _conn()
        model = _model(
            columns=[
                MssqlColumn(
                    name="a", data_type=MssqlType.INT, nullable=False, primary_key=True
                ),
                MssqlColumn(
                    name="b", data_type=MssqlType.INT, nullable=False, primary_key=True
                ),
            ]
        )
        ensure_primary_key(conn=conn, model=model)
        sql = conn.cursor.return_value.execute.call_args_list[0].args[0]
        assert "PRIMARY KEY CLUSTERED ([a], [b])" in sql


class TestEnsureTablePkAndUniqueRedundancy:
    def test_create_table_does_not_emit_inline_pk(self) -> None:
        conn = _conn()
        model = _model(
            columns=[
                MssqlColumn(
                    name="id", data_type=MssqlType.INT, nullable=False, primary_key=True
                ),
                MssqlColumn(name="val", data_type=MssqlType.NVARCHAR, length=50),
            ]
        )
        ensure_table(conn=conn, model=model)
        joined = "\n".join(
            c.args[0] for c in conn.cursor.return_value.execute.call_args_list
        )
        # PK is created via ensure_primary_key (separate ALTER), not in CREATE TABLE.
        assert "CREATE TABLE" in joined
        assert "PRIMARY KEY" not in joined

    def test_unique_skipped_when_same_column_is_primary_key(self) -> None:
        conn = _conn()
        model = _model(
            columns=[
                MssqlColumn(
                    name="id",
                    data_type=MssqlType.INT,
                    nullable=False,
                    primary_key=True,
                    unique=True,
                ),
                MssqlColumn(name="val", data_type=MssqlType.NVARCHAR, length=50),
            ]
        )
        ensure_table(conn=conn, model=model)
        joined = "\n".join(
            c.args[0] for c in conn.cursor.return_value.execute.call_args_list
        )
        # No redundant UQ constraint when the only unique col is also the PK.
        assert "CONSTRAINT [t_uq] UNIQUE" not in joined

    def test_unique_kept_for_columns_that_are_not_primary_key(self) -> None:
        conn = _conn()
        model = _model(
            columns=[
                MssqlColumn(
                    name="id", data_type=MssqlType.INT, nullable=False, primary_key=True
                ),
                MssqlColumn(
                    name="alt", data_type=MssqlType.NVARCHAR, length=50, unique=True
                ),
            ]
        )
        ensure_table(conn=conn, model=model)
        joined = "\n".join(
            c.args[0] for c in conn.cursor.return_value.execute.call_args_list
        )
        # `alt` is unique-only, so the UQ constraint stays — but only on `alt`.
        assert "CONSTRAINT [t_uq] UNIQUE ([alt])" in joined


class TestTargetIndexValidation:
    def test_unknown_column_raises(self) -> None:
        with pytest.raises(ValueError, match="references unknown column"):
            Target(
                name="t",
                schema=TargetSchema(name="s"),
                database=Database.MSSQL,
                columns=[MssqlColumn(name="a", nullable=False)],
                indexes=[MssqlIndex(name="ix", columns=["missing"])],
                write_mode=WriteMode.APPEND,
                column_sorting=None,
            )

    def test_unknown_included_column_raises(self) -> None:
        with pytest.raises(ValueError, match="references unknown column"):
            Target(
                name="t",
                schema=TargetSchema(name="s"),
                database=Database.MSSQL,
                columns=[MssqlColumn(name="a", nullable=False)],
                indexes=[MssqlIndex(name="ix", columns=["a"], included=["missing"])],
                write_mode=WriteMode.APPEND,
                column_sorting=None,
            )

    def test_known_columns_ok(self) -> None:
        t = Target(
            name="t",
            schema=TargetSchema(name="s"),
            database=Database.MSSQL,
            columns=[
                MssqlColumn(name="a", nullable=False),
                MssqlColumn(name="b"),
            ],
            indexes=[MssqlIndex(name="ix", columns=["a"], included=["b"])],
            write_mode=WriteMode.APPEND,
            column_sorting=None,
        )
        assert len(t.indexes) == 1
