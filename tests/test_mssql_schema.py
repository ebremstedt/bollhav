from unittest.mock import MagicMock
import sys

sys.modules.setdefault("pyodbc", MagicMock())

import pytest  # noqa: E402

from bollhav.model.database import Database  # noqa: E402
from bollhav.model.model import Model  # noqa: E402
from bollhav.model.schema import Schema  # noqa: E402
from bollhav.model.source import Source  # noqa: E402
from bollhav.model.target import Target  # noqa: E402
from bollhav.model.write_modes import WriteMode  # noqa: E402
from bollhav.mssql.columns import MssqlColumn, MssqlType  # noqa: E402
from bollhav.mssql.indexes import MssqlIndex  # noqa: E402
from bollhav.mssql.schema import (  # noqa: E402
    _index_ddl,
    ensure_indexes,
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
        source=Source(name="src"),
        target=Target(
            name="t",
            schema=Schema(name="s"),
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


class TestTargetIndexValidation:
    def test_unknown_column_raises(self) -> None:
        with pytest.raises(ValueError, match="references unknown column"):
            Target(
                name="t",
                schema=Schema(name="s"),
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
                schema=Schema(name="s"),
                database=Database.MSSQL,
                columns=[MssqlColumn(name="a", nullable=False)],
                indexes=[MssqlIndex(name="ix", columns=["a"], included=["missing"])],
                write_mode=WriteMode.APPEND,
                column_sorting=None,
            )

    def test_known_columns_ok(self) -> None:
        t = Target(
            name="t",
            schema=Schema(name="s"),
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
