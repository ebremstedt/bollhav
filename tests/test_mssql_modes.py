from unittest.mock import MagicMock
import sys

sys.modules.setdefault("pyodbc", MagicMock())

import polars as pl  # noqa: E402

from bollhav.model.database import Database  # noqa: E402
from bollhav.model.model import Model  # noqa: E402
from bollhav.model.target_schema import TargetSchema  # noqa: E402
from bollhav.model.source_table import SourceTable  # noqa: E402
from bollhav.model.target import Target  # noqa: E402
from bollhav.model.write_modes import WriteMode  # noqa: E402
from bollhav.mssql.columns import MssqlColumn, MssqlType  # noqa: E402
from bollhav.mssql.modes import append  # noqa: E402
from bollhav.mssql.schema import ensure_table  # noqa: E402


def _model(
    write_mode: WriteMode = WriteMode.APPEND,
    recreate_table: bool = False,
    truncate_table: bool = False,
) -> Model:
    return Model(
        source=SourceTable(name="src"),
        target=Target(
            name="t",
            schema=TargetSchema(name="s"),
            database=Database.MSSQL,
            columns=[
                MssqlColumn(name="id", data_type=MssqlType.INT, nullable=False),
                MssqlColumn(name="val", data_type=MssqlType.NVARCHAR, length=50),
            ],
            write_mode=write_mode,
            column_sorting=None,
            recreate_table=recreate_table,
            truncate_table=truncate_table,
        ),
    )


def _df() -> pl.DataFrame:
    return pl.DataFrame({"id": [1, 2], "val": ["a", "b"]})


class TestAppend:
    def test_does_not_truncate(self) -> None:
        conn = MagicMock()
        append(conn=conn, model=_model(), df=_df(), fast_executemany=False)
        joined = "\n".join(
            c.args[0] for c in conn.cursor.return_value.execute.call_args_list
        )
        assert "TRUNCATE" not in joined

    def test_issues_insert(self) -> None:
        conn = MagicMock()
        append(conn=conn, model=_model(), df=_df(), fast_executemany=False)
        conn.cursor.return_value.executemany.assert_called_once()
        insert_sql = conn.cursor.return_value.executemany.call_args.args[0]
        assert insert_sql.startswith("INSERT INTO [s].[t]")
        assert "([id], [val])" in insert_sql

    def test_commits(self) -> None:
        conn = MagicMock()
        append(conn=conn, model=_model(), df=_df(), fast_executemany=False)
        conn.cursor.return_value.commit.assert_called_once()


class TestEnsureTableWithTruncate:
    def test_truncate_runs_after_create(self) -> None:
        conn = MagicMock()
        ensure_table(conn=conn, model=_model(truncate_table=True))
        statements = [
            c.args[0] for c in conn.cursor.return_value.execute.call_args_list
        ]
        assert any("CREATE TABLE" in s for s in statements), statements
        assert any("TRUNCATE TABLE [s].[t]" in s for s in statements), statements


class TestEnsureTableWithRecreate:
    def test_drop_runs_before_create(self) -> None:
        conn = MagicMock()
        ensure_table(conn=conn, model=_model(recreate_table=True))
        statements = [
            c.args[0] for c in conn.cursor.return_value.execute.call_args_list
        ]
        assert any("DROP TABLE" in s for s in statements), statements
        assert any("CREATE TABLE" in s for s in statements), statements
