"""Tests for the new MSSQL data backend (bollhav.mssql.data.MssqlData)
and the staged write path that pairs with it.

pyodbc is mocked at import. MssqlData mirrors PostgresData's method
surface so the one lifecycle hook drives both backends; these tests pin
the SQL each asset-DDL step issues, the delegation of the staging
lifecycle to the staging free functions, and the "no state with MSSQL"
guard.
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from bollhav.model.modelrun import ModelRun
from uuid import UUID

import polars as pl
import pytest

sys.modules.setdefault("pyodbc", MagicMock())

from bollhav.model.batch import Batch
from bollhav.model.database import Database
from bollhav.model.temporality import Temporality
from bollhav.model.intervals import TZInterval
from bollhav.model.staging import Staging
from bollhav.model.state import State
from bollhav.model.target import Target
from bollhav.model.write_modes import WriteMode
from bollhav.mssql.columns import MssqlColumn, MssqlType
from bollhav.mssql.data import MssqlData

RUN_ID = UUID("00000000-0000-0000-0000-00000000beef")
SINCE = datetime(2024, 1, 1, tzinfo=timezone.utc)
UNTIL = datetime(2024, 1, 2, tzinfo=timezone.utc)


def _model(
    *,
    staging=None,
    state=None,
    write_mode=WriteMode.APPEND,
    recreate_table=False,
    truncate_table=False,
    cols=None,
    temporality=Temporality.TEMPORAL,
    view=False,
):
    from bollhav.model.model import Model

    if cols is None:
        cols = [
            MssqlColumn(name="id", data_type=MssqlType.BIGINT, nullable=False),
            MssqlColumn(name="val", data_type=MssqlType.NVARCHAR, length=50),
        ]
    # A TIMELESS model (incl. a view) has no batching; a TEMPORAL one is batched.
    batching = None if temporality is Temporality.TIMELESS else Batch()
    return Model(
        target=Target(
            name="events",
            schema="warehouse",
            catalog="cat",
            database=Database.MSSQL,
            write_mode=write_mode,
            dsn_env_var="MSSQL_DSN",
            staging=staging,
            recreate_table=recreate_table,
            truncate_table=truncate_table,
            columns=cols,
        ),
        state=state,
        batching=batching,
        temporality=temporality,
        view=view,
    )


def _conn():
    """A pyodbc-like connection whose cursor records every execute()."""
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    return conn, cursor


def _sql(cursor) -> str:
    """All SQL passed to cursor.execute across calls, concatenated."""
    return "\n".join(str(c.args[0]) for c in cursor.execute.call_args_list)


class TestStateGuard:
    def test_state_on_mssql_model_is_allowed(self):
        # MSSQL data + Postgres state is supported: MssqlData is data-side
        # only and doesn't care about state (it runs in Postgres elsewhere).
        conn, _ = _conn()
        assert MssqlData(_model(state=State()), conn).model.state is not None

    def test_stateless_model_constructs_fine(self):
        conn, _ = _conn()
        assert MssqlData(_model(), conn).model.target.name == "events"


class TestMssqlPostgresStateWiring:
    """MSSQL data + Postgres state: state lives in Postgres on a separate
    connection. The lifecycle's `_conns` rejects a state-tracked MSSQL model
    that wasn't given a distinct state_conn (which would otherwise try to run
    Postgres state SQL on the MSSQL connection)."""

    def test_missing_state_conn_raises(self):
        from bollhav.model.lifecycle import _conns

        conn, _ = _conn()
        m = _model(state=State())
        with pytest.raises(ValueError, match="separate Postgres"):
            _conns({"data_conn": conn, "run": ModelRun(model=m)})

    def test_separate_state_conn_is_used(self):
        from bollhav.model.lifecycle import _conns

        data_conn, _ = _conn()
        state_conn = MagicMock()
        m = _model(state=State())
        dc, sc = _conns(
            {"data_conn": data_conn, "state_conn": state_conn, "run": ModelRun(model=m)}
        )
        assert dc is data_conn and sc is state_conn

    def test_stateless_mssql_needs_no_state_conn(self):
        from bollhav.model.lifecycle import _conns

        conn, _ = _conn()
        dc, sc = _conns({"data_conn": conn, "run": ModelRun(model=_model())})
        assert dc is conn and sc is conn


class TestAssetDDL:
    def test_create_schema(self):
        conn, cursor = _conn()
        MssqlData(_model(), conn).create_schema()
        assert "CREATE SCHEMA" in _sql(cursor)

    def test_create_table_is_guarded_and_creates(self):
        conn, cursor = _conn()
        MssqlData(_model(), conn).create_table()
        sql = _sql(cursor)
        assert "IF NOT EXISTS" in sql
        assert "CREATE TABLE" in sql
        assert "[warehouse].[events]" in sql

    def test_recreate_table_drops(self):
        conn, cursor = _conn()
        MssqlData(_model(), conn).recreate_table()
        assert "DROP TABLE" in _sql(cursor)

    def test_truncate_table(self):
        conn, cursor = _conn()
        MssqlData(_model(), conn).truncate_table()
        assert "TRUNCATE TABLE [warehouse].[events]" in _sql(cursor)

    def test_add_unique_constraint_when_unique_columns(self):
        conn, cursor = _conn()
        cols = [
            MssqlColumn(
                name="id", data_type=MssqlType.BIGINT, nullable=False, unique=True
            ),
            MssqlColumn(name="val", data_type=MssqlType.NVARCHAR, length=50),
        ]
        MssqlData(_model(cols=cols), conn).add_unique_constraint()
        sql = _sql(cursor)
        assert "ADD CONSTRAINT [events_uq] UNIQUE" in sql

    def test_add_unique_constraint_noop_without_unique_columns(self):
        conn, cursor = _conn()
        MssqlData(_model(), conn).add_unique_constraint()
        cursor.execute.assert_not_called()


class TestStagingLifecycleDelegation:
    """The staging methods are thin wrappers over the staging free
    functions; assert MssqlData routes to them with the right args."""

    def test_create_staging_schema(self):
        conn, _ = _conn()
        with patch("bollhav.mssql.data.ensure_staging_schema") as f:
            MssqlData(_model(staging=Staging()), conn).create_staging_schema()
        f.assert_called_once()

    def test_create_staging_table(self):
        conn, _ = _conn()
        m = _model(staging=Staging())
        with patch("bollhav.mssql.data.ensure_staging_table") as f:
            MssqlData(m, conn).create_staging_table(RUN_ID)
        f.assert_called_once_with(conn, m, RUN_ID)

    def test_write_to_staging(self):
        conn, _ = _conn()
        m = _model(staging=Staging())
        df = pl.DataFrame({"id": [1], "val": ["a"]})
        with patch("bollhav.mssql.data.write_to_staging") as f:
            MssqlData(m, conn).write_to_staging(RUN_ID, df)
        f.assert_called_once_with(conn, m, RUN_ID, df)

    def test_apply_unpacks_interval_into_since_until(self):
        conn, _ = _conn()
        m = _model(staging=Staging())
        with patch("bollhav.mssql.data.apply_atomically_to_target") as f:
            MssqlData(m, conn).apply_staging_to_target(
                RUN_ID, TZInterval(since=SINCE, until=UNTIL)
            )
        f.assert_called_once_with(conn, m, run_id=RUN_ID, since=SINCE, until=UNTIL)

    def test_drop_staging_table(self):
        conn, _ = _conn()
        m = _model(staging=Staging())
        with patch("bollhav.mssql.data.drop_staging_table") as f:
            MssqlData(m, conn).drop_staging_table(RUN_ID)
        f.assert_called_once_with(conn, m, RUN_ID)

    def test_drop_staging_table_skipped_when_keep_after_apply(self):
        conn, _ = _conn()
        m = _model(staging=Staging(keep_after_apply=True))
        with patch("bollhav.mssql.data.drop_staging_table") as f:
            MssqlData(m, conn).drop_staging_table(RUN_ID)
        f.assert_not_called()


class TestStagedWritePath:
    """mssql.write() in staged mode just lands chunks via MssqlData —
    the table lifecycle is owned by @execute_lifecycle, not write()."""

    def test_staged_write_lands_each_chunk_and_does_not_manage_table(self):
        from bollhav.mssql.write_modes import write

        conn, _ = _conn()
        m = _model(staging=Staging())
        frames = [
            pl.DataFrame({"id": [1], "val": ["a"]}),
            pl.DataFrame({"id": [2], "val": ["b"]}),
        ]

        with (
            patch("bollhav.mssql.data.write_to_staging") as land,
            patch("bollhav.mssql.data.ensure_staging_table") as create,
            patch("bollhav.mssql.data.apply_atomically_to_target") as apply,
            patch("bollhav.mssql.data.drop_staging_table") as drop,
        ):
            write(conn=conn, run=ModelRun(model=m), df_gen=iter(frames))

        assert land.call_count == 2  # one per chunk
        create.assert_not_called()  # lifecycle owns create
        apply.assert_not_called()  # lifecycle owns apply
        drop.assert_not_called()  # lifecycle owns drop

    def test_view_target_is_rejected(self):
        from bollhav.mssql.write_modes import write

        conn, _ = _conn()
        m = _model(temporality=Temporality.TIMELESS, view=True)
        with pytest.raises(ValueError, match="VIEW"):
            write(
                conn=conn,
                run=ModelRun(model=m),
                df_gen=iter([pl.DataFrame({"id": [1]})]),
            )
