"""Tests for bollhav.postgres.staging — stage and atomic flush.

Postgres connection is mocked. Covers:
  * `stage()` happy path (DDL → COPY → flush tx)
  * exception path (no flush, marker not set)
  * preconditions (write mode, state, batching, run_id)
  * @state_tracker bypass after a flush
  * orphan staging-table GC

Real-DB exercise lives in the runnable example."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
from uuid import UUID

import polars as pl
import pytest


SINCE = datetime(2024, 1, 1, tzinfo=timezone.utc)
UNTIL = datetime(2024, 1, 2, tzinfo=timezone.utc)
RUN_ID = UUID("00000000-0000-0000-0000-00000000beef")


def _model(*, write_mode=None, staging_cfg=None):
    from bollhav.model.staging import Staging
    from bollhav.model.state import State
    from bollhav.model.write_modes import WriteMode
    from bollhav.postgres.columns import PostgresColumn, PostgresType

    model = MagicMock()
    model.state = State()
    model.batching = MagicMock()
    model.target.name = "orders"
    model.target.name_resolved = "orders"
    model.target.full_name = "public.orders"
    model.target.schema.resolved = "public"
    model.target.write_mode = write_mode or WriteMode.APPEND
    model.target.staging = staging_cfg if staging_cfg is not None else Staging()
    model.target.columns = [
        PostgresColumn(name="id", data_type=PostgresType.BIGINT),
        PostgresColumn(name="amount", data_type=PostgresType.NUMERIC),
    ]
    model._state_run_id = RUN_ID
    model._state_applied_via_staging = None
    return model


def _mock_conn():
    conn = MagicMock()
    conn.transaction.return_value.__enter__ = MagicMock(return_value=None)
    conn.transaction.return_value.__exit__ = MagicMock(return_value=None)

    cursor = MagicMock()
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=None)
    copy_ctx = MagicMock()
    copy_ctx.__enter__ = MagicMock(return_value=copy_ctx)
    copy_ctx.__exit__ = MagicMock(return_value=None)
    cursor.copy.return_value = copy_ctx
    conn.cursor.return_value = cursor
    return conn


# ── preconditions ────────────────────────────────────────────────────


class TestPreconditions:
    def test_rejects_non_append_write_mode(self) -> None:
        from bollhav.model.write_modes import WriteMode
        from bollhav.postgres.staging import stage

        model = _model(write_mode=WriteMode.UPSERT_NO_DELETE)
        with pytest.raises(NotImplementedError, match="WriteMode.APPEND only"):
            with stage(_mock_conn(), model, since=SINCE, until=UNTIL):
                pass

    def test_rejects_missing_state(self) -> None:
        from bollhav.postgres.staging import stage

        model = _model()
        model.state = None
        with pytest.raises(ValueError, match="model.state to be set"):
            with stage(_mock_conn(), model, since=SINCE, until=UNTIL):
                pass

    def test_rejects_missing_batching(self) -> None:
        from bollhav.postgres.staging import stage

        model = _model()
        model.batching = None
        with pytest.raises(ValueError, match="model.batching to be set"):
            with stage(_mock_conn(), model, since=SINCE, until=UNTIL):
                pass

    def test_rejects_missing_run_id(self) -> None:
        from bollhav.postgres.staging import stage

        model = _model()
        model._state_run_id = None
        with pytest.raises(ValueError, match="_state_run_id"):
            with stage(_mock_conn(), model, since=SINCE, until=UNTIL):
                pass

    def test_rejects_cross_db_state(self) -> None:
        """Staging needs the state row flip and the data move to share
        a transaction — that requires state and target in the same DB."""
        from bollhav.model.state import State
        from bollhav.postgres.staging import stage

        model = _model()
        model.state = State(dsn_env_var="STATE_DSN")
        with pytest.raises(NotImplementedError, match="share a DB"):
            with stage(_mock_conn(), model, since=SINCE, until=UNTIL):
                pass


# ── happy path ───────────────────────────────────────────────────────


class TestHappyPath:
    def test_creates_staging_table_on_entry(self) -> None:
        from bollhav.postgres.staging import stage

        conn = _mock_conn()
        with stage(conn, _model(), since=SINCE, until=UNTIL):
            pass

        ddl_calls = [
            call
            for call in conn.execute.call_args_list
            if "CREATE UNLOGGED TABLE" in str(call.args[0])
        ]
        assert len(ddl_calls) == 1

    def test_write_copies_each_chunk(self) -> None:
        from bollhav.postgres.staging import stage

        conn = _mock_conn()
        with stage(conn, _model(), since=SINCE, until=UNTIL) as s:
            s.write(pl.DataFrame({"id": [1, 2], "amount": [1.0, 2.0]}))
            s.write(pl.DataFrame({"id": [3], "amount": [3.0]}))

        assert s.rows_written == 3
        assert conn.cursor.return_value.copy.call_count == 2

    def test_empty_chunk_is_skipped(self) -> None:
        from bollhav.postgres.staging import stage

        conn = _mock_conn()
        with stage(conn, _model(), since=SINCE, until=UNTIL) as s:
            s.write(pl.DataFrame({"id": [], "amount": []}))

        assert s.rows_written == 0
        assert conn.cursor.return_value.copy.call_count == 0

    def test_flush_runs_three_statements(self) -> None:
        from bollhav.postgres.staging import stage

        conn = _mock_conn()
        with stage(conn, _model(), since=SINCE, until=UNTIL) as s:
            s.write(pl.DataFrame({"id": [1], "amount": [1.0]}))

        executed = [str(call.args[0]) for call in conn.execute.call_args_list]
        insert_select = [q for q in executed if "INSERT INTO" in q and "SELECT" in q]
        drop = [q for q in executed if "DROP TABLE" in q and "staging" in q]
        update_state = [
            q for q in executed if "UPDATE" in q and "status = 'applied'" in q
        ]
        assert len(insert_select) == 1
        assert len(drop) == 1
        assert len(update_state) == 1

    def test_flush_sets_state_applied_marker(self) -> None:
        from bollhav.postgres.staging import stage

        model = _model()
        with stage(_mock_conn(), model, since=SINCE, until=UNTIL) as s:
            s.write(pl.DataFrame({"id": [1], "amount": [1.0]}))

        assert model._state_applied_via_staging == (SINCE, UNTIL)


# ── exception path ───────────────────────────────────────────────────


class TestExceptionPath:
    def test_exception_skips_flush_and_marker(self) -> None:
        from bollhav.postgres.staging import stage

        model = _model()
        conn = _mock_conn()
        with pytest.raises(RuntimeError, match="boom"):
            with stage(conn, model, since=SINCE, until=UNTIL):
                raise RuntimeError("boom")

        executed = [str(call.args[0]) for call in conn.execute.call_args_list]
        flush_queries = [q for q in executed if "INSERT INTO" in q and "SELECT" in q]
        assert flush_queries == []
        assert model._state_applied_via_staging is None


# ── naming + GC ──────────────────────────────────────────────────────


class TestNaming:
    def test_staging_table_uses_short_run_id(self) -> None:
        from bollhav.postgres.staging import _staging_table

        assert _staging_table(_model(), RUN_ID) == "orders_staging_00000000"

    def test_staging_schema_is_z_prefixed(self) -> None:
        from bollhav.postgres.staging import _staging_schema

        assert _staging_schema(_model()) == "z_public"


class TestGc:
    def test_drops_orphans_keeps_current(self) -> None:
        from bollhav.postgres.staging import gc_orphan_staging_tables

        model = _model()
        current = f"orders_staging_{str(RUN_ID)[:8]}"
        rows = [
            ("orders_staging_aaaaaaaa",),
            ("orders_staging_bbbbbbbb",),
            (current,),
        ]
        conn = _mock_conn()
        conn.execute.return_value.fetchall.return_value = rows

        with patch("bollhav.postgres.state._connect") as connect:
            connect.return_value.__enter__ = MagicMock(return_value=conn)
            connect.return_value.__exit__ = MagicMock(return_value=None)
            gc_orphan_staging_tables(model, keep_run_id=RUN_ID)

        dropped = [
            str(call.args[0])
            for call in conn.execute.call_args_list
            if "DROP TABLE" in str(call.args[0])
        ]
        assert len(dropped) == 2

    def test_skipped_when_keep_after_flush(self) -> None:
        from bollhav.model.staging import Staging
        from bollhav.postgres.staging import gc_orphan_staging_tables

        model = _model(staging_cfg=Staging(keep_after_flush=True))
        conn = _mock_conn()

        with patch("bollhav.postgres.state._connect") as connect:
            connect.return_value.__enter__ = MagicMock(return_value=conn)
            connect.return_value.__exit__ = MagicMock(return_value=None)
            gc_orphan_staging_tables(model, keep_run_id=RUN_ID)

        # Never even queried pg_tables — no GC at all.
        conn.execute.assert_not_called()


# ── Staging fields ───────────────────────────────────────────────────


class TestStagingSchemaOverride:
    def test_default_uses_z_prefix(self) -> None:
        from bollhav.postgres.staging import _staging_schema

        assert _staging_schema(_model()) == "z_public"

    def test_override_takes_precedence(self) -> None:
        from bollhav.model.staging import Staging
        from bollhav.postgres.staging import _staging_schema

        model = _model(staging_cfg=Staging(schema="ops"))
        assert _staging_schema(model) == "ops"

    def test_override_propagates_to_ddl(self) -> None:
        from bollhav.model.staging import Staging
        from bollhav.postgres.staging import stage

        model = _model(staging_cfg=Staging(schema="ops"))
        conn = _mock_conn()
        with stage(conn, model, since=SINCE, until=UNTIL) as s:
            s.write(pl.DataFrame({"id": [1], "amount": [1.0]}))

        ddl = next(
            str(call.args[0])
            for call in conn.execute.call_args_list
            if "CREATE" in str(call.args[0]) and "TABLE" in str(call.args[0])
        )
        assert "Identifier('ops')" in ddl
        assert "z_public" not in ddl


class TestStagingTablePrefixOverride:
    def test_default_prefix(self) -> None:
        from bollhav.postgres.staging import _staging_table_prefix

        assert _staging_table_prefix(_model()) == "orders_staging_"

    def test_override_changes_table_name(self) -> None:
        from bollhav.model.staging import Staging
        from bollhav.postgres.staging import _staging_table

        model = _model(staging_cfg=Staging(table_prefix="bollhav_orders_stg_"))
        assert _staging_table(model, RUN_ID) == "bollhav_orders_stg_00000000"


class TestLoggedField:
    def test_default_is_unlogged(self) -> None:
        from bollhav.postgres.staging import stage

        conn = _mock_conn()
        with stage(conn, _model(), since=SINCE, until=UNTIL):
            pass

        ddl = next(
            str(call.args[0])
            for call in conn.execute.call_args_list
            if "CREATE" in str(call.args[0])
        )
        assert "UNLOGGED TABLE" in ddl

    def test_logged_true_omits_unlogged(self) -> None:
        from bollhav.model.staging import Staging
        from bollhav.postgres.staging import stage

        model = _model(staging_cfg=Staging(logged=True))
        conn = _mock_conn()
        with stage(conn, model, since=SINCE, until=UNTIL):
            pass

        ddl = next(
            str(call.args[0])
            for call in conn.execute.call_args_list
            if "CREATE" in str(call.args[0])
        )
        assert "UNLOGGED" not in ddl
        assert "CREATE TABLE" in ddl


class TestKeepAfterFlush:
    def test_default_drops_staging_on_flush(self) -> None:
        from bollhav.postgres.staging import stage

        conn = _mock_conn()
        with stage(conn, _model(), since=SINCE, until=UNTIL) as s:
            s.write(pl.DataFrame({"id": [1], "amount": [1.0]}))

        executed = [str(call.args[0]) for call in conn.execute.call_args_list]
        drops = [q for q in executed if "DROP TABLE" in q]
        assert len(drops) == 1  # the staging table

    def test_keep_after_flush_skips_drop(self) -> None:
        from bollhav.model.staging import Staging
        from bollhav.postgres.staging import stage

        model = _model(staging_cfg=Staging(keep_after_flush=True))
        conn = _mock_conn()
        with stage(conn, model, since=SINCE, until=UNTIL) as s:
            s.write(pl.DataFrame({"id": [1], "amount": [1.0]}))

        executed = [str(call.args[0]) for call in conn.execute.call_args_list]
        drops = [q for q in executed if "DROP TABLE" in q]
        assert drops == []
        # state row still gets flipped — that's the atomicity story
        assert any("status = 'applied'" in q for q in executed)
