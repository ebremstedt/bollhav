"""Tests for bollhav.postgres.staging — stage and atomic flush.

Postgres connection is mocked. Covers:
  * `stage()` happy path (DDL → COPY → flush tx)
  * exception path (no flush, marker not set)
  * preconditions (write mode, state, batching, run_id)
  * @state bypass after a flush
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
    # (removed) Mutations replaced by Actions system
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
    # Action runtime state — `_applied` empty so PRE actions fire,
    # `actions=None` so the runner resolves defaults, `setup_complete`
    # explicitly False so the runner doesn't short-circuit.
    from bollhav.postgres.actions import default_actions as _da

    model.target._applied_model_actions = {}
    model.target.actions = []
    model.target.default_actions = _da()
    # `effective_actions` is a property on real Target; MagicMock
    # doesn't compute it, so pin it to defaults+user explicitly.
    model.target.effective_actions = list(model.target.default_actions)
    model.target.setup_complete = False
    # Directive flags pinned so should_run gates evaluate predictably.
    # A MagicMock attribute is truthy by default, which would make
    # every PRE action's should_run() pass and break tests.
    model.target.recreate_table = False
    model.target.truncate_table = False
    model.target.partitioned_by = None
    model.target.unique_columns = []
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

    def test_pre_actions_are_one_shot(self) -> None:
        """`run_pre_model_actions` runs the full PRE action list on the
        first call, records each in `target._applied_model_actions`, and is a
        complete no-op on every subsequent call within the same
        pipeline run because `setup_complete` short-circuits."""
        from bollhav.postgres.actions import default_actions, run_pre_model_actions
        from bollhav.model.actions import Phase

        model = _model()
        model.target._applied_model_actions = {}
        model.target.actions = []
        model.target.default_actions = default_actions()
        model.target.effective_actions = list(model.target.default_actions)

        # MagicMock doesn't compute properties, so emulate
        # `setup_complete` against the live `_applied` dict.
        def _live_setup_complete() -> bool:
            for a in model.target.effective_actions:
                if a.phase is not Phase.PRE_MODEL:
                    continue
                if not a.should_run(model.target):
                    continue
                if not model.target._applied_model_actions.get(a.name):
                    return False
            return True

        type(model.target).setup_complete = property(
            lambda _self: _live_setup_complete()
        )

        conn = _mock_conn()

        run_pre_model_actions(conn, model)
        assert model.target._applied_model_actions.get("staging_schema_created") is True
        assert model.target._applied_model_actions.get("staging_table_created") is True
        first_call_count = conn.execute.call_count
        assert first_call_count >= 1

        run_pre_model_actions(conn, model)
        run_pre_model_actions(conn, model)
        # Two more calls, zero new DDL — setup_complete short-circuits.
        assert conn.execute.call_count == first_call_count

    def test_accepts_missing_state(self) -> None:
        """Staging without state is a supported configuration —
        memory bounding + atomic per-interval finalization remain
        useful even without resumability. The flush still issues
        the atomic INSERT, but the state UPDATE doesn't run (no
        state row to flip). In the default REUSED mode the staging
        table also stays — the next interval's TRUNCATE will clear
        it; orphan-GC handles it across runs."""
        from bollhav.postgres.staging import stage

        model = _model()
        model.state = None
        conn = _mock_conn()
        with stage(conn, model, since=SINCE, until=UNTIL):
            pass

        executed = [str(c.args[0]) for c in conn.execute.call_args_list]
        assert any("INSERT INTO" in q for q in executed), (
            "flush should still issue the atomic move into target"
        )
        assert not any("UPDATE" in q and "applied" in q for q in executed), (
            "without state there is no state row to flip — UPDATE must not run"
        )

    def test_interval_mode_without_state_drops_staging(self) -> None:
        """In `StagingMode.INTERVAL`, even without state, flush
        still drops the staging table — that's the mode's lifecycle.
        Confirms the no-state branch and the per-interval drop branch
        compose correctly."""
        from bollhav.model.staging import Staging, StagingMode
        from bollhav.postgres.staging import stage

        model = _model(staging_cfg=Staging(mode=StagingMode.INTERVAL))
        model.state = None
        conn = _mock_conn()
        with stage(conn, model, since=SINCE, until=UNTIL):
            pass

        executed = [str(c.args[0]) for c in conn.execute.call_args_list]
        assert any("INSERT INTO" in q for q in executed)
        assert any("DROP TABLE" in q for q in executed)
        assert not any("UPDATE" in q and "applied" in q for q in executed)

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

    def test_flush_reused_mode_runs_two_statements(self) -> None:
        """Default `StagingMode.REUSED` flush issues INSERT and
        UPDATE state — no DROP (staging stays for next interval)."""
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
        assert len(drop) == 0
        assert len(update_state) == 1

    def test_flush_interval_mode_runs_three_statements(self) -> None:
        """`StagingMode.INTERVAL` flush issues INSERT, DROP, and
        UPDATE state — each interval is self-contained."""
        from bollhav.model.staging import Staging, StagingMode
        from bollhav.postgres.staging import stage

        conn = _mock_conn()
        model = _model(staging_cfg=Staging(mode=StagingMode.INTERVAL))
        with stage(conn, model, since=SINCE, until=UNTIL) as s:
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

        # Filter for the staging CREATE TABLE — the action set also
        # creates the target table (`public.orders`), which mentions
        # neither `ops` nor `z_public`.
        ddl = next(
            str(call.args[0])
            for call in conn.execute.call_args_list
            if "UNLOGGED TABLE" in str(call.args[0])
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

        # `stage()` runs the full PRE action set, which includes
        # CREATE TABLE for the target AND CREATE UNLOGGED TABLE for
        # the staging table. Filter to the staging one.
        ddl = next(
            str(call.args[0])
            for call in conn.execute.call_args_list
            if "UNLOGGED" in str(call.args[0])
        )
        assert "UNLOGGED TABLE" in ddl

    def test_logged_true_omits_unlogged(self) -> None:
        from bollhav.model.staging import Staging
        from bollhav.postgres.staging import stage

        model = _model(staging_cfg=Staging(logged=True))
        conn = _mock_conn()
        with stage(conn, model, since=SINCE, until=UNTIL):
            pass

        # Logged=True: filter for the staging table CREATE, which now
        # says "CREATE TABLE IF NOT EXISTS z_public.orders_staging_..."
        ddls = [
            str(call.args[0])
            for call in conn.execute.call_args_list
            if "CREATE TABLE IF NOT EXISTS" in str(call.args[0])
            and "staging_" in str(call.args[0])
        ]
        assert ddls, "expected a staging CREATE TABLE statement"
        assert all("UNLOGGED" not in d for d in ddls)


class TestKeepAfterFlush:
    """`keep_after_flush` only applies to `StagingMode.INTERVAL`
    — REUSED mode always keeps the table for the next interval."""

    def test_interval_default_drops_staging_on_flush(self) -> None:
        from bollhav.model.staging import Staging, StagingMode
        from bollhav.postgres.staging import stage

        conn = _mock_conn()
        model = _model(staging_cfg=Staging(mode=StagingMode.INTERVAL))
        with stage(conn, model, since=SINCE, until=UNTIL) as s:
            s.write(pl.DataFrame({"id": [1], "amount": [1.0]}))

        executed = [str(call.args[0]) for call in conn.execute.call_args_list]
        drops = [q for q in executed if "DROP TABLE" in q]
        assert len(drops) == 1  # the staging table

    def test_interval_keep_after_flush_skips_drop(self) -> None:
        from bollhav.model.staging import Staging, StagingMode
        from bollhav.postgres.staging import stage

        model = _model(
            staging_cfg=Staging(mode=StagingMode.INTERVAL, keep_after_flush=True)
        )
        conn = _mock_conn()
        with stage(conn, model, since=SINCE, until=UNTIL) as s:
            s.write(pl.DataFrame({"id": [1], "amount": [1.0]}))

        executed = [str(call.args[0]) for call in conn.execute.call_args_list]
        drops = [q for q in executed if "DROP TABLE" in q]
        assert drops == []
        # state row still gets flipped — that's the atomicity story
        assert any("status = 'applied'" in q for q in executed)

    def test_reused_mode_lifecycle_across_two_intervals(self) -> None:
        """`StagingMode.REUSED` (default) issues `CREATE TABLE`
        exactly once for the pipeline run, then `TRUNCATE` before
        each subsequent interval to clear the prior interval's
        rows. No DROP between intervals."""
        from datetime import timedelta

        from bollhav.postgres.staging import stage

        conn = _mock_conn()
        model = _model()
        with stage(conn, model, since=SINCE, until=UNTIL) as s:
            s.write(pl.DataFrame({"id": [1], "amount": [1.0]}))
        with stage(conn, model, since=UNTIL, until=UNTIL + timedelta(days=1)) as s:
            s.write(pl.DataFrame({"id": [2], "amount": [2.0]}))

        executed = [str(call.args[0]) for call in conn.execute.call_args_list]
        # Filter for staging CREATEs only — `run_pre_model_actions` also
        # fires `CREATE TABLE` for the target on the first interval.
        creates = [q for q in executed if "UNLOGGED TABLE" in q]
        truncates = [q for q in executed if "TRUNCATE TABLE" in q]
        drops = [q for q in executed if "DROP TABLE" in q]
        # Exactly one staging-table CREATE across the two intervals.
        assert len(creates) == 1
        # TRUNCATE fires at the start of EVERY interval — including
        # the first (where the table was just CREATEd empty and the
        # TRUNCATE is a no-op). Keeping the logic simple here is
        # worth the trivial cost on UNLOGGED tables.
        assert len(truncates) == 2
        # No DROPs — staging stays for the next pipeline run's GC.
        assert len(drops) == 0
        assert model.target._applied_model_actions.get("staging_table_created") is True
        assert model.target._applied_model_actions.get("staging_schema_created") is True

    def test_reused_mode_never_drops_on_flush(self) -> None:
        """The default mode keeps the staging table after every
        flush — the next interval's TRUNCATE clears it."""
        from bollhav.postgres.staging import stage

        conn = _mock_conn()
        with stage(conn, _model(), since=SINCE, until=UNTIL) as s:
            s.write(pl.DataFrame({"id": [1], "amount": [1.0]}))

        executed = [str(call.args[0]) for call in conn.execute.call_args_list]
        drops = [q for q in executed if "DROP TABLE" in q]
        assert drops == []
        assert any("status = 'applied'" in q for q in executed)
