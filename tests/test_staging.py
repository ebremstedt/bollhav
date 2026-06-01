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
from unittest.mock import MagicMock
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
    def test_rejects_view_write_mode(self) -> None:
        from bollhav.model.write_modes import WriteMode
        from bollhav.postgres.staging import stage

        model = _model()
        model.target.write_mode = WriteMode.VIEW
        with pytest.raises(NotImplementedError, match="VIEW"):
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
        state row to flip). The staging table is still dropped on
        flush — staging self-cleans regardless of state."""
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

    def test_without_state_still_drops_staging(self) -> None:
        """Even without state, flush still drops the staging table —
        staging self-cleans. Confirms the no-state branch and the
        per-interval drop compose correctly."""
        from bollhav.model.staging import Staging
        from bollhav.postgres.staging import stage

        model = _model(staging_cfg=Staging())
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

    def test_flush_runs_three_statements(self) -> None:
        """Flush issues INSERT, DROP (fresh table per interval), and
        UPDATE state — each interval is self-contained."""
        from bollhav.postgres.staging import stage

        conn = _mock_conn()
        model = _model()
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

        gc_orphan_staging_tables(conn, model, keep_run_id=RUN_ID)

        dropped = [
            str(call.args[0])
            for call in conn.execute.call_args_list
            if "DROP TABLE" in str(call.args[0])
        ]
        assert len(dropped) == 2

    def test_skipped_when_keep_after_apply(self) -> None:
        from bollhav.model.staging import Staging
        from bollhav.postgres.staging import gc_orphan_staging_tables

        model = _model(staging_cfg=Staging(keep_after_apply=True))
        conn = _mock_conn()

        gc_orphan_staging_tables(conn, model, keep_run_id=RUN_ID)

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
        from bollhav.postgres.staging import PostgresStaging, stage

        model = _model(staging_cfg=PostgresStaging(logged=True))
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
    """Staging makes a fresh table per interval and drops it on flush;
    `keep_after_apply` skips that drop."""

    def test_default_drops_staging_on_flush(self) -> None:
        from bollhav.postgres.staging import stage

        conn = _mock_conn()
        model = _model()
        with stage(conn, model, since=SINCE, until=UNTIL) as s:
            s.write(pl.DataFrame({"id": [1], "amount": [1.0]}))

        executed = [str(call.args[0]) for call in conn.execute.call_args_list]
        drops = [q for q in executed if "DROP TABLE" in q]
        assert len(drops) == 1  # the staging table

    def test_keep_after_apply_skips_drop(self) -> None:
        from bollhav.model.staging import Staging
        from bollhav.postgres.staging import stage

        model = _model(staging_cfg=Staging(keep_after_apply=True))
        conn = _mock_conn()
        with stage(conn, model, since=SINCE, until=UNTIL) as s:
            s.write(pl.DataFrame({"id": [1], "amount": [1.0]}))

        executed = [str(call.args[0]) for call in conn.execute.call_args_list]
        drops = [q for q in executed if "DROP TABLE" in q]
        assert drops == []
        # state row still gets flipped — that's the atomicity story
        assert any("status = 'applied'" in q for q in executed)

    def test_lifecycle_across_two_intervals(self) -> None:
        """Each interval CREATEs a fresh staging table and DROPs it on
        flush — so across two intervals there are two staging CREATEs
        and two DROPs, and no TRUNCATE."""
        from datetime import timedelta

        from bollhav.postgres.staging import stage

        conn = _mock_conn()
        model = _model()
        with stage(conn, model, since=SINCE, until=UNTIL) as s:
            s.write(pl.DataFrame({"id": [1], "amount": [1.0]}))
        with stage(conn, model, since=UNTIL, until=UNTIL + timedelta(days=1)) as s:
            s.write(pl.DataFrame({"id": [2], "amount": [2.0]}))

        executed = [str(call.args[0]) for call in conn.execute.call_args_list]
        # `UNLOGGED TABLE` only appears for the staging CREATE (the target
        # CREATE on interval 1 is a plain TABLE).
        creates = [q for q in executed if "UNLOGGED TABLE" in q]
        truncates = [q for q in executed if "TRUNCATE TABLE" in q]
        drops = [q for q in executed if "DROP TABLE" in q]
        assert len(creates) == 2  # fresh staging table each interval
        assert len(truncates) == 0  # no reuse → no truncate
        assert len(drops) == 2  # dropped on each flush
        assert model.target._applied_model_actions.get("staging_schema_created") is True


# ── write_mode combinations ──────────────────────────────────────────


def _rendered(conn) -> list[str]:
    """Render every psycopg Composed object on the conn into actual
    SQL text via `as_string(None)`. `str()` on a Composed gives its
    Python repr, not the SQL, so substring matches against quoted
    identifiers fail. `as_string` does the real rendering."""
    from psycopg.sql import Composable

    out = []
    for call in conn.execute.call_args_list:
        q = call.args[0]
        out.append(q.as_string(None) if isinstance(q, Composable) else str(q))
    return out


def _model_with_unique():
    """Model with a unique column so UPSERT_NO_DELETE-style merges
    have something to merge on."""
    from bollhav.postgres.columns import PostgresColumn, PostgresType

    m = _model()
    m.target.columns = [
        PostgresColumn(
            name="id",
            data_type=PostgresType.BIGINT,
            primary_key=True,
            nullable=False,
        ),
        PostgresColumn(name="amount", data_type=PostgresType.NUMERIC),
    ]
    m.target.unique_columns = [m.target.columns[0]]
    return m


class TestStagingWriteModeAppend:
    """staging.write_mode=APPEND (default) — chunks COPY into staging."""

    def test_each_chunk_copies_no_on_conflict_into_staging(self) -> None:
        from bollhav.postgres.staging import stage

        conn = _mock_conn()
        with stage(conn, _model(), since=SINCE, until=UNTIL) as s:
            s.write(pl.DataFrame({"id": [1], "amount": [1.0]}))
            s.write(pl.DataFrame({"id": [2], "amount": [2.0]}))

        executed = _rendered(conn)
        # No ON CONFLICT INTO staging — APPEND just COPYs.
        merges_into_staging = [
            q
            for q in executed
            if "ON CONFLICT" in q and 'INTO "z_public"."orders_staging_' in q
        ]
        assert not merges_into_staging


class TestStagingWriteModeUpsert:
    """staging.write_mode=UPSERT_NO_DELETE — chunks upsert into staging
    via a temp table + ON CONFLICT."""

    def test_each_chunk_upserts_into_staging(self) -> None:
        from bollhav.model.staging import Staging
        from bollhav.model.write_modes import WriteMode
        from bollhav.postgres.staging import stage

        conn = _mock_conn()
        m = _model_with_unique()
        m.target.write_mode = WriteMode.UPSERT_NO_DELETE
        m.target.staging = Staging(write_mode=WriteMode.UPSERT_NO_DELETE)
        with stage(conn, m, since=SINCE, until=UNTIL) as s:
            s.write(pl.DataFrame({"id": [1], "amount": [1.0]}))
            s.write(pl.DataFrame({"id": [1], "amount": [2.0]}))  # duplicate key

        executed = _rendered(conn)
        staging_upserts = [
            q
            for q in executed
            if "ON CONFLICT" in q and 'INTO "z_public"."orders_staging_' in q
        ]
        assert len(staging_upserts) == 2


class TestApplyTargetAppend:
    """target.write_mode=APPEND — apply does INSERT FROM staging."""

    def test_apply_issues_insert_select(self) -> None:
        from bollhav.postgres.staging import stage

        conn = _mock_conn()
        with stage(conn, _model(), since=SINCE, until=UNTIL) as s:
            s.write(pl.DataFrame({"id": [1], "amount": [1.0]}))

        executed = _rendered(conn)
        applies = [
            q
            for q in executed
            if 'INSERT INTO "public"."orders"' in q
            and 'FROM "z_public"."orders_staging_' in q
            and "ON CONFLICT" not in q
        ]
        assert len(applies) == 1


class TestApplyTargetUpsert:
    """target.write_mode=UPSERT_NO_DELETE — apply does INSERT FROM
    staging ... ON CONFLICT DO UPDATE."""

    def test_apply_issues_insert_with_on_conflict(self) -> None:
        from bollhav.model.write_modes import WriteMode
        from bollhav.postgres.staging import stage

        conn = _mock_conn()
        m = _model_with_unique()
        m.target.write_mode = WriteMode.UPSERT_NO_DELETE
        with stage(conn, m, since=SINCE, until=UNTIL) as s:
            s.write(pl.DataFrame({"id": [1], "amount": [1.0]}))

        executed = _rendered(conn)
        target_upserts = [
            q
            for q in executed
            if 'INSERT INTO "public"."orders"' in q
            and 'FROM "z_public"."orders_staging_' in q
            and "ON CONFLICT" in q
        ]
        assert len(target_upserts) == 1


class TestApplyTargetRecreatePartition:
    """target.write_mode=RECREATE_PARTITION — apply deletes the window
    then INSERTs from staging, all in one transaction."""

    def test_apply_issues_delete_window_then_insert(self) -> None:
        from bollhav.model.write_modes import WriteMode
        from bollhav.postgres.columns import PostgresColumn, PostgresType
        from bollhav.postgres.staging import stage

        conn = _mock_conn()
        m = _model()
        m.target.columns = [
            PostgresColumn(name="ts", data_type=PostgresType.TIMESTAMPTZ),
            PostgresColumn(name="amount", data_type=PostgresType.NUMERIC),
        ]
        m.target.write_mode = WriteMode.RECREATE_PARTITION
        m.target.partitioned_by = "ts"
        with stage(conn, m, since=SINCE, until=UNTIL) as s:
            s.write(pl.DataFrame({"ts": [SINCE], "amount": [1.0]}))

        executed = _rendered(conn)
        target_deletes = [
            q
            for q in executed
            if 'DELETE FROM "public"."orders"' in q and '"ts" >= %s AND "ts" < %s' in q
        ]
        assert len(target_deletes) == 1
        target_inserts = [
            q
            for q in executed
            if 'INSERT INTO "public"."orders"' in q
            and 'FROM "z_public"."orders_staging_' in q
        ]
        assert len(target_inserts) == 1
