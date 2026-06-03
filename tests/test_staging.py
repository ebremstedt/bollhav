"""Tests for bollhav.postgres.staging — the staging primitives.

Postgres connection is mocked. The old `stage()` context manager is
gone; the staging table's *lifecycle* now lives in `@execute_lifecycle`
(create -> write -> apply -> drop) and is driven through `PostgresData`.
This module holds the primitives those steps delegate to:

  * `write_to_staging(conn, model, run_id, df)` — COPY / upsert one chunk
  * `apply_atomically_to_target(conn, model, run_id=, since=, until=)` —
    merge staging -> target in one tx (drops staging unless kept)
  * `drop_staging_table(conn, model, run_id)` — tear the table down
  * naming helpers `_staging_schema` / `_staging_table_prefix` /
    `_staging_table`
  * orphan GC, which now lives on
    `PostgresData.gc_orphan_staging_tables`

Real-DB exercise lives in the runnable example.
"""

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
    # Directive flags pinned so derived gates evaluate predictably — a
    # MagicMock attribute is truthy by default, which would misfire.
    model.target.recreate_table = False
    model.target.truncate_table = False
    model.target.partitioned_by = None
    model.target.unique_columns = []
    model._state_run_id = RUN_ID
    model.run_id = RUN_ID
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


def _rendered(conn) -> list[str]:
    """Render every psycopg Composed object on the conn into actual SQL
    text via `as_string(None)`. `str()` on a Composed gives its Python
    repr, not the SQL, so substring matches against quoted identifiers
    fail. `as_string` does the real rendering."""
    from psycopg.sql import Composable

    out = []
    for call in conn.execute.call_args_list:
        q = call.args[0]
        out.append(q.as_string(None) if isinstance(q, Composable) else str(q))
    return out


# ── create staging table ─────────────────────────────────────────────


class TestCreateStagingTable:
    def test_creates_staging_table(self) -> None:
        """`PostgresData.create_staging_table` issues one CREATE for the
        per-interval staging table (UNLOGGED by default)."""
        from bollhav.postgres.data import PostgresData

        conn = _mock_conn()
        PostgresData(_model(), conn).create_staging_table(RUN_ID)

        ddl_calls = [
            call
            for call in conn.execute.call_args_list
            if "CREATE UNLOGGED TABLE" in str(call.args[0])
        ]
        assert len(ddl_calls) == 1


# ── write to staging ─────────────────────────────────────────────────


class TestWriteToStaging:
    def test_write_copies_each_chunk(self) -> None:
        from bollhav.postgres.staging import write_to_staging

        conn = _mock_conn()
        model = _model()
        write_to_staging(
            conn, model, RUN_ID, pl.DataFrame({"id": [1, 2], "amount": [1.0, 2.0]})
        )
        write_to_staging(
            conn, model, RUN_ID, pl.DataFrame({"id": [3], "amount": [3.0]})
        )

        assert conn.cursor.return_value.copy.call_count == 2

    def test_empty_chunk_is_skipped(self) -> None:
        from bollhav.postgres.staging import write_to_staging

        conn = _mock_conn()
        write_to_staging(conn, _model(), RUN_ID, pl.DataFrame({"id": [], "amount": []}))

        assert conn.cursor.return_value.copy.call_count == 0


# ── apply staging -> target ──────────────────────────────────────────


class TestApply:
    def test_apply_runs_insert_and_drop(self) -> None:
        """Apply issues INSERT and (by default) DROP the staging table.
        The state flip is decoupled — `@execute_lifecycle` marks applied
        separately on the state connection, not in the data-move tx."""
        from bollhav.postgres.staging import apply_atomically_to_target

        conn = _mock_conn()
        apply_atomically_to_target(
            conn, _model(), run_id=RUN_ID, since=SINCE, until=UNTIL
        )

        executed = [str(call.args[0]) for call in conn.execute.call_args_list]
        insert_select = [q for q in executed if "INSERT INTO" in q and "SELECT" in q]
        drop = [q for q in executed if "DROP TABLE" in q]
        update_state = [
            q for q in executed if "UPDATE" in q and "status = 'applied'" in q
        ]
        assert len(insert_select) == 1
        assert len(drop) == 1
        assert update_state == []  # apply no longer flips state

    def test_drop_after_apply_false_keeps_staging(self) -> None:
        """The lifecycle hook calls apply with `drop_after_apply=False`
        so the merge and the teardown stay distinct phases."""
        from bollhav.postgres.staging import apply_atomically_to_target

        conn = _mock_conn()
        apply_atomically_to_target(
            conn,
            _model(),
            run_id=RUN_ID,
            since=SINCE,
            until=UNTIL,
            drop_after_apply=False,
        )

        executed = [str(call.args[0]) for call in conn.execute.call_args_list]
        assert any("INSERT INTO" in q for q in executed)
        assert not any("DROP TABLE" in q for q in executed)


# ── naming ───────────────────────────────────────────────────────────


class TestNaming:
    def test_staging_table_uses_short_run_id(self) -> None:
        from bollhav.postgres.staging import _staging_table

        assert _staging_table(_model(), RUN_ID) == "orders_staging_00000000"

    def test_staging_schema_is_z_prefixed(self) -> None:
        from bollhav.postgres.staging import _staging_schema

        assert _staging_schema(_model()) == "z_public"


# ── orphan GC ────────────────────────────────────────────────────────


class TestGc:
    def test_drops_orphans_keeps_current(self) -> None:
        from bollhav.postgres.data import PostgresData

        model = _model()
        current = f"orders_staging_{str(RUN_ID)[:8]}"
        rows = [
            ("orders_staging_aaaaaaaa",),
            ("orders_staging_bbbbbbbb",),
            (current,),
        ]
        conn = _mock_conn()
        conn.execute.return_value.fetchall.return_value = rows

        PostgresData(model, conn).gc_orphan_staging_tables(keep_run_id=RUN_ID)

        dropped = [
            str(call.args[0])
            for call in conn.execute.call_args_list
            if "DROP TABLE" in str(call.args[0])
        ]
        assert len(dropped) == 2

    def test_skipped_when_keep_after_apply(self) -> None:
        from bollhav.model.staging import Staging
        from bollhav.postgres.data import PostgresData

        model = _model(staging_cfg=Staging(keep_after_apply=True))
        conn = _mock_conn()

        PostgresData(model, conn).gc_orphan_staging_tables(keep_run_id=RUN_ID)

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
        from bollhav.postgres.data import PostgresData

        model = _model(staging_cfg=Staging(schema="ops"))
        conn = _mock_conn()
        PostgresData(model, conn).create_staging_table(RUN_ID)

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
        from bollhav.postgres.data import PostgresData

        conn = _mock_conn()
        PostgresData(_model(), conn).create_staging_table(RUN_ID)

        ddl = next(
            str(call.args[0])
            for call in conn.execute.call_args_list
            if "UNLOGGED" in str(call.args[0])
        )
        assert "UNLOGGED TABLE" in ddl

    def test_logged_true_omits_unlogged(self) -> None:
        from bollhav.postgres.data import PostgresData
        from bollhav.postgres.staging import PostgresStaging

        model = _model(staging_cfg=PostgresStaging(logged=True))
        conn = _mock_conn()
        PostgresData(model, conn).create_staging_table(RUN_ID)

        # Logged=True: the staging CREATE says "CREATE TABLE IF NOT
        # EXISTS z_public.orders_staging_..." with no UNLOGGED.
        ddls = [
            str(call.args[0])
            for call in conn.execute.call_args_list
            if "CREATE TABLE IF NOT EXISTS" in str(call.args[0])
            and "staging_" in str(call.args[0])
        ]
        assert ddls, "expected a staging CREATE TABLE statement"
        assert all("UNLOGGED" not in d for d in ddls)


class TestKeepAfterApply:
    """Apply makes a fresh table per interval and drops it on flush;
    `keep_after_apply` skips that drop."""

    def test_default_drops_staging_on_apply(self) -> None:
        from bollhav.postgres.staging import apply_atomically_to_target

        conn = _mock_conn()
        apply_atomically_to_target(
            conn, _model(), run_id=RUN_ID, since=SINCE, until=UNTIL
        )

        executed = [str(call.args[0]) for call in conn.execute.call_args_list]
        drops = [q for q in executed if "DROP TABLE" in q]
        assert len(drops) == 1  # the staging table

    def test_keep_after_apply_skips_drop(self) -> None:
        from bollhav.model.staging import Staging
        from bollhav.postgres.staging import apply_atomically_to_target

        model = _model(staging_cfg=Staging(keep_after_apply=True))
        conn = _mock_conn()
        apply_atomically_to_target(conn, model, run_id=RUN_ID, since=SINCE, until=UNTIL)

        executed = [str(call.args[0]) for call in conn.execute.call_args_list]
        drops = [q for q in executed if "DROP TABLE" in q]
        assert drops == []
        # apply no longer flips state — that's the lifecycle's job now
        assert not any("status = 'applied'" in q for q in executed)

    def test_drop_staging_table_no_op_when_kept(self) -> None:
        """`PostgresData.drop_staging_table` is a no-op under
        `keep_after_apply` — the teardown step respects the keep flag."""
        from bollhav.model.staging import Staging
        from bollhav.postgres.data import PostgresData

        model = _model(staging_cfg=Staging(keep_after_apply=True))
        conn = _mock_conn()
        PostgresData(model, conn).drop_staging_table(RUN_ID)

        executed = [str(call.args[0]) for call in conn.execute.call_args_list]
        assert not any("DROP TABLE" in q for q in executed)

    def test_lifecycle_across_two_intervals(self) -> None:
        """Each interval CREATEs a fresh staging table and DROPs it on
        flush — so across two intervals there are two staging CREATEs
        and two DROPs, and no TRUNCATE."""
        from datetime import timedelta

        from bollhav.postgres.data import PostgresData
        from bollhav.postgres.staging import (
            apply_atomically_to_target,
            write_to_staging,
        )

        conn = _mock_conn()
        model = _model()
        data = PostgresData(model, conn)

        data.create_staging_table(RUN_ID)
        write_to_staging(
            conn, model, RUN_ID, pl.DataFrame({"id": [1], "amount": [1.0]})
        )
        apply_atomically_to_target(conn, model, run_id=RUN_ID, since=SINCE, until=UNTIL)

        data.create_staging_table(RUN_ID)
        write_to_staging(
            conn, model, RUN_ID, pl.DataFrame({"id": [2], "amount": [2.0]})
        )
        apply_atomically_to_target(
            conn, model, run_id=RUN_ID, since=UNTIL, until=UNTIL + timedelta(days=1)
        )

        executed = [str(call.args[0]) for call in conn.execute.call_args_list]
        creates = [q for q in executed if "UNLOGGED TABLE" in q]
        truncates = [q for q in executed if "TRUNCATE TABLE" in q]
        drops = [q for q in executed if "DROP TABLE" in q]
        assert len(creates) == 2  # fresh staging table each interval
        assert len(truncates) == 0  # no reuse → no truncate
        assert len(drops) == 2  # dropped on each flush


# ── write_mode combinations ──────────────────────────────────────────


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
        from bollhav.postgres.staging import write_to_staging

        conn = _mock_conn()
        model = _model()
        write_to_staging(
            conn, model, RUN_ID, pl.DataFrame({"id": [1], "amount": [1.0]})
        )
        write_to_staging(
            conn, model, RUN_ID, pl.DataFrame({"id": [2], "amount": [2.0]})
        )

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
        from bollhav.postgres.staging import write_to_staging

        conn = _mock_conn()
        m = _model_with_unique()
        m.target.write_mode = WriteMode.UPSERT_NO_DELETE
        m.target.staging = Staging(write_mode=WriteMode.UPSERT_NO_DELETE)
        write_to_staging(conn, m, RUN_ID, pl.DataFrame({"id": [1], "amount": [1.0]}))
        write_to_staging(
            conn, m, RUN_ID, pl.DataFrame({"id": [1], "amount": [2.0]})
        )  # dup key

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
        from bollhav.postgres.staging import apply_atomically_to_target

        conn = _mock_conn()
        apply_atomically_to_target(
            conn, _model(), run_id=RUN_ID, since=SINCE, until=UNTIL
        )

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
        from bollhav.postgres.staging import apply_atomically_to_target

        conn = _mock_conn()
        m = _model_with_unique()
        m.target.write_mode = WriteMode.UPSERT_NO_DELETE
        apply_atomically_to_target(conn, m, run_id=RUN_ID, since=SINCE, until=UNTIL)

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
        from bollhav.postgres.staging import apply_atomically_to_target

        conn = _mock_conn()
        m = _model()
        m.target.columns = [
            PostgresColumn(name="ts", data_type=PostgresType.TIMESTAMPTZ),
            PostgresColumn(name="amount", data_type=PostgresType.NUMERIC),
        ]
        m.target.write_mode = WriteMode.RECREATE_PARTITION
        m.target.partitioned_by = "ts"
        apply_atomically_to_target(conn, m, run_id=RUN_ID, since=SINCE, until=UNTIL)

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
