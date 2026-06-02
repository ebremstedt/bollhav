"""Tests for bollhav.mssql.staging — the staged write path for MSSQL.

pyodbc connection is mocked. Three things matter for these tests:
  * `write()` routes to the staged path when `model.target.staging` is set
  * `stage()` issues the right DDL/DML in the right order
  * `_assert_supported` refuses cases the phase-1 scope can't handle yet
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from unittest.mock import MagicMock
from uuid import UUID

import polars as pl
import pytest

sys.modules.setdefault("pyodbc", MagicMock())


SINCE = datetime(2024, 1, 1, tzinfo=timezone.utc)
UNTIL = datetime(2024, 1, 2, tzinfo=timezone.utc)
RUN_ID = UUID("00000000-0000-0000-0000-00000000beef")


def _model(
    *,
    staging_cfg=None,
    with_state=False,
    write_mode=None,
    name="orders",
    schema_name="public",
    cols=None,
    partitioned_by=None,
):
    from bollhav.model.batch import Batch
    from bollhav.model.database import Database
    from bollhav.model.staging import Staging
    from bollhav.model.state import State
    from bollhav.model.target import Target
    from bollhav.model.target_schema import TargetSchema
    from bollhav.model.source_table import SourceTable
    from bollhav.model.write_modes import WriteMode
    from bollhav.model.model import Model
    from bollhav.mssql.columns import MssqlColumn, MssqlType

    if cols is None:
        cols = [
            MssqlColumn(name="id", data_type=MssqlType.INT, nullable=False),
            MssqlColumn(name="val", data_type=MssqlType.NVARCHAR, length=50),
        ]
    target_kwargs = dict(
        name=name,
        schema=TargetSchema(name=schema_name),
        database=Database.MSSQL,
        columns=cols,
        write_mode=write_mode or WriteMode.APPEND,
        staging=staging_cfg if staging_cfg is not None else Staging(),
    )
    if partitioned_by is not None:
        target_kwargs["partitioned_by"] = partitioned_by
    model = Model(
        source=SourceTable(name="src"),
        target=Target(**target_kwargs),
        batching=Batch(),
        state=State() if with_state else None,
    )
    model._state_run_id = RUN_ID
    return model


def _mock_conn():
    conn = MagicMock()
    cursor = MagicMock()
    cursor.execute.return_value = cursor
    cursor.fetchall.return_value = []
    conn.cursor.return_value = cursor
    return conn


def _gen(*dfs):
    yield from dfs


# ── naming defaults ──────────────────────────────────────────────────


class TestNamingDefaults:
    def test_default_staging_schema_is_z_prefix(self) -> None:
        from bollhav.mssql.staging import _staging_schema

        assert _staging_schema(_model()) == "z_public"

    def test_default_table_prefix_is_target_name(self) -> None:
        from bollhav.mssql.staging import _staging_table_prefix

        assert _staging_table_prefix(_model()) == "orders_staging_"

    def test_staging_table_uses_first_8_of_run_id(self) -> None:
        from bollhav.mssql.staging import _staging_table

        # run_id "00000000-0000-0000-0000-00000000beef" → first 8 chars = "00000000"
        assert _staging_table(_model(), RUN_ID) == "orders_staging_00000000"

    def test_staging_schema_can_be_overridden(self) -> None:
        from bollhav.model.staging import Staging
        from bollhav.mssql.staging import _staging_schema

        m = _model(staging_cfg=Staging(schema="ops_staging"))
        assert _staging_schema(m) == "ops_staging"

    def test_table_prefix_can_be_overridden(self) -> None:
        from bollhav.model.staging import Staging
        from bollhav.mssql.staging import _staging_table_prefix

        m = _model(staging_cfg=Staging(table_prefix="stg_"))
        assert _staging_table_prefix(m) == "stg_"


# ── guard rails ─────────────────────────────────────────────────────


class TestAssertSupported:
    def test_rejects_state_set(self) -> None:
        from bollhav.mssql.staging import _assert_supported

        m = _model(with_state=True)
        with pytest.raises(NotImplementedError, match="state coordination"):
            _assert_supported(m)

    def test_rejects_view(self) -> None:
        from bollhav.model.write_modes import WriteMode
        from bollhav.mssql.staging import _assert_supported

        m = _model()
        m.target.write_mode = WriteMode.VIEW
        with pytest.raises(NotImplementedError, match="VIEW"):
            _assert_supported(m)

    def test_rejects_no_batching(self) -> None:
        from bollhav.mssql.staging import _assert_supported

        m = _model()
        m.batching = None
        with pytest.raises(ValueError, match="batching"):
            _assert_supported(m)


# ── dispatcher → staged path ────────────────────────────────────────


class TestWriteDispatch:
    def test_staged_routes_to_write_staged(self) -> None:
        from unittest.mock import patch

        from bollhav.mssql.write_modes import write

        with (
            patch("bollhav.mssql.write_modes.write_dataframes") as wd,
            patch("bollhav.mssql.write_modes._write_staged") as ws,
        ):
            write(
                _mock_conn(),
                _model(),
                _gen(pl.DataFrame({"id": [1], "val": ["a"]})),
                since=SINCE,
                until=UNTIL,
            )
        ws.assert_called_once()
        wd.assert_not_called()

    def test_unstaged_routes_to_write_dataframes(self) -> None:
        from unittest.mock import patch

        from bollhav.mssql.write_modes import write

        m = _model()
        m.target.staging = None
        with (
            patch("bollhav.mssql.write_modes.write_dataframes") as wd,
            patch("bollhav.mssql.write_modes._write_staged") as ws,
        ):
            write(_mock_conn(), m, _gen(pl.DataFrame({"id": [1], "val": ["a"]})))
        wd.assert_called_once()
        ws.assert_not_called()

    def test_staged_requires_since_until(self) -> None:
        from bollhav.mssql.write_modes import write

        with pytest.raises(ValueError, match="since and until"):
            write(
                _mock_conn(),
                _model(),
                _gen(pl.DataFrame({"id": [1], "val": ["a"]})),
                since=None,
                until=UNTIL,
            )


# ── stage() end-to-end ───────────────────────────────────────────────


class TestStageEndToEnd:
    def test_streams_through_staging_and_flushes(self) -> None:
        from unittest.mock import patch

        from bollhav.mssql.write_modes import write

        conn = _mock_conn()
        with patch("bollhav.mssql.write_modes.ensure_schema_table_and_indexes"):
            write(
                conn,
                _model(),
                _gen(
                    pl.DataFrame({"id": [1, 2], "val": ["a", "b"]}),
                    pl.DataFrame({"id": [3], "val": ["c"]}),
                ),
                since=SINCE,
                until=UNTIL,
            )

        executed = [
            str(c.args[0]) for c in conn.cursor.return_value.execute.call_args_list
        ]
        # CREATE staging schema and table fired
        assert any("CREATE SCHEMA" in q for q in executed)
        assert any("CREATE TABLE" in q and "orders_staging_" in q for q in executed)
        # Fresh table per interval → no TRUNCATE; the table is dropped on flush
        assert not any("TRUNCATE TABLE" in q for q in executed)
        assert any("DROP TABLE" in q and "orders_staging_" in q for q in executed)
        # Two bulk inserts via fast_executemany — one per chunk
        assert conn.cursor.return_value.executemany.call_count == 2
        # Final flush: INSERT INTO target SELECT FROM staging
        assert any(
            "INSERT INTO" in q and "SELECT" in q and "FROM" in q for q in executed
        )

    def test_drops_staging_in_flush(self) -> None:
        from unittest.mock import patch

        from bollhav.mssql.write_modes import write

        conn = _mock_conn()
        m = _model()
        with patch("bollhav.mssql.write_modes.ensure_schema_table_and_indexes"):
            write(
                conn,
                m,
                _gen(pl.DataFrame({"id": [1], "val": ["a"]})),
                since=SINCE,
                until=UNTIL,
            )

        executed = [
            str(c.args[0]) for c in conn.cursor.return_value.execute.call_args_list
        ]
        # Fresh table per interval: staging table dropped in the flush
        assert any("DROP TABLE" in q and "orders_staging_" in q for q in executed)

    def test_keep_after_apply_does_not_drop(self) -> None:
        from unittest.mock import patch

        from bollhav.model.staging import Staging
        from bollhav.mssql.write_modes import write

        conn = _mock_conn()
        m = _model(staging_cfg=Staging(keep_after_apply=True))
        with patch("bollhav.mssql.write_modes.ensure_schema_table_and_indexes"):
            write(
                conn,
                m,
                _gen(pl.DataFrame({"id": [1], "val": ["a"]})),
                since=SINCE,
                until=UNTIL,
            )

        executed = [
            str(c.args[0]) for c in conn.cursor.return_value.execute.call_args_list
        ]
        # keep_after_apply=True → no DROP of the staging table
        assert not any("DROP TABLE" in q and "orders_staging_" in q for q in executed)


# ── orphan GC ───────────────────────────────────────────────────────


class TestGcOrphanStagingTables:
    def test_drops_tables_matching_prefix(self) -> None:
        from bollhav.mssql.staging import gc_orphan_staging_tables

        conn = _mock_conn()
        cursor = conn.cursor.return_value
        cursor.execute.return_value.fetchall.return_value = [
            ("orders_staging_abc12345",),
            ("orders_staging_def67890",),
        ]

        gc_orphan_staging_tables(conn, _model())

        executed = [str(c.args[0]) for c in cursor.execute.call_args_list]
        drops = [q for q in executed if "DROP TABLE" in q]
        assert len(drops) == 2

    def test_keep_run_id_preserves_current_table(self) -> None:
        from bollhav.mssql.staging import gc_orphan_staging_tables

        conn = _mock_conn()
        cursor = conn.cursor.return_value
        cursor.execute.return_value.fetchall.return_value = [
            ("orders_staging_00000000",),  # this one matches RUN_ID's first 8
            ("orders_staging_def67890",),
        ]

        gc_orphan_staging_tables(conn, _model(), keep_run_id=RUN_ID)

        executed = [str(c.args[0]) for c in cursor.execute.call_args_list]
        drops = [q for q in executed if "DROP TABLE" in q]
        # one orphan dropped, one kept
        assert len(drops) == 1
        assert "def67890" in drops[0]

    def test_keep_after_apply_disables_gc(self) -> None:
        from bollhav.model.staging import Staging
        from bollhav.mssql.staging import gc_orphan_staging_tables

        conn = _mock_conn()
        m = _model(staging_cfg=Staging(keep_after_apply=True))
        gc_orphan_staging_tables(conn, m)
        # When keep_after_apply=True, GC is a no-op — no SELECT, no DROP
        assert conn.cursor.return_value.execute.call_count == 0


# ── MssqlStaging subclass ───────────────────────────────────────────


class TestMssqlStagingSubclass:
    def test_is_subclass_of_neutral_staging(self) -> None:
        from bollhav.model.staging import Staging
        from bollhav.mssql.staging import MssqlStaging

        assert issubclass(MssqlStaging, Staging)

    def test_mssql_staging_can_be_used_on_target(self) -> None:
        from bollhav.mssql.staging import MssqlStaging, _staging_schema

        m = _model(staging_cfg=MssqlStaging(schema="custom"))
        assert _staging_schema(m) == "custom"


# ── write_mode combinations ──────────────────────────────────────────


def _unique_cols():
    from bollhav.mssql.columns import MssqlColumn, MssqlType

    return [
        MssqlColumn(
            name="id",
            data_type=MssqlType.INT,
            nullable=False,
            primary_key=True,
            unique=True,
        ),
        MssqlColumn(name="val", data_type=MssqlType.NVARCHAR, length=50),
    ]


class TestStagingWriteModeAppend:
    """staging.write_mode=APPEND — chunks bulk-insert into staging."""

    def test_each_chunk_appended_via_bulk_insert(self) -> None:
        from unittest.mock import patch

        from bollhav.mssql.write_modes import write

        conn = _mock_conn()
        with patch("bollhav.mssql.write_modes.ensure_schema_table_and_indexes"):
            write(
                conn,
                _model(),
                _gen(
                    pl.DataFrame({"id": [1, 2], "val": ["a", "b"]}),
                    pl.DataFrame({"id": [3], "val": ["c"]}),
                ),
                since=SINCE,
                until=UNTIL,
            )

        # No MERGE on the staging side — both chunks landed via executemany.
        executed = [
            str(c.args[0]) for c in conn.cursor.return_value.execute.call_args_list
        ]
        merge_into_staging = [
            q for q in executed if "MERGE INTO" in q and "orders_staging_" in q
        ]
        assert not merge_into_staging
        assert conn.cursor.return_value.executemany.call_count == 2


class TestStagingWriteModeUpsert:
    """staging.write_mode=UPSERT_NO_DELETE — chunks MERGE into staging."""

    def test_each_chunk_merged_into_staging(self) -> None:
        from unittest.mock import patch

        from bollhav.model.staging import Staging
        from bollhav.model.write_modes import WriteMode
        from bollhav.mssql.write_modes import write

        conn = _mock_conn()
        m = _model(
            cols=_unique_cols(),
            write_mode=WriteMode.UPSERT_NO_DELETE,
            staging_cfg=Staging(write_mode=WriteMode.UPSERT_NO_DELETE),
        )
        with patch("bollhav.mssql.write_modes.ensure_schema_table_and_indexes"):
            write(
                conn,
                m,
                _gen(
                    pl.DataFrame({"id": [1], "val": ["a"]}),
                    pl.DataFrame({"id": [2], "val": ["b"]}),
                ),
                since=SINCE,
                until=UNTIL,
            )

        executed = [
            str(c.args[0]) for c in conn.cursor.return_value.execute.call_args_list
        ]
        # MERGE INTO staging happens once per chunk (via the
        # _merge_via_temp helper that lands rows in #tmp_merge_* then
        # MERGEs into the staging table). The target-side MERGE on
        # flush is filtered out by anchoring on the staging-as-target
        # form.
        merge_into_staging = [
            q for q in executed if "MERGE INTO [z_public].[orders_staging_" in q
        ]
        assert len(merge_into_staging) == 2


class TestFlushTargetAppend:
    """target.write_mode=APPEND — flush does INSERT FROM staging."""

    def test_flush_issues_insert_select(self) -> None:
        from unittest.mock import patch

        from bollhav.mssql.write_modes import write

        conn = _mock_conn()
        with patch("bollhav.mssql.write_modes.ensure_schema_table_and_indexes"):
            write(
                conn,
                _model(),
                _gen(pl.DataFrame({"id": [1], "val": ["a"]})),
                since=SINCE,
                until=UNTIL,
            )

        executed = [
            str(c.args[0]) for c in conn.cursor.return_value.execute.call_args_list
        ]
        # Final flush — INSERT INTO target SELECT FROM staging
        flush_inserts = [
            q for q in executed if q.startswith("INSERT INTO [public].[orders]")
        ]
        assert len(flush_inserts) == 1
        assert "SELECT" in flush_inserts[0]
        assert "orders_staging_" in flush_inserts[0]


class TestFlushTargetUpsert:
    """target.write_mode=UPSERT_NO_DELETE — flush does MERGE target USING staging."""

    def test_flush_issues_merge_into_target(self) -> None:
        from unittest.mock import patch

        from bollhav.model.write_modes import WriteMode
        from bollhav.mssql.write_modes import write

        conn = _mock_conn()
        m = _model(cols=_unique_cols(), write_mode=WriteMode.UPSERT_NO_DELETE)
        with patch("bollhav.mssql.write_modes.ensure_schema_table_and_indexes"):
            write(
                conn,
                m,
                _gen(pl.DataFrame({"id": [1], "val": ["a"]})),
                since=SINCE,
                until=UNTIL,
            )

        executed = [
            str(c.args[0]) for c in conn.cursor.return_value.execute.call_args_list
        ]
        # The target-side MERGE pulls directly from staging (no #tmp
        # hop), so the USING clause references the staging table.
        target_merges = [
            q
            for q in executed
            if "MERGE INTO [public].[orders]" in q and "orders_staging_" in q
        ]
        assert len(target_merges) == 1


class TestFlushTargetRecreatePartition:
    """target.write_mode=RECREATE_PARTITION — flush deletes the window
    and inserts from staging in one transaction."""

    def test_flush_issues_delete_window_then_insert(self) -> None:
        from datetime import datetime
        from unittest.mock import patch

        from bollhav.model.write_modes import WriteMode
        from bollhav.mssql.columns import MssqlColumn, MssqlType
        from bollhav.mssql.write_modes import write

        conn = _mock_conn()
        cols = [
            MssqlColumn(name="ts", data_type=MssqlType.DATETIME2, partition_on=True),
            MssqlColumn(name="val", data_type=MssqlType.NVARCHAR, length=50),
        ]
        m = _model(cols=cols, write_mode=WriteMode.RECREATE_PARTITION)

        with patch("bollhav.mssql.write_modes.ensure_schema_table_and_indexes"):
            write(
                conn,
                m,
                _gen(pl.DataFrame({"ts": [datetime(2024, 1, 1, 12)], "val": ["a"]})),
                since=SINCE,
                until=UNTIL,
            )

        executed = [
            str(c.args[0]) for c in conn.cursor.return_value.execute.call_args_list
        ]
        # The flush is the only DELETE on the target (not on staging).
        target_deletes = [
            q
            for q in executed
            if q.startswith("DELETE FROM [public].[orders]")
            and "[ts] >= ? AND [ts] < ?" in q
        ]
        assert len(target_deletes) == 1
        # Followed by INSERT INTO target SELECT FROM staging
        flush_inserts = [
            q for q in executed if q.startswith("INSERT INTO [public].[orders]")
        ]
        assert len(flush_inserts) == 1
        assert "orders_staging_" in flush_inserts[0]


class TestStagingWriteModeValidation:
    """Staging.__post_init__ rejects write_modes that don't apply to chunks."""

    def test_rejects_recreate_partition(self) -> None:
        from bollhav.model.staging import Staging
        from bollhav.model.write_modes import WriteMode

        with pytest.raises(ValueError, match="must be WriteMode.APPEND"):
            Staging(write_mode=WriteMode.RECREATE_PARTITION)

    def test_rejects_view(self) -> None:
        from bollhav.model.staging import Staging
        from bollhav.model.write_modes import WriteMode

        with pytest.raises(ValueError, match="must be WriteMode.APPEND"):
            Staging(write_mode=WriteMode.VIEW)
