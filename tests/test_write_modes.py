"""Tests for bollhav.postgres.write_modes.write — the top-level
dispatcher that picks between direct and staged write paths.

Postgres connection is mocked; the two real branches we care about are:
  * `model.target.staging is None` → routes to `write_dataframes` (per-chunk tx)
  * `model.target.staging is set`  → routes to `_write_staged` → `stage()`
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
from uuid import UUID

import polars as pl
import pytest


SINCE = datetime(2024, 1, 1, tzinfo=timezone.utc)
UNTIL = datetime(2024, 1, 2, tzinfo=timezone.utc)
RUN_ID = UUID("00000000-0000-0000-0000-00000000beef")


def _model(*, staged=False, with_state=False, write_mode=None, staging_cfg=None):
    from bollhav.model.staging import Staging
    from bollhav.model.state import State
    from bollhav.model.write_modes import WriteMode
    from bollhav.postgres.columns import PostgresColumn, PostgresType

    # (removed) Mutations replaced by Actions system

    model = MagicMock()
    model.state = State() if (staged or with_state) else None
    model.batching = MagicMock()
    model.target.name = "orders"
    model.target.name_resolved = "orders"
    model.target.full_name = "public.orders"
    model.target.schema.resolved = "public"
    model.target.write_mode = write_mode or WriteMode.APPEND
    model.target.staging = staging_cfg or (Staging() if staged else None)
    model.target.columns = [
        PostgresColumn(name="id", data_type=PostgresType.BIGINT),
        PostgresColumn(name="amount", data_type=PostgresType.NUMERIC),
    ]
    from bollhav.postgres.actions import default_actions as _da

    model.target._applied_model_actions = {}
    model.target.actions = []
    model.target.default_actions = _da()
    model.target.effective_actions = list(model.target.default_actions)
    model.target.setup_complete = False
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


def _gen(*dfs):
    yield from dfs


# ── dispatch ─────────────────────────────────────────────────────────


class TestDispatchToDirect:
    def test_unstaged_routes_to_write_dataframes(self) -> None:
        from bollhav.postgres.write_modes import write

        model = _model(staged=False)
        df_gen = _gen(pl.DataFrame({"id": [1], "amount": [1.0]}))

        with (
            patch("bollhav.postgres.write_modes.write_dataframes") as wd,
            patch("bollhav.postgres.write_modes._write_staged") as ws,
        ):
            write(_mock_conn(), model, df_gen, since=SINCE, until=UNTIL)

        wd.assert_called_once()
        ws.assert_not_called()

    def test_unstaged_does_not_require_since_until(self) -> None:
        """Direct writes for APPEND don't need since/until — the chunk
        is just inserted as-is."""
        from bollhav.postgres.write_modes import write

        model = _model(staged=False)
        df_gen = _gen(pl.DataFrame({"id": [1], "amount": [1.0]}))

        with patch("bollhav.postgres.write_modes.write_dataframes") as wd:
            write(_mock_conn(), model, df_gen)
        wd.assert_called_once()


class TestDispatchToStaged:
    def test_staged_routes_to_write_staged(self) -> None:
        from bollhav.postgres.write_modes import write

        model = _model(staged=True)
        df_gen = _gen(pl.DataFrame({"id": [1], "amount": [1.0]}))

        with (
            patch("bollhav.postgres.write_modes.write_dataframes") as wd,
            patch("bollhav.postgres.write_modes._write_staged") as ws,
        ):
            write(_mock_conn(), model, df_gen, since=SINCE, until=UNTIL)

        ws.assert_called_once()
        wd.assert_not_called()

    def test_staged_requires_since(self) -> None:
        from bollhav.postgres.write_modes import write

        model = _model(staged=True)
        df_gen = _gen(pl.DataFrame({"id": [1], "amount": [1.0]}))

        with pytest.raises(ValueError, match="since and until are required"):
            write(_mock_conn(), model, df_gen, since=None, until=UNTIL)

    def test_staged_requires_until(self) -> None:
        from bollhav.postgres.write_modes import write

        model = _model(staged=True)
        df_gen = _gen(pl.DataFrame({"id": [1], "amount": [1.0]}))

        with pytest.raises(ValueError, match="since and until are required"):
            write(_mock_conn(), model, df_gen, since=SINCE, until=None)


class TestStagedPathEndToEnd:
    """Drive `write()` with staged=True against the real (mocked) stage()
    machinery and assert the expected SQL got issued."""

    def test_streams_through_staging_and_flushes(self) -> None:
        from bollhav.postgres.write_modes import write

        model = _model(staged=True)
        df_gen = _gen(
            pl.DataFrame({"id": [1, 2], "amount": [1.0, 2.0]}),
            pl.DataFrame({"id": [3], "amount": [3.0]}),
        )
        conn = _mock_conn()

        with patch("bollhav.postgres.write_modes.run_pre_model_actions"):
            write(conn, model, df_gen, since=SINCE, until=UNTIL)

        executed = [str(call.args[0]) for call in conn.execute.call_args_list]
        # Staging table created.
        assert any("CREATE UNLOGGED TABLE" in q for q in executed)
        # Two COPY contexts on the cursor (one per chunk).
        assert conn.cursor.return_value.copy.call_count == 2
        # Default `StagingMode.REUSED` flush: INSERT + UPDATE only —
        # the staging table stays for the next interval.
        assert any("INSERT INTO" in q and "SELECT" in q for q in executed)
        assert not any("DROP TABLE" in q and "staging" in q for q in executed)
        assert any("status = 'applied'" in q for q in executed)

    def test_marker_set_after_successful_flush(self) -> None:
        from bollhav.postgres.write_modes import write

        model = _model(staged=True)
        df_gen = _gen(pl.DataFrame({"id": [1], "amount": [1.0]}))

        with patch("bollhav.postgres.write_modes.run_pre_model_actions"):
            write(_mock_conn(), model, df_gen, since=SINCE, until=UNTIL)

        assert model._state_applied_via_staging == (SINCE, UNTIL)


# ── VIEW + unhandled modes (regression coverage on the dispatcher) ──


class TestViewMode:
    def test_view_does_not_take_df_gen(self) -> None:
        from bollhav.model.write_modes import WriteMode
        from bollhav.postgres.write_modes import write

        model = _model(write_mode=WriteMode.VIEW)
        df_gen = _gen(pl.DataFrame({"id": [1], "amount": [1.0]}))
        with pytest.raises(ValueError, match="VIEW does not need a dataframe"):
            write(_mock_conn(), model, df_gen)

    def test_view_routes_to_create_replace_view(self) -> None:
        from bollhav.model.write_modes import WriteMode
        from bollhav.postgres.write_modes import write

        model = _model(write_mode=WriteMode.VIEW)
        with patch("bollhav.postgres.write_modes.create_replace_view") as crv:
            write(_mock_conn(), model, df_gen=None)
        crv.assert_called_once()


class TestDfGenMissing:
    def test_append_needs_df_gen(self) -> None:
        from bollhav.postgres.write_modes import write

        model = _model(staged=False)
        with pytest.raises(ValueError, match="need a dataframe"):
            write(_mock_conn(), model, df_gen=None)
