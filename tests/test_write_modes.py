"""Tests for bollhav.postgres.write_modes.write — the top-level
dispatcher that picks between the direct and staged write paths.

Postgres connection is mocked; the two real branches we care about are:
  * `model.target.stage is False` → routes to `write_dataframes` (per-chunk tx)
  * `model.target.stage is True`  → routes to `_write_staged`, which COPYs
    each chunk into the per-interval staging table

The staging table's *lifecycle* (create -> apply -> drop) is no longer
done inside `write()`; it lives in `@execute_lifecycle`. `_write_staged`
just lands the rows.
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


def _model(
    *, staged=False, with_state=False, write_mode=None, staging_cfg=None, is_view=False
):
    from bollhav.model.staging import Staging
    from bollhav.model.state import State
    from bollhav.model.write_modes import WriteMode
    from bollhav.postgres.columns import PostgresColumn, PostgresType

    model = MagicMock()
    model.state = State() if (staged or with_state) else None
    model.batching = MagicMock()
    model.target.name = "orders"
    model.target.name_resolved = "orders"
    model.target.full_name = "public.orders"
    model.target.schema.resolved = "public"
    model.target.write_mode = write_mode or WriteMode.APPEND
    model.target.staging = staging_cfg or (Staging() if staged else None)
    # `stage` is the dispatch switch (Target derives it from `staging`).
    # `is_view` keys the view guard. Pin both — a MagicMock attribute is
    # truthy by default, which would misroute every call.
    model.target.stage = staged
    model.is_view = is_view
    model.target.columns = [
        PostgresColumn(name="id", data_type=PostgresType.BIGINT),
        PostgresColumn(name="amount", data_type=PostgresType.NUMERIC),
    ]
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


class TestStagedPathEndToEnd:
    """Drive `write()` with staged=True against the real (mocked)
    `_write_staged` machinery and assert the rows were COPYd into the
    staging table. The table lifecycle (create / apply / drop) is owned
    by `@execute_lifecycle`, not `write()`, so it's out of scope here."""

    def test_streams_each_chunk_into_staging(self) -> None:
        from bollhav.postgres.write_modes import write

        model = _model(staged=True)
        df_gen = _gen(
            pl.DataFrame({"id": [1, 2], "amount": [1.0, 2.0]}),
            pl.DataFrame({"id": [3], "amount": [3.0]}),
        )
        conn = _mock_conn()

        write(conn, model, df_gen, since=SINCE, until=UNTIL)

        # Two COPY contexts on the cursor — one per chunk. `_write_staged`
        # only lands rows; no CREATE / INSERT-into-target / DROP here.
        assert conn.cursor.return_value.copy.call_count == 2
        executed = [str(call.args[0]) for call in conn.execute.call_args_list]
        assert not any("INSERT INTO" in q and "SELECT" in q for q in executed)
        assert not any("DROP TABLE" in q for q in executed)


# ── view + missing-df guards (regression coverage on the dispatcher) ──


class TestViewMode:
    def test_view_is_rejected(self) -> None:
        """A VIEW has nothing to write — `write()` refuses it. Views are
        created by `@model_lifecycle` (PostgresData.create_or_replace_view),
        identified via `model.is_view`, not a write mode."""
        from bollhav.postgres.write_modes import write

        model = _model(is_view=True)
        df_gen = _gen(pl.DataFrame({"id": [1], "amount": [1.0]}))
        with pytest.raises(ValueError, match="views"):
            write(_mock_conn(), model, df_gen)


class TestDfGenMissing:
    def test_append_needs_df_gen(self) -> None:
        from bollhav.postgres.write_modes import write

        model = _model(staged=False)
        with pytest.raises(ValueError, match="need a dataframe"):
            write(_mock_conn(), model, df_gen=None)
