"""Tests for the cross-pipeline model library — `bollhav.postgres.library`.

Postgres connection is mocked. Covers:
  * `ensure_library` issues the schema + table DDL
  * `register` upserts the right tuple — TABLE writes state pointers,
    VIEW and library-only TABLE write NULL state pointers
  * `lookup` returns the registered row (or None) as a LibraryEntry
  * `is_satisfied` — state-tracked tables check the applied row;
    VIEW / library-only entries are satisfied by mere presence
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest


SINCE = datetime(2024, 1, 1, tzinfo=timezone.utc)
UNTIL = datetime(2024, 1, 2, tzinfo=timezone.utc)


def _mock_conn(fetchone_value=None, rowcount=0):
    conn = MagicMock()
    conn.transaction.return_value.__enter__ = MagicMock(return_value=None)
    conn.transaction.return_value.__exit__ = MagicMock(return_value=None)
    result = MagicMock()
    result.fetchone.return_value = fetchone_value
    conn.execute.return_value = result
    return conn


def _model(
    *,
    full_name="warehouse.orders",
    upstream=None,
    is_view=False,
    has_staging=True,
):
    """Build a mock Model. `is_view=True` flips the target to a view
    (no staging, no state); `has_staging=False` produces a plain
    library-only table (no state pointers written by register)."""
    from bollhav.model.staging import Staging
    from bollhav.model.state import State

    model = MagicMock()
    model.target.name = full_name.split(".")[-1]
    model.target.full_name = full_name
    model.target.schema.resolved = full_name.split(".")[0]
    model.target.is_view = is_view
    if is_view or not has_staging:
        model.target.staging = None
        model.state = None
    else:
        model.target.staging = Staging()
        model.state = State()
    model.upstream = upstream if upstream is not None else []
    return model


# ── DDL ──────────────────────────────────────────────────────────────


class TestEnsureLibrary:
    def test_creates_schema_and_table(self) -> None:
        from bollhav.postgres.library import ensure_library

        conn = _mock_conn()
        ensure_library(conn)

        executed = [str(call.args[0]) for call in conn.execute.call_args_list]
        assert any("CREATE SCHEMA IF NOT EXISTS" in q for q in executed)
        assert any(
            "CREATE TABLE IF NOT EXISTS" in q and "z_bollhav" in q for q in executed
        )
        assert any("model_library" in q for q in executed)


# ── register / lookup ────────────────────────────────────────────────


class TestRegister:
    """`register` upserts a row keyed by full_name. Params are
    positional: (full_name, upstream[], model_type, state_schema,
    state_table)."""

    def test_table_writes_state_pointers(self) -> None:
        from bollhav.postgres.library import register

        m = _model(full_name="warehouse.orders", upstream=["raw.orders"])
        conn = _mock_conn()
        register(conn, m)

        insert_calls = [
            c for c in conn.execute.call_args_list if "INSERT INTO" in str(c.args[0])
        ]
        assert len(insert_calls) == 1
        params = insert_calls[0].args[1]
        assert params[0] == "warehouse.orders"
        assert params[1] == ["raw.orders"]
        assert params[2] == "TABLE"
        assert params[3] == "z_warehouse"
        assert params[4] == "orders_state"

    def test_view_writes_null_state_pointers(self) -> None:
        """Views have no state table — register stores NULLs so the
        satisfaction check distinguishes presence-based VIEW upstreams
        from state-tracked TABLE upstreams."""
        from bollhav.postgres.library import register

        m = _model(full_name="warehouse.v_orders", is_view=True)
        conn = _mock_conn()
        register(conn, m)

        params = next(
            c.args[1]
            for c in conn.execute.call_args_list
            if "INSERT INTO" in str(c.args[0])
        )
        assert params[2] == "VIEW"
        assert params[3] is None
        assert params[4] is None

    def test_library_only_table_writes_null_state_pointers(self) -> None:
        """A TABLE that opted in to library registration without
        state tracking (`library=True`, no state, no staging) also
        writes NULL state pointers — presence in the library is the
        satisfaction proof."""
        from bollhav.postgres.library import register

        m = _model(full_name="lookup.countries", has_staging=False)
        conn = _mock_conn()
        register(conn, m)

        params = next(
            c.args[1]
            for c in conn.execute.call_args_list
            if "INSERT INTO" in str(c.args[0])
        )
        assert params[2] == "TABLE"
        assert params[3] is None
        assert params[4] is None

    def test_empty_upstream_list(self) -> None:
        from bollhav.postgres.library import register

        m = _model(upstream=[])
        conn = _mock_conn()
        register(conn, m)

        params = next(
            c.args[1]
            for c in conn.execute.call_args_list
            if "INSERT INTO" in str(c.args[0])
        )
        assert params[1] == []


class TestLookup:
    def test_returns_none_when_not_registered(self) -> None:
        from bollhav.postgres.library import lookup

        conn = _mock_conn(fetchone_value=None)
        assert lookup(conn, "missing.model") is None

    def test_returns_entry_for_state_tracked_table(self) -> None:
        from bollhav.postgres.library import LibraryEntry, lookup

        conn = _mock_conn(
            fetchone_value=(["a.b"], "TABLE", "z_warehouse", "orders_state")
        )
        result = lookup(conn, "warehouse.orders")
        assert isinstance(result, LibraryEntry)
        assert result.upstream == ["a.b"]
        assert result.model_type == "TABLE"
        assert result.state_schema == "z_warehouse"
        assert result.state_table == "orders_state"

    def test_returns_entry_for_view_with_null_state_pointers(self) -> None:
        from bollhav.postgres.library import lookup

        conn = _mock_conn(fetchone_value=([], "VIEW", None, None))
        result = lookup(conn, "warehouse.v_orders")
        assert result.model_type == "VIEW"
        assert result.state_schema is None
        assert result.state_table is None


# ── is_satisfied ─────────────────────────────────────────────────────


def _entry(*, model_type="TABLE", state_schema=None, state_table=None):
    from bollhav.postgres.library import LibraryEntry

    return LibraryEntry(
        upstream=[],
        model_type=model_type,
        state_schema=state_schema,
        state_table=state_table,
    )


class TestIsSatisfied:
    def test_view_entry_satisfied_by_presence(self) -> None:
        """VIEW entries store NULL state pointers — they're satisfied
        by mere presence in the library, no SQL query needed."""
        from bollhav.postgres.library import is_satisfied

        conn = _mock_conn()
        entry = _entry(model_type="VIEW")
        assert is_satisfied(conn, entry=entry, since=SINCE, until=UNTIL)
        # No SQL was issued — short-circuit at the entry level.
        assert conn.execute.call_count == 0

    def test_library_only_table_satisfied_by_presence(self) -> None:
        """A TABLE registered without state pointers (library-only
        opt-in) is also satisfied by presence — same NULL-state-cols
        rule as views, irrespective of `model_type`."""
        from bollhav.postgres.library import is_satisfied

        conn = _mock_conn()
        entry = _entry(model_type="TABLE", state_schema=None, state_table=None)
        assert is_satisfied(conn, entry=entry, since=SINCE, until=UNTIL)
        assert conn.execute.call_count == 0

    def test_state_tracked_table_exact_match_satisfies(self) -> None:
        from bollhav.postgres.library import is_satisfied

        conn = _mock_conn(fetchone_value=(1,))
        entry = _entry(state_schema="z_raw", state_table="orders_state")
        assert is_satisfied(conn, entry=entry, since=SINCE, until=UNTIL)

    def test_state_tracked_table_no_applied_row_not_satisfied(self) -> None:
        from bollhav.postgres.library import is_satisfied

        conn = _mock_conn(fetchone_value=None)
        entry = _entry(state_schema="z_raw", state_table="orders_state")
        assert not is_satisfied(conn, entry=entry, since=SINCE, until=UNTIL)

    def test_missing_state_table_returns_false_not_error(self) -> None:
        """Library row outliving its state table (cleanup, restored
        backup, etc.) shouldn't crash. pg_tables existence check
        returns no row → False, downstream blocks rather than errors."""
        from bollhav.postgres.library import is_satisfied

        conn = MagicMock()
        conn.transaction.return_value.__enter__ = MagicMock(return_value=None)
        conn.transaction.return_value.__exit__ = MagicMock(return_value=None)
        exists_result = MagicMock()
        exists_result.fetchone.return_value = None
        conn.execute.return_value = exists_result

        entry = _entry(state_schema="z_raw", state_table="orders_state")
        assert not is_satisfied(conn, entry=entry, since=SINCE, until=UNTIL)
        assert conn.execute.call_count == 1
        assert "pg_tables" in str(conn.execute.call_args_list[0].args[0])

    def test_query_uses_encapsulation_predicate(self) -> None:
        """Sanity-check: `since <= my_since AND until >= my_until`
        is what makes a daily-upstream applied row cover an hourly
        downstream interval."""
        from bollhav.postgres.library import is_satisfied

        conn = _mock_conn(fetchone_value=(1,))
        entry = _entry(state_schema="z_raw", state_table="orders_state")
        is_satisfied(conn, entry=entry, since=SINCE, until=UNTIL)

        select_q = next(
            str(c.args[0])
            for c in conn.execute.call_args_list
            if "status = 'applied'" in str(c.args[0])
        )
        assert "since <= %s" in select_q
        assert "until >= %s" in select_q
        params = next(
            c.args[1]
            for c in conn.execute.call_args_list
            if "status = 'applied'" in str(c.args[0])
        )
        assert params == [SINCE, UNTIL]


# ── bootstrap blocked path ───────────────────────────────────────────


class TestBootstrapBlockedPath:
    """The library + state operate together during the bootstrap.
    These tests verify the 'when an out-of-pipeline upstream isn't
    satisfied, the interval gets marked blocked' logic."""

    def _staged_model(self, *, upstream, contract):
        from bollhav.model.staging import Staging

        model = MagicMock()
        model.target.is_view = False
        model.library = False
        model.target.staging = Staging()
        model.target.full_name = "warehouse.enriched"
        model.upstream = list(upstream)
        model.intervals = list(contract)
        return model

    def _conn_ctx(self):
        conn = MagicMock()
        conn.__enter__ = MagicMock(return_value=conn)
        conn.__exit__ = MagicMock(return_value=None)
        return conn

    def test_unregistered_upstream_blocks_interval(self) -> None:
        from unittest.mock import patch

        from bollhav.model.intervals import TZInterval
        from bollhav.model.load_models import _bootstrap_state_for_staged_models
        from bollhav.model.state import StateMode

        m = self._staged_model(
            upstream=["raw.orders"], contract=[TZInterval(SINCE, UNTIL)]
        )
        conn = self._conn_ctx()
        with (
            patch("bollhav.postgres.state._connect", return_value=conn),
            patch("bollhav.postgres.library.ensure_library"),
            patch("bollhav.postgres.library.register"),
            patch("bollhav.postgres.library.lookup", return_value=None),
            patch("bollhav.postgres.state.ensure_tables"),
            patch("bollhav.postgres.state.prefill") as pf,
            patch("bollhav.postgres.state.read_actionable", return_value=[]),
        ):
            _bootstrap_state_for_staged_models([m], state_mode=StateMode.DISCOVER)

        rows = pf.call_args.kwargs["intervals"]
        assert len(rows) == 1
        interval, status, reason = rows[0]
        assert status == "blocked"
        assert reason.startswith("STATE_001:")
        assert "raw.orders" in reason
        assert "not registered" in reason

    def test_unsatisfied_upstream_blocks_with_window_in_reason(self) -> None:
        from unittest.mock import patch

        from bollhav.model.intervals import TZInterval
        from bollhav.model.load_models import _bootstrap_state_for_staged_models
        from bollhav.model.state import StateMode

        m = self._staged_model(
            upstream=["raw.orders"], contract=[TZInterval(SINCE, UNTIL)]
        )
        conn = self._conn_ctx()
        with (
            patch("bollhav.postgres.state._connect", return_value=conn),
            patch("bollhav.postgres.library.ensure_library"),
            patch("bollhav.postgres.library.register"),
            patch(
                "bollhav.postgres.library.lookup",
                return_value=_entry(state_schema="z_raw", state_table="orders_state"),
            ),
            patch("bollhav.postgres.library.is_satisfied", return_value=False),
            patch("bollhav.postgres.state.ensure_tables"),
            patch("bollhav.postgres.state.prefill") as pf,
            patch("bollhav.postgres.state.read_actionable", return_value=[]),
        ):
            _bootstrap_state_for_staged_models([m], state_mode=StateMode.DISCOVER)

        _, status, reason = pf.call_args.kwargs["intervals"][0]
        assert status == "blocked"
        assert reason.startswith("STATE_002:")
        assert "raw.orders" in reason
        assert SINCE.isoformat() in reason
        assert UNTIL.isoformat() in reason

    def test_satisfied_upstream_keeps_pending(self) -> None:
        from unittest.mock import patch

        from bollhav.model.intervals import TZInterval
        from bollhav.model.load_models import _bootstrap_state_for_staged_models
        from bollhav.model.state import StateMode

        m = self._staged_model(
            upstream=["raw.orders"], contract=[TZInterval(SINCE, UNTIL)]
        )
        conn = self._conn_ctx()
        with (
            patch("bollhav.postgres.state._connect", return_value=conn),
            patch("bollhav.postgres.library.ensure_library"),
            patch("bollhav.postgres.library.register"),
            patch(
                "bollhav.postgres.library.lookup",
                return_value=_entry(state_schema="z_raw", state_table="orders_state"),
            ),
            patch("bollhav.postgres.library.is_satisfied", return_value=True),
            patch("bollhav.postgres.state.ensure_tables"),
            patch("bollhav.postgres.state.prefill") as pf,
            patch("bollhav.postgres.state.read_actionable", return_value=[]),
        ):
            _bootstrap_state_for_staged_models([m], state_mode=StateMode.DISCOVER)

        _, status, reason = pf.call_args.kwargs["intervals"][0]
        assert status == "pending"
        assert reason is None

    def test_in_pipeline_upstream_is_not_checked(self) -> None:
        """Topological ordering handles in-pipeline upstreams; the
        bootstrap shouldn't query the library for them."""
        from unittest.mock import patch

        from bollhav.model.intervals import TZInterval
        from bollhav.model.load_models import _bootstrap_state_for_staged_models
        from bollhav.model.state import StateMode
        from bollhav.model.staging import Staging

        downstream = self._staged_model(
            upstream=["warehouse.upstream"],
            contract=[TZInterval(SINCE, UNTIL)],
        )
        upstream = MagicMock()
        upstream.target.is_view = False
        upstream.library = False
        upstream.target.staging = Staging()
        upstream.target.full_name = "warehouse.upstream"
        upstream.upstream = []
        upstream.intervals = []

        conn = self._conn_ctx()
        with (
            patch("bollhav.postgres.state._connect", return_value=conn),
            patch("bollhav.postgres.library.ensure_library"),
            patch("bollhav.postgres.library.register"),
            patch("bollhav.postgres.library.lookup") as lookup_mock,
            patch("bollhav.postgres.state.ensure_tables"),
            patch("bollhav.postgres.state.prefill") as pf,
            patch("bollhav.postgres.state.read_actionable", return_value=[]),
        ):
            _bootstrap_state_for_staged_models(
                [downstream, upstream], state_mode=StateMode.DISCOVER
            )

        lookup_mock.assert_not_called()
        _, status, reason = pf.call_args_list[0].kwargs["intervals"][0]
        assert status == "pending"
        assert reason is None


# ── library-only registration paths ──────────────────────────────────


class TestLibraryOnlyRegistration:
    """Models that go through the register-only path (views and
    `library=True` tables without state) skip the state-table ensure
    + prefill but still upsert a library row."""

    def _conn_ctx(self):
        conn = MagicMock()
        conn.__enter__ = MagicMock(return_value=conn)
        conn.__exit__ = MagicMock(return_value=None)
        return conn

    def test_view_with_library_true_registers_without_state_machinery(self) -> None:
        from unittest.mock import patch

        from bollhav.model.load_models import _bootstrap_state_for_staged_models
        from bollhav.model.state import StateMode

        m = MagicMock()
        m.target.is_view = True
        m.library = True
        m.target.staging = None
        m.target.full_name = "warehouse.v_orders"
        m.upstream = []

        conn = self._conn_ctx()
        with (
            patch("bollhav.postgres.state._connect", return_value=conn),
            patch("bollhav.postgres.library.ensure_library") as el,
            patch("bollhav.postgres.library.register") as reg,
            patch("bollhav.postgres.state.ensure_tables") as et,
            patch("bollhav.postgres.state.prefill") as pf,
        ):
            _bootstrap_state_for_staged_models([m], state_mode=StateMode.DISCOVER)

        el.assert_called_once()
        reg.assert_called_once()
        et.assert_not_called()
        pf.assert_not_called()

    def test_view_without_library_true_does_not_register(self) -> None:
        """A VIEW without `library=True` is a perfectly valid bollhav
        model (the CREATE OR REPLACE VIEW still runs in the user's
        execute), but it's not claimable as upstream — no library row
        is upserted."""
        from unittest.mock import patch

        from bollhav.model.load_models import _bootstrap_state_for_staged_models
        from bollhav.model.state import StateMode

        m = MagicMock()
        m.target.is_view = True
        m.library = False
        m.target.staging = None
        m.target.full_name = "warehouse.v_orders"
        m.upstream = []

        conn = self._conn_ctx()
        with (
            patch("bollhav.postgres.state._connect", return_value=conn),
            patch("bollhav.postgres.library.ensure_library") as el,
            patch("bollhav.postgres.library.register") as reg,
            patch("bollhav.postgres.state.ensure_tables") as et,
            patch("bollhav.postgres.state.prefill") as pf,
        ):
            _bootstrap_state_for_staged_models([m], state_mode=StateMode.DISCOVER)

        el.assert_not_called()
        reg.assert_not_called()
        et.assert_not_called()
        pf.assert_not_called()

    def test_library_true_table_registers_without_state_machinery(self) -> None:
        from unittest.mock import patch

        from bollhav.model.load_models import _bootstrap_state_for_staged_models
        from bollhav.model.state import StateMode

        m = MagicMock()
        m.target.is_view = False
        m.library = True
        m.target.staging = None
        m.target.full_name = "lookup.countries"
        m.upstream = []

        conn = self._conn_ctx()
        with (
            patch("bollhav.postgres.state._connect", return_value=conn),
            patch("bollhav.postgres.library.ensure_library") as el,
            patch("bollhav.postgres.library.register") as reg,
            patch("bollhav.postgres.state.ensure_tables") as et,
            patch("bollhav.postgres.state.prefill") as pf,
        ):
            _bootstrap_state_for_staged_models([m], state_mode=StateMode.DISCOVER)

        el.assert_called_once()
        reg.assert_called_once()
        et.assert_not_called()
        pf.assert_not_called()


# ── prefill row normalization ────────────────────────────────────────


class TestPrefillRowNormalization:
    def test_bare_TZInterval_is_pending(self) -> None:
        from bollhav.model.intervals import TZInterval
        from bollhav.postgres.state import _normalize_prefill_row

        iv = TZInterval(SINCE, UNTIL)
        assert _normalize_prefill_row(iv) == (iv, "pending", None)

    def test_pending_tuple_clears_reason(self) -> None:
        from bollhav.model.intervals import TZInterval
        from bollhav.postgres.state import _normalize_prefill_row

        iv = TZInterval(SINCE, UNTIL)
        assert _normalize_prefill_row((iv, "pending", "ignored")) == (
            iv,
            "pending",
            None,
        )

    def test_blocked_requires_reason(self) -> None:
        from bollhav.model.intervals import TZInterval
        from bollhav.postgres.state import _normalize_prefill_row

        iv = TZInterval(SINCE, UNTIL)
        with pytest.raises(ValueError, match="blocked rows require"):
            _normalize_prefill_row((iv, "blocked", None))

    def test_unknown_status_raises(self) -> None:
        from bollhav.model.intervals import TZInterval
        from bollhav.postgres.state import _normalize_prefill_row

        iv = TZInterval(SINCE, UNTIL)
        with pytest.raises(ValueError, match="must be 'pending' or 'blocked'"):
            _normalize_prefill_row((iv, "weird", None))
