"""Tests for the cross-pipeline model library — `bollhav.postgres.library`.

Postgres connection is mocked. Covers:
  * `ensure_library` issues the schema + table DDL
  * `register` upserts the right tuple
  * `lookup` returns the registered row (or None)
  * `is_satisfied` accepts both exact-match and encapsulating upstream
    rows (per the design — supports cross-cadence upstreams like a
    daily upstream covering an hourly downstream's interval)
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


def _model(*, full_name="warehouse.orders", upstream=None):
    from bollhav.model.staging import Staging
    from bollhav.model.state import State

    model = MagicMock()
    model.target.name = full_name.split(".")[-1]
    model.target.full_name = full_name
    model.target.schema.resolved = full_name.split(".")[0]
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
    def test_upserts_with_upstream_and_state_pointers(self) -> None:
        from bollhav.postgres.library import register

        m = _model(full_name="warehouse.orders", upstream=["raw.orders"])
        conn = _mock_conn()
        register(conn, m)

        # The INSERT happened once.
        insert_calls = [
            c for c in conn.execute.call_args_list if "INSERT INTO" in str(c.args[0])
        ]
        assert len(insert_calls) == 1
        params = insert_calls[0].args[1]
        assert params[0] == "warehouse.orders"
        assert params[1] == ["raw.orders"]
        assert params[2] == "z_warehouse"  # state schema (with z_ prefix)
        assert params[3] == "orders_state"

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

    def test_returns_upstream_and_state_pointers(self) -> None:
        from bollhav.postgres.library import lookup

        conn = _mock_conn(fetchone_value=(["a.b"], "z_warehouse", "orders_state"))
        result = lookup(conn, "warehouse.orders")
        assert result == (["a.b"], "z_warehouse", "orders_state")


# ── is_satisfied — encapsulation semantics ───────────────────────────


class TestIsSatisfied:
    def test_exact_match_satisfies(self) -> None:
        from bollhav.postgres.library import is_satisfied

        conn = _mock_conn(fetchone_value=(1,))
        assert is_satisfied(
            conn,
            upstream_state_schema="z_raw",
            upstream_state_table="orders_state",
            since=SINCE,
            until=UNTIL,
        )

    def test_no_applied_row_means_not_satisfied(self) -> None:
        from bollhav.postgres.library import is_satisfied

        conn = _mock_conn(fetchone_value=None)
        assert not is_satisfied(
            conn,
            upstream_state_schema="z_raw",
            upstream_state_table="orders_state",
            since=SINCE,
            until=UNTIL,
        )

    def test_missing_state_table_returns_false_not_error(self) -> None:
        """Library row outliving its state table (cleanup, restored
        backup, etc.) shouldn't crash a downstream's bootstrap. The
        pg_tables existence check returns no row → is_satisfied
        returns False, downstream gets blocked rather than erroring."""
        from bollhav.postgres.library import is_satisfied

        conn = MagicMock()
        conn.transaction.return_value.__enter__ = MagicMock(return_value=None)
        conn.transaction.return_value.__exit__ = MagicMock(return_value=None)
        # First execute (pg_tables) returns None — table absent.
        exists_result = MagicMock()
        exists_result.fetchone.return_value = None
        conn.execute.return_value = exists_result

        assert not is_satisfied(
            conn,
            upstream_state_schema="z_raw",
            upstream_state_table="orders_state",
            since=SINCE,
            until=UNTIL,
        )
        # Only ONE query issued — we short-circuited before the real check.
        assert conn.execute.call_count == 1
        assert "pg_tables" in str(conn.execute.call_args_list[0].args[0])

    def test_query_uses_encapsulation_predicate(self) -> None:
        """Sanity-check the SQL: `since <= my_since AND until >= my_until`
        is what makes a daily-upstream applied row cover an hourly
        downstream interval."""
        from bollhav.postgres.library import is_satisfied

        conn = _mock_conn(fetchone_value=(1,))
        is_satisfied(
            conn,
            upstream_state_schema="z_raw",
            upstream_state_table="orders_state",
            since=SINCE,
            until=UNTIL,
        )

        # is_satisfied issues two queries: first a pg_tables existence
        # check, then the real encapsulation check. Find the latter.
        select_q = next(
            str(c.args[0])
            for c in conn.execute.call_args_list
            if "status = 'applied'" in str(c.args[0])
        )
        assert "since <= %s" in select_q
        assert "until >= %s" in select_q
        # Params should be (my_since, my_until) — the downstream window.
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
            patch("bollhav.postgres.state.read_pending", return_value=[]),
        ):
            _bootstrap_state_for_staged_models([m], state_mode=StateMode.RESPECT)

        rows = pf.call_args.kwargs["intervals"]
        assert len(rows) == 1
        interval, status, reason = rows[0]
        assert status == "blocked"
        # Code-prefixed for grep/lookup.
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
                return_value=(["upstream-of-upstream"], "z_raw", "orders_state"),
            ),
            patch("bollhav.postgres.library.is_satisfied", return_value=False),
            patch("bollhav.postgres.state.ensure_tables"),
            patch("bollhav.postgres.state.prefill") as pf,
            patch("bollhav.postgres.state.read_pending", return_value=[]),
        ):
            _bootstrap_state_for_staged_models([m], state_mode=StateMode.RESPECT)

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
                return_value=([], "z_raw", "orders_state"),
            ),
            patch("bollhav.postgres.library.is_satisfied", return_value=True),
            patch("bollhav.postgres.state.ensure_tables"),
            patch("bollhav.postgres.state.prefill") as pf,
            patch("bollhav.postgres.state.read_pending", return_value=[]),
        ):
            _bootstrap_state_for_staged_models([m], state_mode=StateMode.RESPECT)

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
            patch("bollhav.postgres.state.read_pending", return_value=[]),
        ):
            _bootstrap_state_for_staged_models(
                [downstream, upstream], state_mode=StateMode.RESPECT
            )

        # No lookup happened for downstream's upstream (it's in pipeline).
        lookup_mock.assert_not_called()
        # Downstream's row is pending — upstream is trusted to run via topo order.
        _, status, reason = pf.call_args_list[0].kwargs["intervals"][0]
        assert status == "pending"
        assert reason is None


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
        # Even if a reason is passed, pending rows drop it.
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
