"""Tests for the cross-pipeline model library.

After the refactor the library lives INSIDE `bollhav.postgres.state` —
there is no separate `bollhav.postgres.library` module. The free
functions became methods/staticmethods on `PostgresState`:

  * `ensure_library`        — issues the schema + table DDL
  * `register_model`        — upserts the row (state-tracked TABLE writes
    state pointers; VIEW / library-only TABLE write NULL state pointers)
  * `lookup_model` (static) — returns the registered row as a LibraryEntry
  * `is_satisfied` (static) — state-tracked tables check the applied row;
    VIEW / library-only entries are satisfied by mere presence

`LibraryEntry` now carries a `kind` field (`interval` | `oneshot` |
`view`). Postgres connection is mocked throughout.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from bollhav.model.intervals import TZInterval


SINCE = datetime(2024, 1, 1, tzinfo=timezone.utc)
UNTIL = datetime(2024, 1, 2, tzinfo=timezone.utc)
INTERVAL = TZInterval(SINCE, UNTIL)


def _mock_conn(fetchone_value=None):
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
    from bollhav.model.temporality import Temporality
    from bollhav.model.staging import Staging
    from bollhav.model.state import State

    model = MagicMock()
    model.target.name = full_name.split(".")[-1]
    model.target.full_name = full_name
    # no suffix in these mocks → canonical identity equals full_name
    model.target.canonical_full_name = full_name
    model.target.schema_resolved = full_name.split(".")[0]
    model.target.schema_suffix = ""
    model.target.schema_suffix_appendix = None
    # `register_model` keys off the Model-level `is_view` flag and the
    # `kind` enum (it writes `'VIEW' if model.is_view else 'TABLE'` into
    # model_type and `model.temporality.value` into kind).
    model.is_view = is_view
    upstream_list = list(upstream) if upstream is not None else []
    # Each declared name becomes a gated upstream (a SourceModel + contract);
    # `register_model` reads `upstream_names` and the live gating loop reads
    # `gated_upstreams`. Pin both on the mock.
    from bollhav.model.source import Source, SourceModel
    from bollhav.model.upstream import UpstreamContract

    gated = [
        Source(n, type=SourceModel(), contract=UpstreamContract.ENCAPSULATE)
        for n in upstream_list
    ]
    model.upstream = gated
    model.upstream_names = upstream_list
    model.gated_upstreams = gated
    if is_view:
        model.target.staging = None
        model.state = None
        model.temporality = Temporality.TIMELESS
    elif not has_staging:
        model.target.staging = None
        model.state = None
        model.temporality = Temporality.TEMPORAL
    else:
        model.target.staging = Staging()
        model.state = State()
        model.temporality = Temporality.TEMPORAL
    return model


# ── DDL ──────────────────────────────────────────────────────────────


class TestEnsureLibrary:
    def test_creates_schema_and_table(self) -> None:
        from bollhav.postgres.state import PostgresState

        conn = _mock_conn()
        PostgresState(_model(), conn).ensure_library()

        executed = [str(call.args[0]) for call in conn.execute.call_args_list]
        assert any("CREATE SCHEMA IF NOT EXISTS" in q for q in executed)
        assert any(
            "CREATE TABLE IF NOT EXISTS" in q and "z_bollhav" in q for q in executed
        )
        assert any("'library'" in q for q in executed)


# ── register / lookup ────────────────────────────────────────────────


class TestRegister:
    """`register_model` upserts a row keyed by full_name. Params are
    positional: (full_name, upstream[], model_type, state_schema,
    state_table, kind)."""

    def test_table_writes_state_pointers(self) -> None:
        from bollhav.postgres.state import (
            LIBRARY_SCHEMA,
            PostgresState,
            state_table_name,
        )

        m = _model(full_name="warehouse.orders", upstream=["raw.orders"])
        conn = _mock_conn()
        PostgresState(m, conn).register_model()

        insert_calls = [
            c for c in conn.execute.call_args_list if "INSERT INTO" in str(c.args[0])
        ]
        assert len(insert_calls) == 1
        params = insert_calls[0].args[1]
        assert params[0] == "warehouse.orders"
        assert params[1] == ["raw.orders"]
        assert params[2] == "TABLE"
        # state pointers now point at the central schema + deterministic name
        assert params[3] == LIBRARY_SCHEMA
        assert params[4] == state_table_name("warehouse.orders")
        assert params[5] == "temporal"

    def test_view_writes_null_state_pointers(self) -> None:
        """Views have no state table — register stores NULLs so the
        satisfaction check distinguishes presence-based VIEW upstreams
        from state-tracked TABLE upstreams."""
        from bollhav.postgres.state import PostgresState

        m = _model(full_name="warehouse.v_orders", is_view=True)
        conn = _mock_conn()
        PostgresState(m, conn).register_model()

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
        from bollhav.postgres.state import PostgresState

        m = _model(full_name="lookup.countries", has_staging=False)
        conn = _mock_conn()
        PostgresState(m, conn).register_model()

        params = next(
            c.args[1]
            for c in conn.execute.call_args_list
            if "INSERT INTO" in str(c.args[0])
        )
        assert params[2] == "TABLE"
        assert params[3] is None
        assert params[4] is None

    def test_empty_upstream_list(self) -> None:
        from bollhav.postgres.state import PostgresState

        m = _model(upstream=[])
        conn = _mock_conn()
        PostgresState(m, conn).register_model()

        params = next(
            c.args[1]
            for c in conn.execute.call_args_list
            if "INSERT INTO" in str(c.args[0])
        )
        assert params[1] == []


class TestLookup:
    def test_returns_none_when_not_registered(self) -> None:
        from bollhav.postgres.state import PostgresState

        conn = _mock_conn(fetchone_value=None)
        assert PostgresState.lookup_model(conn, "missing.model") is None

    def test_returns_entry_for_state_tracked_table(self) -> None:
        from bollhav.postgres.state import LibraryEntry, PostgresState

        conn = _mock_conn(
            fetchone_value=(
                ["a.b"],
                "TABLE",
                "z_warehouse",
                "orders_state",
                "temporal",
                True,
                [{"name": "raw.landing", "kind": "database"}],
            )
        )
        result = PostgresState.lookup_model(conn, "warehouse.orders")
        assert isinstance(result, LibraryEntry)
        assert result.upstream == ["a.b"]
        assert result.model_type == "TABLE"
        assert result.state_schema == "z_warehouse"
        assert result.state_table == "orders_state"
        assert result.temporality == "temporal"
        assert result.fixed_intervals is True
        assert result.sources == [{"name": "raw.landing", "kind": "database"}]

    def test_returns_entry_for_view_with_null_state_pointers(self) -> None:
        from bollhav.postgres.state import PostgresState

        conn = _mock_conn(fetchone_value=([], "VIEW", None, None, "timeless", True, []))
        result = PostgresState.lookup_model(conn, "warehouse.v_orders")
        assert result.model_type == "VIEW"
        assert result.state_schema is None
        assert result.state_table is None
        assert result.sources == []
        assert result.temporality == "timeless"
        assert result.fixed_intervals is True


# ── is_satisfied ─────────────────────────────────────────────────────


def _entry(
    *, model_type="TABLE", state_schema=None, state_table=None, temporality="temporal"
):
    from bollhav.postgres.state import LibraryEntry

    return LibraryEntry(
        upstream=[],
        model_type=model_type,
        state_schema=state_schema,
        state_table=state_table,
        temporality=temporality,
    )


class TestIsSatisfied:
    def test_view_entry_satisfied_by_presence(self) -> None:
        """VIEW entries store NULL state pointers — they're satisfied
        by mere presence in the library, no SQL query needed."""
        from bollhav.postgres.state import PostgresState

        conn = _mock_conn()
        entry = _entry(model_type="VIEW", temporality="timeless")
        assert PostgresState.is_satisfied(conn, entry=entry, interval=INTERVAL)
        # No SQL was issued — short-circuit at the entry level.
        assert conn.execute.call_count == 0

    def test_library_only_table_satisfied_by_presence(self) -> None:
        """A TABLE registered without state pointers (library-only
        opt-in) is also satisfied by presence — same NULL-state-cols
        rule as views, irrespective of `model_type`."""
        from bollhav.postgres.state import PostgresState

        conn = _mock_conn()
        entry = _entry(model_type="TABLE", state_schema=None, state_table=None)
        assert PostgresState.is_satisfied(conn, entry=entry, interval=INTERVAL)
        assert conn.execute.call_count == 0

    def test_state_tracked_table_exact_match_satisfies(self) -> None:
        from bollhav.postgres.state import PostgresState

        conn = _mock_conn(fetchone_value=(1,))
        entry = _entry(state_schema="z_raw", state_table="orders_state")
        assert PostgresState.is_satisfied(conn, entry=entry, interval=INTERVAL)

    def test_state_tracked_table_no_applied_row_not_satisfied(self) -> None:
        from bollhav.postgres.state import PostgresState

        # pg_tables existence check returns a row; then the ENCAPSULATE level
        # (temporal fallback) runs two queries — the single-row fast path
        # (None → no container) and the union-coverage fallback (no rows → NULL
        # bounds) → not satisfied.
        conn = MagicMock()
        results = [MagicMock(), MagicMock(), MagicMock()]
        results[0].fetchone.return_value = (1,)  # pg_tables → exists
        results[1].fetchone.return_value = None  # fast-path container → none
        results[2].fetchone.return_value = (None,)  # union coverage → NULL → false
        conn.execute.side_effect = results

        entry = _entry(state_schema="z_raw", state_table="orders_state")
        assert not PostgresState.is_satisfied(conn, entry=entry, interval=INTERVAL)

    def test_missing_state_table_returns_false_not_error(self) -> None:
        """Library row outliving its state table (cleanup, restored
        backup, etc.) shouldn't crash. pg_tables existence check
        returns no row → False, downstream blocks rather than errors."""
        from bollhav.postgres.state import PostgresState

        conn = _mock_conn(fetchone_value=None)

        entry = _entry(state_schema="z_raw", state_table="orders_state")
        assert not PostgresState.is_satisfied(conn, entry=entry, interval=INTERVAL)
        assert conn.execute.call_count == 1
        assert "pg_tables" in str(conn.execute.call_args_list[0].args[0])

    def test_query_uses_encapsulation_predicate(self) -> None:
        """Sanity-check: `since <= my_since AND until >= my_until`
        is what makes a daily-upstream applied row cover an hourly
        downstream interval."""
        from bollhav.postgres.state import PostgresState

        conn = _mock_conn(fetchone_value=(1,))
        entry = _entry(state_schema="z_raw", state_table="orders_state")
        PostgresState.is_satisfied(conn, entry=entry, interval=INTERVAL)

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


# ── live upstream check (runtime block path) ─────────────────────────
#
# The old bootstrap-blocked-path tests (`_connect` / `prefill` /
# `read_actionable` / `bollhav.postgres.library.*`) are gone: blocking
# now happens at run time in `@execute_lifecycle`, which calls
# `PostgresState.is_upstream_satisfied_live` and writes the verdict's
# `UpstreamCheck.reason` onto the state row. These tests preserve the
# original intent (unregistered upstream = documentation, not blocked;
# unsatisfied registered upstream = STATE_002 blocked; satisfied =
# clear) against that new API. The block window is no longer repeated in
# the reason text — it lives on the row's `since`/`until` — so the old
# "window in reason" assertions were dropped.


class TestUpstreamSatisfiedLive:
    def _state(self, model, conn):
        from bollhav.postgres.state import PostgresState

        return PostgresState(model=model, conn=conn)

    def test_unregistered_gated_upstream_raises(self) -> None:
        # A gated upstream is a hard demand: if it isn't registered (never
        # ran), that's an error, not a silent pass. (An ungated source would
        # never be checked here at all.)
        from unittest.mock import patch

        import pytest

        from bollhav.postgres.state import PostgresState

        m = _model(upstream=["raw.orders"])
        conn = _mock_conn()
        with patch.object(PostgresState, "lookup_model", return_value=None):
            with pytest.raises(ValueError, match="not registered"):
                self._state(m, conn).is_upstream_satisfied_live(INTERVAL)

    def test_unsatisfied_upstream_blocks_with_state_002(self) -> None:
        from unittest.mock import patch

        from bollhav.postgres.state import PostgresState

        m = _model(upstream=["raw.orders"])
        conn = _mock_conn()
        entry = _entry(state_schema="z_raw", state_table="orders_state")
        with (
            patch.object(PostgresState, "lookup_model", return_value=entry),
            patch.object(PostgresState, "is_satisfied", return_value=False),
        ):
            check = self._state(m, conn).is_upstream_satisfied_live(INTERVAL)

        assert not check.satisfied
        reason = check.reason
        assert reason is not None
        assert reason.startswith("STATE_002:")
        assert "raw.orders" in reason

    def test_satisfied_upstream_is_clear(self) -> None:
        from unittest.mock import patch

        from bollhav.postgres.state import PostgresState

        m = _model(upstream=["raw.orders"])
        conn = _mock_conn()
        entry = _entry(state_schema="z_raw", state_table="orders_state")
        with (
            patch.object(PostgresState, "lookup_model", return_value=entry),
            patch.object(PostgresState, "is_satisfied", return_value=True),
        ):
            check = self._state(m, conn).is_upstream_satisfied_live(INTERVAL)

        assert check.satisfied
        assert check.reason is None

    def _exact_gated(self, m):
        # Replace the default ENCAPSULATE gate with an EXACT one.
        from bollhav.model.source import Source, SourceModel
        from bollhav.model.upstream import UpstreamContract

        m.gated_upstreams = [
            Source("raw.orders", type=SourceModel(), contract=UpstreamContract.EXACT)
        ]
        return m

    def _entry_for(self, *, fixed_intervals):
        from bollhav.postgres.state import LibraryEntry

        return LibraryEntry(
            upstream=[],
            model_type="TABLE",
            state_schema="z_raw",
            state_table="orders_state",
            temporality="temporal",
            fixed_intervals=fixed_intervals,
        )

    def test_exact_contract_on_flexible_upstream_raises(self) -> None:
        # Guard A: a flexible upstream (fixed_intervals=False) coalesces away
        # its exact-grain rows, so EXACT can never match → block forever. Make
        # it a loud definition error instead.
        from unittest.mock import patch

        import pytest

        from bollhav.postgres.state import PostgresState

        m = self._exact_gated(_model(upstream=["raw.orders"]))
        conn = _mock_conn()
        flexible = self._entry_for(fixed_intervals=False)
        with patch.object(PostgresState, "lookup_model", return_value=flexible):
            with pytest.raises(ValueError, match="flexible upstream"):
                self._state(m, conn).is_upstream_satisfied_live(INTERVAL)

    def test_exact_contract_on_fixed_upstream_is_allowed(self) -> None:
        # The complement: EXACT against a fixed-grid upstream is fine — it
        # proceeds to the normal satisfaction check.
        from unittest.mock import patch

        from bollhav.postgres.state import PostgresState

        m = self._exact_gated(_model(upstream=["raw.orders"]))
        conn = _mock_conn()
        fixed = self._entry_for(fixed_intervals=True)
        with (
            patch.object(PostgresState, "lookup_model", return_value=fixed),
            patch.object(PostgresState, "is_satisfied", return_value=True),
        ):
            check = self._state(m, conn).is_upstream_satisfied_live(INTERVAL)
        assert check.satisfied


# ── prefill row normalization ────────────────────────────────────────


class TestPrefillRowNormalization:
    """`_normalize_prefill_row` moved onto `PostgresState` as a
    staticmethod — same accept-bare-or-3-tuple contract."""

    def test_bare_TZInterval_is_pending(self) -> None:
        from bollhav.postgres.state import PostgresState

        iv = TZInterval(SINCE, UNTIL)
        assert PostgresState._normalize_prefill_row(iv) == (iv, "pending", None)

    def test_pending_tuple_clears_reason(self) -> None:
        from bollhav.postgres.state import PostgresState

        iv = TZInterval(SINCE, UNTIL)
        assert PostgresState._normalize_prefill_row((iv, "pending", "ignored")) == (
            iv,
            "pending",
            None,
        )

    def test_blocked_requires_reason(self) -> None:
        from bollhav.postgres.state import PostgresState

        iv = TZInterval(SINCE, UNTIL)
        with pytest.raises(ValueError, match="blocked rows require"):
            PostgresState._normalize_prefill_row((iv, "blocked", None))

    def test_unknown_status_raises(self) -> None:
        from bollhav.postgres.state import PostgresState

        iv = TZInterval(SINCE, UNTIL)
        with pytest.raises(ValueError, match="must be 'pending' or 'blocked'"):
            PostgresState._normalize_prefill_row((iv, "weird", None))
