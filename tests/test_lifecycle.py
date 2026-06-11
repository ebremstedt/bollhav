"""Tests for the @model_lifecycle / @execute_lifecycle decorators.

Connections and the state backend are mocked — these assert the
*orchestration*: gate, lock, mark_running/applied/failure, upstream
blocking, pass-through for state-less models, and that every state
write targets the injected `state_conn`.

The state machine constructs `PostgresState(model=model, conn=state_conn)`
inline, so the per-call mocks are wired by patching
`bollhav.postgres.state.PostgresState` to return a fake state object;
the connection that state work runs against is asserted via the
construction `conn=` kwarg (not per-method, as in the old API).
"""

import sys
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

sys.modules.setdefault("pyodbc", MagicMock())  # bollhav.mssql.data imports it

from bollhav.model.lifecycle import execute_lifecycle, model_lifecycle
from bollhav.model.modelrun import ModelRun
from bollhav.model.state import ModelLockedError
from bollhav.model.upstream import UpstreamCheck

SINCE = datetime(2024, 1, 1, tzinfo=timezone.utc)
UNTIL = datetime(2024, 1, 2, tzinfo=timezone.utc)


def _model(*, state=True, allow_concurrent=True, upstream=None, staging=False):
    m = MagicMock()
    m.target.full_name = "public.orders"
    m.target.database = None  # → Postgres data backend (the default)
    m.target.stage = staging
    m.curfew = None  # real Models default to no curfew; mock must match
    m.upstream = list(upstream) if upstream else []
    if state:
        m.state = MagicMock()
        m.state.allow_concurrent_runs = allow_concurrent
        m.stateful = True
    else:
        m.state = None
        m.stateful = False
    return ModelRun(model=m)


def _interval():
    iv = MagicMock()
    iv.since = SINCE
    iv.until = UNTIL
    return iv


# ── execute_lifecycle ────────────────────────────────────────────────


class TestExecuteLifecycle:
    def _pg(self, *, applied=False, lock=True, upstream_ok=True):
        """Build a fake PostgresState instance. `is_upstream_satisfied_live`
        returns an `UpstreamCheck` (object with `.satisfied`/`.reason`), not
        a tuple, matching the new contract API."""
        pg = MagicMock()
        pg.is_applied.return_value = applied
        pg.try_acquire_interval_lock.return_value = lock
        pg.is_upstream_satisfied_live.return_value = (
            UpstreamCheck()
            if upstream_ok
            else UpstreamCheck(blockers=("up (interval)",))
        )
        return pg

    def test_state_less_is_passthrough(self):
        ran = []

        @execute_lifecycle
        def execute(run, interval, data_conn, state_conn=None):
            ran.append(True)
            return "ok"

        # state None and no staging → no PostgresState use at all
        with patch("bollhav.postgres.state.PostgresState") as pg_cls:
            result = execute(_model(state=False), _interval(), "DATA")
        assert result == "ok"
        assert ran == [True]
        pg_cls.assert_not_called()

    def test_happy_path_marks_running_then_applied_on_state_conn(self):
        pg = self._pg()
        ran = []

        @execute_lifecycle
        def execute(run, interval, data_conn, state_conn=None):
            ran.append(True)
            return "ok"

        with patch("bollhav.postgres.state.PostgresState", return_value=pg) as pg_cls:
            result = execute(_model(), _interval(), "DATA", "STATE")

        assert result == "ok"
        assert ran == [True]
        pg.mark_running.assert_called_once()
        pg.mark_applied.assert_called_once()
        pg.release_interval_lock.assert_called_once()
        # state work runs against the injected state_conn — bound at
        # construction, so assert the constructor conn kwarg.
        assert pg_cls.call_args.kwargs["conn"] == "STATE"

    def test_state_conn_defaults_to_data_conn(self):
        pg = self._pg()

        @execute_lifecycle
        def execute(run, interval, data_conn, state_conn=None):
            return None

        with patch("bollhav.postgres.state.PostgresState", return_value=pg) as pg_cls:
            execute(_model(), _interval(), "DATA")  # no state_conn → co-located

        assert pg_cls.call_args.kwargs["conn"] == "DATA"

    def test_applied_interval_is_gated(self):
        pg = self._pg(applied=True)
        ran = []

        @execute_lifecycle
        def execute(run, interval, data_conn, state_conn=None):
            ran.append(True)

        with patch("bollhav.postgres.state.PostgresState", return_value=pg):
            result = execute(_model(), _interval(), "DATA", "STATE")

        assert result is None
        assert ran == []  # body never ran
        pg.try_acquire_interval_lock.assert_not_called()

    def test_lock_held_skips(self):
        pg = self._pg(lock=False)
        ran = []

        @execute_lifecycle
        def execute(run, interval, data_conn, state_conn=None):
            ran.append(True)

        with patch("bollhav.postgres.state.PostgresState", return_value=pg):
            result = execute(_model(), _interval(), "DATA", "STATE")

        assert result is None
        assert ran == []
        pg.mark_running.assert_not_called()

    def test_upstream_not_satisfied_marks_blocked(self):
        pg = self._pg(upstream_ok=False)
        ran = []

        @execute_lifecycle
        def execute(run, interval, data_conn, state_conn=None):
            ran.append(True)

        with patch("bollhav.postgres.state.PostgresState", return_value=pg):
            result = execute(_model(upstream=["up"]), _interval(), "DATA", "STATE")

        assert result is None
        assert ran == []
        pg.mark_blocked.assert_called_once()
        # mark_blocked carries the composed UpstreamCheck.reason
        assert pg.mark_blocked.call_args.kwargs["reason"]
        pg.mark_running.assert_not_called()

    def test_exception_records_failure_and_reraises(self):
        pg = self._pg()

        @execute_lifecycle
        def execute(run, interval, data_conn, state_conn=None):
            raise RuntimeError("boom")

        with patch("bollhav.postgres.state.PostgresState", return_value=pg):
            with pytest.raises(RuntimeError, match="boom"):
                execute(_model(), _interval(), "DATA", "STATE")

        pg.record_failure.assert_called_once()
        kw = pg.record_failure.call_args.kwargs
        assert kw["update_state"] is True  # not staged → downgrade to error
        pg.mark_applied.assert_not_called()
        pg.release_interval_lock.assert_called_once()


# ── model_lifecycle ──────────────────────────────────────────────────


class TestModelLifecycle:
    """`@model_lifecycle` does asset DDL via `PostgresData` and, when the
    model is stateful, the state bootstrap via `PostgresState`. The old
    `run_pre/post_model_actions` and `_bootstrap_model` seams are gone, so
    these patch `PostgresData` / `PostgresState` directly."""

    def _stateful_model(self, *, allow_concurrent=True):
        from bollhav.model.database import Database
        from bollhav.model.state import StateBackend

        m = MagicMock()
        m.target.full_name = "public.orders"
        m.target.database = Database.POSTGRES
        m.target.recreate_table = False
        m.target.truncate_table = False
        m.target.partitioned_by = None
        m.target.unique_columns = []
        m.target.stage = False
        m.curfew = None
        m.upstream = []
        m.state = MagicMock()
        m.state.backend = StateBackend.POSTGRES
        m.state.allow_concurrent_runs = allow_concurrent
        m.stateful = True
        m.is_view = False
        m.is_kind_monolithic = False
        m.is_kind_view = False
        # No resolved window on this bare mock → compute_intervals() returns
        # the (None,) contract instead of trying to split a MagicMock.
        m.window = None
        return ModelRun(model=m)

    def _stateless_model(self):
        from bollhav.model.database import Database

        m = MagicMock()
        m.target.full_name = "public.orders"
        m.target.database = Database.POSTGRES
        m.target.recreate_table = False
        m.target.truncate_table = False
        m.target.partitioned_by = None
        m.target.unique_columns = []
        m.target.stage = False
        m.curfew = None
        m.upstream = []
        m.state = None
        m.stateful = False
        m.is_view = False
        m.is_kind_monolithic = False
        m.is_kind_view = False
        return ModelRun(model=m)

    def test_state_less_runs_assets_no_bootstrap(self):
        """State-less model: asset DDL runs (PostgresData), but no state
        bootstrap (PostgresState is never constructed)."""
        ran = []

        @model_lifecycle
        def execute_model(run, data_conn, state_conn=None):
            ran.append(True)

        with (
            patch("bollhav.postgres.data.PostgresData") as data_cls,
            patch("bollhav.postgres.state.PostgresState") as state_cls,
        ):
            execute_model(self._stateless_model(), "DATA")

        assert ran == [True]
        data_cls.assert_called_once()  # target asset DDL ran
        data_cls.return_value.create_schema.assert_called_once()
        data_cls.return_value.create_table.assert_called_once()
        state_cls.assert_not_called()  # no state → no state bootstrap

    def test_stateful_bootstraps_then_runs(self):
        """Stateful model: state bootstrap runs (ensure_library /
        register_model / ensure_tables, seed intervals, filter actionable)
        before the body, and asset DDL runs too."""
        ran = []

        model = self._stateful_model()

        @model_lifecycle
        def execute_model(run, data_conn, state_conn=None):
            ran.append(True)

        with (
            patch("bollhav.postgres.data.PostgresData") as data_cls,
            patch("bollhav.postgres.state.PostgresState") as state_cls,
        ):
            state = state_cls.return_value
            state.acquire_model_lock.return_value = True
            execute_model(model, "DATA", "STATE")

        assert ran == [True]
        data_cls.assert_called()  # asset DDL ran
        state.ensure_library.assert_called_once()
        state.register_model.assert_called_once()
        state.ensure_tables.assert_called_once()
        state.get_actionable_intervals.assert_called_once()

    def test_no_concurrent_runs_takes_and_releases_lock(self):
        model = self._stateful_model(allow_concurrent=False)

        @model_lifecycle
        def execute_model(run, data_conn, state_conn=None):
            return None

        with (
            patch("bollhav.postgres.data.PostgresData"),
            patch("bollhav.postgres.state.PostgresState") as state_cls,
        ):
            state = state_cls.return_value
            state.acquire_model_lock.return_value = True
            execute_model(model, "DATA", "STATE")

        state.acquire_model_lock.assert_called_once()
        state.release_lock.assert_called_once()

    def test_no_concurrent_runs_lock_held_raises(self):
        """When another run holds the model lock, `acquire_model_lock`
        raises `ModelLockedError` and the lifecycle propagates it."""
        model = self._stateful_model(allow_concurrent=False)

        @model_lifecycle
        def execute_model(run, data_conn, state_conn=None):
            return None

        with (
            patch("bollhav.postgres.data.PostgresData"),
            patch("bollhav.postgres.state.PostgresState") as state_cls,
        ):
            state_cls.return_value.acquire_model_lock.side_effect = ModelLockedError(
                "held"
            )
            with pytest.raises(ModelLockedError):
                execute_model(model, "DATA", "STATE")


class TestBackendDispatch:
    """The lifecycle resolves the data backend from `model.target.database`:
    Postgres → PostgresData, MSSQL → MssqlData. MSSQL is never stateful, so
    its asset DDL runs but no state bootstrap."""

    def _mssql_model(self, *, stage=False):
        from bollhav.model.database import Database

        m = MagicMock()
        m.target.full_name = "warehouse.events"
        m.target.database = Database.MSSQL
        m.target.recreate_table = False
        m.target.truncate_table = False
        m.target.partitioned_by = None
        m.target.unique_columns = []
        m.target.stage = stage
        m.curfew = None
        m.upstream = []
        m.state = None
        m.stateful = False
        m.is_view = False
        m.is_kind_monolithic = False
        m.is_kind_view = False
        return ModelRun(model=m)

    def test_mssql_model_routes_assets_through_mssql_data(self):
        @model_lifecycle
        def execute_model(run, data_conn, state_conn=None):
            return None

        with (
            patch("bollhav.mssql.data.MssqlData") as mssql_cls,
            patch("bollhav.postgres.data.PostgresData") as pg_cls,
            patch("bollhav.postgres.state.PostgresState") as state_cls,
        ):
            execute_model(self._mssql_model(), "DATA")

        mssql_cls.assert_called_once()  # MSSQL backend resolved
        mssql_cls.return_value.create_schema.assert_called_once()
        mssql_cls.return_value.create_table.assert_called_once()
        pg_cls.assert_not_called()  # not the Postgres backend
        state_cls.assert_not_called()  # MSSQL is never stateful

    def test_mssql_staged_execute_routes_through_mssql_data(self):
        @execute_lifecycle
        def execute_interval(run, interval, data_conn, state_conn=None):
            return None

        with patch("bollhav.mssql.data.MssqlData") as mssql_cls:
            execute_interval(self._mssql_model(stage=True), _interval(), "DATA")

        data = mssql_cls.return_value
        data.create_staging_table.assert_called_once()
        data.apply_staging_to_target.assert_called_once()
        data.drop_staging_table.assert_called_once()
