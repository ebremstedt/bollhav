"""Tests for the @model_lifecycle / @interval_lifecycle decorators.

Connections and the state backend are mocked — these assert the
*orchestration*: gate, lock, mark_running/applied/failure, upstream
blocking, pass-through for state-less models, and that every state
write targets the injected `state_conn`.
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from bollhav.model.lifecycle import interval_lifecycle, model_lifecycle
from bollhav.model.state import ModelLockedError

SINCE = datetime(2024, 1, 1, tzinfo=timezone.utc)
UNTIL = datetime(2024, 1, 2, tzinfo=timezone.utc)


def _model(*, state=True, allow_concurrent=True, upstream=None, staging=True):
    m = MagicMock()
    m.target.full_name = "public.orders"
    m.target.database = None  # → Postgres data backend (the default)
    m.target.staging = MagicMock() if staging else None
    m.upstream = list(upstream) if upstream else []
    if state:
        m.state = MagicMock()
        m.state.allow_concurrent_runs = allow_concurrent
        # state_activated is a property on the real Model; pin it here.
        m.state_activated = True
    else:
        m.state = None
        m.state_activated = False
    return m


def _interval():
    iv = MagicMock()
    iv.since = SINCE
    iv.until = UNTIL
    return iv


# ── interval_lifecycle ───────────────────────────────────────────────


class TestIntervalLifecycle:
    def _pg(self, *, applied=False, lock=True, upstream_ok=True):
        pg = MagicMock()
        pg.is_applied.return_value = applied
        pg.try_acquire_interval_lock.return_value = lock
        pg.is_upstream_satisfied_live.return_value = (upstream_ok, "STATE_002: blocked")
        return pg

    def test_state_less_is_passthrough(self):
        ran = []

        @interval_lifecycle
        def execute(model, interval, data_conn, state_conn=None):
            ran.append(True)
            return "ok"

        # state None → no _pg() use at all
        with patch("bollhav.model.lifecycle._state_backend") as pg_factory:
            result = execute(_model(state=False), _interval(), "DATA")
        assert result == "ok"
        assert ran == [True]
        pg_factory.assert_not_called()

    def test_happy_path_marks_running_then_applied_on_state_conn(self):
        pg = self._pg()
        ran = []

        @interval_lifecycle
        def execute(model, interval, data_conn, state_conn=None):
            ran.append(True)
            return "ok"

        with patch("bollhav.model.lifecycle._state_backend", return_value=pg):
            result = execute(_model(), _interval(), "DATA", "STATE")

        assert result == "ok"
        assert ran == [True]
        pg.mark_running.assert_called_once()
        pg.mark_applied.assert_called_once()
        pg.release_interval_lock.assert_called_once()
        # state writes go on the injected state_conn
        assert pg.mark_running.call_args.kwargs["conn"] == "STATE"
        assert pg.mark_applied.call_args.kwargs["conn"] == "STATE"

    def test_state_conn_defaults_to_data_conn(self):
        pg = self._pg()

        @interval_lifecycle
        def execute(model, interval, data_conn, state_conn=None):
            return None

        with patch("bollhav.model.lifecycle._state_backend", return_value=pg):
            execute(_model(), _interval(), "DATA")  # no state_conn → co-located

        assert pg.mark_running.call_args.kwargs["conn"] == "DATA"

    def test_applied_interval_is_gated(self):
        pg = self._pg(applied=True)
        ran = []

        @interval_lifecycle
        def execute(model, interval, data_conn, state_conn=None):
            ran.append(True)

        with patch("bollhav.model.lifecycle._state_backend", return_value=pg):
            result = execute(_model(), _interval(), "DATA", "STATE")

        assert result is None
        assert ran == []  # body never ran
        pg.try_acquire_interval_lock.assert_not_called()

    def test_lock_held_skips(self):
        pg = self._pg(lock=False)
        ran = []

        @interval_lifecycle
        def execute(model, interval, data_conn, state_conn=None):
            ran.append(True)

        with patch("bollhav.model.lifecycle._state_backend", return_value=pg):
            result = execute(_model(), _interval(), "DATA", "STATE")

        assert result is None
        assert ran == []
        pg.mark_running.assert_not_called()

    def test_upstream_not_satisfied_marks_blocked(self):
        pg = self._pg(upstream_ok=False)
        ran = []

        @interval_lifecycle
        def execute(model, interval, data_conn, state_conn=None):
            ran.append(True)

        with patch("bollhav.model.lifecycle._state_backend", return_value=pg):
            result = execute(_model(upstream=["up"]), _interval(), "DATA", "STATE")

        assert result is None
        assert ran == []
        pg.mark_blocked.assert_called_once()
        assert pg.mark_blocked.call_args.kwargs["conn"] == "STATE"
        pg.mark_running.assert_not_called()

    def test_exception_records_failure_and_reraises(self):
        pg = self._pg()

        @interval_lifecycle
        def execute(model, interval, data_conn, state_conn=None):
            raise RuntimeError("boom")

        with patch("bollhav.model.lifecycle._state_backend", return_value=pg):
            with pytest.raises(RuntimeError, match="boom"):
                execute(_model(), _interval(), "DATA", "STATE")

        pg.record_failure.assert_called_once()
        kw = pg.record_failure.call_args.kwargs
        assert kw["conn"] == "STATE"
        assert kw["update_state"] is True  # not staged → downgrade to error
        pg.mark_applied.assert_not_called()
        pg.release_interval_lock.assert_called_once()


# ── model_lifecycle ──────────────────────────────────────────────────


class TestModelLifecycle:
    def test_state_less_runs_actions_no_bootstrap(self):
        ran = []

        @model_lifecycle
        def execute_model(model, data_conn, state_conn=None):
            ran.append(True)

        with (
            patch("bollhav.postgres.actions.run_pre_model_actions") as pre,
            patch("bollhav.postgres.actions.run_post_model_actions") as post,
            patch("bollhav.model.lifecycle._bootstrap_model") as boot,
            patch("bollhav.model.lifecycle._setup_non_state_model") as setup,
        ):
            execute_model(_model(state=False), "DATA")

        assert ran == [True]
        pre.assert_called_once()
        post.assert_called_once()
        boot.assert_not_called()  # no state → no state bootstrap
        setup.assert_called_once()  # but non-state setup (library/GC) runs

    def test_stateful_bootstraps_then_runs(self):
        ran = []

        @model_lifecycle
        def execute_model(model, data_conn, state_conn=None):
            ran.append(True)

        with (
            patch("bollhav.postgres.actions.run_pre_model_actions"),
            patch("bollhav.postgres.actions.run_post_model_actions") as post,
            patch("bollhav.model.lifecycle._bootstrap_model") as boot,
            patch("bollhav.model.lifecycle._state_backend"),
            patch("bollhav.model.load_models._resolve_state_mode"),
        ):
            execute_model(_model(), "DATA", "STATE")

        boot.assert_called_once()
        assert ran == [True]
        post.assert_called_once()  # POST only on clean return

    def test_no_concurrent_runs_takes_and_releases_lock(self):
        pg = MagicMock()
        pg.try_acquire_lock.return_value = True

        @model_lifecycle
        def execute_model(model, data_conn, state_conn=None):
            return None

        with (
            patch("bollhav.postgres.actions.run_pre_model_actions"),
            patch("bollhav.postgres.actions.run_post_model_actions"),
            patch("bollhav.model.lifecycle._bootstrap_model"),
            patch("bollhav.model.lifecycle._state_backend", return_value=pg),
            patch("bollhav.model.load_models._resolve_state_mode"),
        ):
            execute_model(_model(allow_concurrent=False), "DATA", "STATE")

        pg.try_acquire_lock.assert_called_once()
        pg.release_lock.assert_called_once()

    def test_no_concurrent_runs_lock_held_raises(self):
        pg = MagicMock()
        pg.try_acquire_lock.return_value = False

        @model_lifecycle
        def execute_model(model, data_conn, state_conn=None):
            return None

        with (
            patch("bollhav.model.lifecycle._state_backend", return_value=pg),
            patch("bollhav.postgres.actions.run_pre_model_actions"),
        ):
            with pytest.raises(ModelLockedError):
                execute_model(_model(allow_concurrent=False), "DATA", "STATE")
