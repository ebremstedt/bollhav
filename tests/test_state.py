"""Tests for the @state decorator and the State config.

Postgres backend is mocked — these tests cover the decorator's
gate/run/mark contract and the State + Model integration."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
from uuid import UUID

import pytest

from bollhav.model.state import State, StateMode, state


SINCE = datetime(2024, 1, 1, tzinfo=timezone.utc)
UNTIL = datetime(2024, 1, 2, tzinfo=timezone.utc)
RUN_ID = UUID("00000000-0000-0000-0000-00000000beef")


class TestStateConfig:
    def test_defaults(self) -> None:
        s = State()
        assert s.dsn_env_var is None
        assert s.schema_prefix is None
        assert s.table_suffix is None

    def test_explicit_values(self) -> None:
        s = State(
            dsn_env_var="STATE_DSN",
            schema_prefix="ops_",
            table_suffix="_history",
        )
        assert s.dsn_env_var == "STATE_DSN"
        assert s.schema_prefix == "ops_"
        assert s.table_suffix == "_history"


class TestStateMode:
    def test_values(self) -> None:
        assert StateMode.RESPECT.value == "respect"
        assert StateMode.DISRESPECT.value == "disrespect"


class TestBlockCode:
    """Stable codes — once assigned, never renumbered. These tests
    pin the public values so a refactor can't silently move them."""

    def test_codes_are_stable(self) -> None:
        from bollhav.model.state import BlockCode

        assert BlockCode.UPSTREAM_NOT_REGISTERED.value == "STATE_001"
        assert BlockCode.UPSTREAM_NOT_SATISFIED.value == "STATE_002"

    def test_format_block_reason(self) -> None:
        from bollhav.model.state import BlockCode, format_block_reason

        out = format_block_reason(
            BlockCode.UPSTREAM_NOT_REGISTERED,
            "upstream 'warehouse.orders' not registered",
        )
        assert out == "STATE_001: upstream 'warehouse.orders' not registered"

    def test_format_uses_only_enum_value(self) -> None:
        """The formatter takes the enum's `.value`, not its name —
        so the user-facing string is the code, not 'UPSTREAM_NOT_REGISTERED'."""
        from bollhav.model.state import BlockCode, format_block_reason

        out = format_block_reason(BlockCode.UPSTREAM_NOT_SATISFIED, "boom")
        assert out.startswith("STATE_002:")
        assert "UPSTREAM_NOT_SATISFIED" not in out


class TestModelStateField:
    def _kwargs(self):
        from bollhav.model import Batch, IntervalChunks, Target, TargetSchema

        return dict(
            target=Target(name="orders", schema=TargetSchema(name="public")),
            batching=Batch(interval=IntervalChunks(expression="@hourly")),
        )

    def test_state_defaults_to_none(self) -> None:
        from bollhav.model import Model

        m = Model(**self._kwargs())
        assert m.state is None

    def test_state_accepts_State(self) -> None:
        from bollhav.model import Model

        s = State()
        m = Model(state=s, **self._kwargs())
        assert m.state is s

    def test_state_without_batching_raises(self) -> None:
        from bollhav.model import Model, Target, TargetSchema

        with pytest.raises(ValueError, match="state tracking on model"):
            Model(
                target=Target(name="orders", schema=TargetSchema(name="public")),
                state=State(),
            )

    def test_staging_without_state_is_allowed(self) -> None:
        """Staging without state is a supported combo: memory-bounded
        chunked writes + atomic per-interval finalization, without
        the state-row durability layer. Re-runs re-process every
        interval (no `applied` gate). The orphan-staging GC handles
        crashed prior runs."""
        from bollhav.model import (
            Batch,
            IntervalChunks,
            Model,
            Staging,
            Target,
            TargetSchema,
        )

        m = Model(
            target=Target(
                name="orders",
                schema=TargetSchema(name="public"),
                staging=Staging(),
            ),
            batching=Batch(interval=IntervalChunks(expression="@hourly")),
        )
        assert m.state is None
        assert m.target.staging is not None

    def test_staging_with_state_ok(self) -> None:
        from bollhav.model import (
            Batch,
            IntervalChunks,
            Model,
            Staging,
            Target,
            TargetSchema,
        )

        m = Model(
            target=Target(
                name="orders",
                schema=TargetSchema(name="public"),
                staging=Staging(),
            ),
            batching=Batch(interval=IntervalChunks(expression="@hourly")),
            state=State(),
        )
        assert m.target.staging is not None
        assert m.state is not None


# ── decorator ────────────────────────────────────────────────────────


def _state_enabled_model():
    model = MagicMock()
    model.state = State()
    model.batching = MagicMock()
    model.target.full_name = "public.orders"
    model._state_run_id = RUN_ID
    model._state_applied_via_staging = None
    return model


class TestDecoratorPassthrough:
    def test_no_state_calls_inner_function(self) -> None:
        calls: list = []

        @state
        def execute(model, since, until):
            calls.append((since, until))

        model = MagicMock()
        model.state = None
        execute(model=model, since=SINCE, until=UNTIL)
        assert calls == [(SINCE, UNTIL)]

    def test_no_batching_is_passthrough(self) -> None:
        calls: list = []

        @state
        def execute(model, since, until):
            calls.append((since, until))

        model = MagicMock()
        model.state = State()
        model.batching = None
        execute(model=model, since=SINCE, until=UNTIL)
        assert calls == [(SINCE, UNTIL)]

    def test_none_since_until_is_passthrough(self) -> None:
        calls: list = []

        @state
        def execute(model, since, until):
            calls.append((since, until))

        with patch("bollhav.postgres.state.is_applied") as is_applied:
            execute(model=_state_enabled_model(), since=None, until=None)

        assert calls == [(None, None)]
        is_applied.assert_not_called()


class TestDecoratorGating:
    def test_applied_interval_is_skipped(self) -> None:
        calls: list = []

        @state
        def execute(model, since, until):
            calls.append((since, until))

        with (
            patch("bollhav.postgres.state.is_applied", return_value=True) as is_applied,
            patch("bollhav.postgres.state.mark_running"),
            patch("bollhav.postgres.state.mark_applied") as mark_applied,
        ):
            execute(model=_state_enabled_model(), since=SINCE, until=UNTIL)

        assert calls == []
        is_applied.assert_called_once()
        mark_applied.assert_not_called()

    def test_pending_interval_runs_and_marks_applied(self) -> None:
        calls: list = []

        @state
        def execute(model, since, until):
            calls.append((since, until))

        with (
            patch("bollhav.postgres.state.is_applied", return_value=False),
            patch("bollhav.postgres.state._connect", return_value=MagicMock()),
            patch(
                "bollhav.postgres.state.try_acquire_interval_lock",
                return_value=True,
            ),
            patch("bollhav.postgres.state.release_interval_lock"),
            patch("bollhav.postgres.state.mark_running"),
            patch("bollhav.postgres.state.mark_applied") as mark_applied,
        ):
            execute(model=_state_enabled_model(), since=SINCE, until=UNTIL)

        assert calls == [(SINCE, UNTIL)]
        mark_applied.assert_called_once()


class TestIntervalLock:
    """`@state` takes a per-interval advisory lock so two
    workers can race on the same model without colliding. Whoever
    grabs an interval's lock processes it; the other one sees the
    lock held and moves on."""

    def test_lock_acquired_runs_normally(self) -> None:
        calls: list = []

        @state
        def execute(model, since, until):
            calls.append((since, until))

        with (
            patch("bollhav.postgres.state.is_applied", return_value=False),
            patch("bollhav.postgres.state._connect", return_value=MagicMock()),
            patch(
                "bollhav.postgres.state.try_acquire_interval_lock",
                return_value=True,
            ) as acquire,
            patch("bollhav.postgres.state.release_interval_lock") as release,
            patch("bollhav.postgres.state.mark_running"),
            patch("bollhav.postgres.state.mark_applied") as mark_applied,
        ):
            execute(model=_state_enabled_model(), since=SINCE, until=UNTIL)

        acquire.assert_called_once()
        release.assert_called_once()
        assert calls == [(SINCE, UNTIL)]
        mark_applied.assert_called_once()

    def test_lock_held_by_another_worker_skips_silently(self) -> None:
        calls: list = []

        @state
        def execute(model, since, until):
            calls.append((since, until))

        with (
            patch("bollhav.postgres.state.is_applied", return_value=False),
            patch("bollhav.postgres.state._connect", return_value=MagicMock()),
            patch(
                "bollhav.postgres.state.try_acquire_interval_lock",
                return_value=False,
            ),
            patch("bollhav.postgres.state.release_interval_lock") as release,
            patch("bollhav.postgres.state.mark_running") as mark_running,
            patch("bollhav.postgres.state.mark_applied") as mark_applied,
        ):
            result = execute(model=_state_enabled_model(), since=SINCE, until=UNTIL)

        # Locked → skip everything, return None.
        assert result is None
        assert calls == []
        mark_running.assert_not_called()
        mark_applied.assert_not_called()
        # Nothing to release (we never acquired).
        release.assert_called_once()  # finally still calls release; harmless.

    def test_lock_released_even_on_exception_in_func(self) -> None:
        @state
        def execute(model, since, until):
            raise RuntimeError("inside func")

        with (
            patch("bollhav.postgres.state.is_applied", return_value=False),
            patch("bollhav.postgres.state._connect", return_value=MagicMock()),
            patch(
                "bollhav.postgres.state.try_acquire_interval_lock",
                return_value=True,
            ),
            patch("bollhav.postgres.state.release_interval_lock") as release,
            patch("bollhav.postgres.state.mark_running"),
            patch("bollhav.postgres.state.record_failure"),
        ):
            with pytest.raises(RuntimeError, match="inside func"):
                execute(model=_state_enabled_model(), since=SINCE, until=UNTIL)

        # Lock released even though func raised.
        release.assert_called_once()


class TestRunningStatus:
    """`@state` flips state to 'running' just before invoking
    the user's execute — so live ops dashboards can see exactly which
    intervals are being processed right now."""

    def test_pending_to_running_to_applied(self) -> None:
        order: list[str] = []

        @state
        def execute(model, since, until):
            order.append("execute")

        with (
            patch("bollhav.postgres.state.is_applied", return_value=False),
            patch("bollhav.postgres.state._connect", return_value=MagicMock()),
            patch(
                "bollhav.postgres.state.try_acquire_interval_lock",
                return_value=True,
            ),
            patch("bollhav.postgres.state.release_interval_lock"),
            patch(
                "bollhav.postgres.state.mark_running",
                side_effect=lambda **kw: order.append("running"),
            ),
            patch(
                "bollhav.postgres.state.mark_applied",
                side_effect=lambda **kw: order.append("applied"),
            ),
        ):
            execute(model=_state_enabled_model(), since=SINCE, until=UNTIL)

        # Running must come BEFORE the func, applied AFTER.
        assert order == ["running", "execute", "applied"]

    def test_running_then_error_on_exception(self) -> None:
        order: list[str] = []

        @state
        def execute(model, since, until):
            order.append("execute")
            raise RuntimeError("boom")

        with (
            patch("bollhav.postgres.state.is_applied", return_value=False),
            patch("bollhav.postgres.state._connect", return_value=MagicMock()),
            patch(
                "bollhav.postgres.state.try_acquire_interval_lock",
                return_value=True,
            ),
            patch("bollhav.postgres.state.release_interval_lock"),
            patch(
                "bollhav.postgres.state.mark_running",
                side_effect=lambda **kw: order.append("running"),
            ),
            patch("bollhav.postgres.state.mark_applied"),
            patch(
                "bollhav.postgres.state.record_failure",
                side_effect=lambda **kw: order.append("error"),
            ),
        ):
            with pytest.raises(RuntimeError, match="boom"):
                execute(model=_state_enabled_model(), since=SINCE, until=UNTIL)

        # Running set BEFORE the failure; error logged after.
        assert order == ["running", "execute", "error"]


class TestModelLock:
    """`model_lock` is the user-facing context manager that acquires
    a Postgres advisory lock around their per-model loop. Prevents
    concurrent runs of the same model."""

    def test_lock_acquired_yields_then_released(self) -> None:
        from bollhav.model.state import model_lock

        model = MagicMock()
        model.target.full_name = "warehouse.orders"
        model.state = MagicMock()
        model.batching = MagicMock()

        conn = MagicMock()
        with (
            patch("bollhav.postgres.state._connect", return_value=conn),
            patch(
                "bollhav.postgres.state.try_acquire_lock", return_value=True
            ) as acquire,
            patch("bollhav.postgres.state.release_lock") as release,
        ):
            with model_lock(model):
                acquire.assert_called_once_with(conn, model)
                # Lock held during the with-block — release not yet.
                release.assert_not_called()
            release.assert_called_once_with(conn, model)
            conn.close.assert_called_once()

    def test_lock_conflict_raises_ModelLockedError(self) -> None:
        from bollhav.model.state import ModelLockedError, model_lock

        model = MagicMock()
        model.target.full_name = "warehouse.orders"
        model.state = MagicMock()
        model.batching = MagicMock()

        conn = MagicMock()
        with (
            patch("bollhav.postgres.state._connect", return_value=conn),
            patch("bollhav.postgres.state.try_acquire_lock", return_value=False),
            patch("bollhav.postgres.state.release_lock") as release,
        ):
            with pytest.raises(ModelLockedError, match="warehouse.orders"):
                with model_lock(model):
                    pass  # pragma: no cover
            # Never acquired → don't try to release.
            release.assert_not_called()
            # Connection still closed cleanly.
            conn.close.assert_called_once()

    def test_lock_released_even_on_exception_inside_block(self) -> None:
        from bollhav.model.state import model_lock

        model = MagicMock()
        model.target.full_name = "warehouse.orders"
        model.state = MagicMock()
        model.batching = MagicMock()

        conn = MagicMock()
        with (
            patch("bollhav.postgres.state._connect", return_value=conn),
            patch("bollhav.postgres.state.try_acquire_lock", return_value=True),
            patch("bollhav.postgres.state.release_lock") as release,
        ):
            with pytest.raises(RuntimeError, match="inside block"):
                with model_lock(model):
                    raise RuntimeError("inside block")
            # Lock released; connection closed; exception propagated.
            release.assert_called_once_with(conn, model)
            conn.close.assert_called_once()


class TestDecoratorExceptionPath:
    def test_exception_reraises_logs_failure_and_does_not_mark_applied(self) -> None:
        @state
        def execute(model, since, until):
            raise RuntimeError("boom")

        with (
            patch("bollhav.postgres.state.is_applied", return_value=False),
            patch("bollhav.postgres.state._connect", return_value=MagicMock()),
            patch(
                "bollhav.postgres.state.try_acquire_interval_lock",
                return_value=True,
            ),
            patch("bollhav.postgres.state.release_interval_lock"),
            patch("bollhav.postgres.state.mark_running"),
            patch("bollhav.postgres.state.mark_applied") as mark_applied,
            patch("bollhav.postgres.state.record_failure") as record_failure,
        ):
            with pytest.raises(RuntimeError, match="boom"):
                execute(model=_state_enabled_model(), since=SINCE, until=UNTIL)

        mark_applied.assert_not_called()
        record_failure.assert_called_once()
        kw = record_failure.call_args.kwargs
        assert kw["error_type"] == "RuntimeError"
        assert kw["error_message"] == "boom"
        assert "RuntimeError" in (kw["traceback_text"] or "")
        # No prior staging marker → state SHOULD be flipped to 'error'.
        assert kw["update_state"] is True

    def test_post_stage_exception_logs_but_keeps_applied(self) -> None:
        """When staging successfully flushed (state already 'applied')
        and then user code AFTER the with-block raises, we log the
        error but do NOT downgrade state. Data IS in target."""

        @state
        def execute(model, since, until):
            # Simulate the staging flush having set the marker.
            model._state_applied_via_staging = (since, until)
            raise RuntimeError("post-stage boom")

        model = _state_enabled_model()
        with (
            patch("bollhav.postgres.state.is_applied", return_value=False),
            patch("bollhav.postgres.state._connect", return_value=MagicMock()),
            patch(
                "bollhav.postgres.state.try_acquire_interval_lock",
                return_value=True,
            ),
            patch("bollhav.postgres.state.release_interval_lock"),
            patch("bollhav.postgres.state.mark_running"),
            patch("bollhav.postgres.state.mark_applied"),
            patch("bollhav.postgres.state.record_failure") as record_failure,
        ):
            with pytest.raises(RuntimeError, match="post-stage boom"):
                execute(model=model, since=SINCE, until=UNTIL)

        record_failure.assert_called_once()
        assert record_failure.call_args.kwargs["update_state"] is False
        # The marker is consumed so a later interval can't reuse it.
        assert model._state_applied_via_staging is None


class TestStagingBypass:
    """When stage() flushes inside its own tx, @state must NOT
    re-issue mark_applied. The marker `_state_applied_via_staging` on
    the model signals 'already flipped for this interval'."""

    def test_marker_present_skips_mark_applied(self) -> None:
        @state
        def execute(model, since, until):
            model._state_applied_via_staging = (since, until)

        model = _state_enabled_model()
        with (
            patch("bollhav.postgres.state.is_applied", return_value=False),
            patch("bollhav.postgres.state._connect", return_value=MagicMock()),
            patch(
                "bollhav.postgres.state.try_acquire_interval_lock",
                return_value=True,
            ),
            patch("bollhav.postgres.state.release_interval_lock"),
            patch("bollhav.postgres.state.mark_running"),
            patch("bollhav.postgres.state.mark_applied") as mark_applied,
        ):
            execute(model=model, since=SINCE, until=UNTIL)

        mark_applied.assert_not_called()
        assert model._state_applied_via_staging is None  # consumed

    def test_no_marker_still_marks_applied(self) -> None:
        @state
        def execute(model, since, until):
            pass

        model = _state_enabled_model()
        with (
            patch("bollhav.postgres.state.is_applied", return_value=False),
            patch("bollhav.postgres.state._connect", return_value=MagicMock()),
            patch(
                "bollhav.postgres.state.try_acquire_interval_lock",
                return_value=True,
            ),
            patch("bollhav.postgres.state.release_interval_lock"),
            patch("bollhav.postgres.state.mark_running"),
            patch("bollhav.postgres.state.mark_applied") as mark_applied,
        ):
            execute(model=model, since=SINCE, until=UNTIL)

        mark_applied.assert_called_once()

    def test_stale_marker_from_other_interval_does_not_skip(self) -> None:
        @state
        def execute(model, since, until):
            pass

        model = _state_enabled_model()
        other = datetime(2099, 1, 1, tzinfo=timezone.utc)
        model._state_applied_via_staging = (other, other)

        with (
            patch("bollhav.postgres.state.is_applied", return_value=False),
            patch("bollhav.postgres.state._connect", return_value=MagicMock()),
            patch(
                "bollhav.postgres.state.try_acquire_interval_lock",
                return_value=True,
            ),
            patch("bollhav.postgres.state.release_interval_lock"),
            patch("bollhav.postgres.state.mark_running"),
            patch("bollhav.postgres.state.mark_applied") as mark_applied,
        ):
            execute(model=model, since=SINCE, until=UNTIL)

        mark_applied.assert_called_once()


# ── Postgres backend: naming + DSN resolution ────────────────────────


def _pg_model(*, state_cfg=None, target_dsn="TARGET_DSN"):
    """Model-shaped MagicMock for pg_state helper tests."""
    from bollhav.model.state import State

    model = MagicMock()
    model.state = state_cfg if state_cfg is not None else State()
    model.target.name = "orders"
    model.target.full_name = "public.orders"
    model.target.schema.resolved = "public"
    model.target.dsn_env_var = target_dsn
    return model


class TestStateSchemaName:
    def test_default_is_z_prefixed(self) -> None:
        from bollhav.postgres.state import _state_schema

        assert _state_schema(_pg_model()) == "z_public"

    def test_schema_prefix_override(self) -> None:
        from bollhav.model.state import State
        from bollhav.postgres.state import _state_schema

        m = _pg_model(state_cfg=State(schema_prefix="ops_"))
        assert _state_schema(m) == "ops_public"

    def test_schema_prefix_empty_drops_prefix(self) -> None:
        from bollhav.model.state import State
        from bollhav.postgres.state import _state_schema

        m = _pg_model(state_cfg=State(schema_prefix=""))
        assert _state_schema(m) == "public"


class TestStateTableName:
    def test_default_suffix_is_state(self) -> None:
        from bollhav.postgres.state import _state_table

        assert _state_table(_pg_model()) == "orders_state"

    def test_table_suffix_override(self) -> None:
        from bollhav.model.state import State
        from bollhav.postgres.state import _state_table

        m = _pg_model(state_cfg=State(table_suffix="_history"))
        assert _state_table(m) == "orders_history"


class TestRecordFailure:
    """`record_failure` does the atomic INSERT-error + UPDATE-state
    flip. These tests inspect the SQL it issues — most importantly,
    that the model's full_name is in the errors row so cross-model
    UNION queries can attribute each error to its source model."""

    def _conn(self):
        conn = MagicMock()
        conn.__enter__ = MagicMock(return_value=conn)
        conn.__exit__ = MagicMock(return_value=None)
        conn.transaction.return_value.__enter__ = MagicMock(return_value=None)
        conn.transaction.return_value.__exit__ = MagicMock(return_value=None)
        return conn

    def test_full_name_is_inserted_into_errors_row(self) -> None:
        from bollhav.postgres.state import record_failure

        m = _pg_model()
        m.target.full_name = "warehouse.orders"
        conn = self._conn()

        with patch("bollhav.postgres.state._connect", return_value=conn):
            record_failure(
                m,
                run_id=RUN_ID,
                since=SINCE,
                until=UNTIL,
                error_type="RuntimeError",
                error_message="boom",
                traceback_text="Traceback...",
            )

        insert = next(
            c for c in conn.execute.call_args_list if "INSERT INTO" in str(c.args[0])
        )
        sql_text = str(insert.args[0])
        params = insert.args[1]
        # full_name is the first column written.
        assert "full_name" in sql_text
        assert params[0] == "warehouse.orders"
        # And the rest of the params are as expected.
        assert params[1] == str(RUN_ID)
        assert params[2] == SINCE
        assert params[3] == UNTIL
        assert params[4] == "RuntimeError"
        assert params[5] == "boom"
        assert params[6] == "Traceback..."

    def test_update_state_true_flips_status_to_error(self) -> None:
        from bollhav.postgres.state import record_failure

        m = _pg_model()
        conn = self._conn()
        with patch("bollhav.postgres.state._connect", return_value=conn):
            record_failure(
                m,
                run_id=RUN_ID,
                since=SINCE,
                until=UNTIL,
                error_type="X",
                error_message="m",
                traceback_text=None,
                update_state=True,
            )

        executed = [str(c.args[0]) for c in conn.execute.call_args_list]
        assert any("UPDATE" in q and "status = 'error'" in q for q in executed)

    def test_update_state_false_logs_only(self) -> None:
        from bollhav.postgres.state import record_failure

        m = _pg_model()
        conn = self._conn()
        with patch("bollhav.postgres.state._connect", return_value=conn):
            record_failure(
                m,
                run_id=RUN_ID,
                since=SINCE,
                until=UNTIL,
                error_type="X",
                error_message="m",
                traceback_text=None,
                update_state=False,
            )

        executed = [str(c.args[0]) for c in conn.execute.call_args_list]
        # Insert happened, no UPDATE to state.
        assert any("INSERT INTO" in q for q in executed)
        assert not any("UPDATE" in q and "status = 'error'" in q for q in executed)


class TestReadStatusSummary:
    """`read_status_summary` powers the state banner. It returns
    counts per status plus a code-breakdown for blocked rows."""

    def _conn(self, rows):
        conn = MagicMock()
        conn.__enter__ = MagicMock(return_value=conn)
        conn.__exit__ = MagicMock(return_value=None)
        result = MagicMock()
        result.fetchall.return_value = rows
        conn.execute.return_value = result
        return conn

    def test_empty_state_returns_zeros(self) -> None:
        from bollhav.postgres.state import read_status_summary

        m = _pg_model()
        with patch("bollhav.postgres.state._connect", return_value=self._conn([])):
            s = read_status_summary(m)
        assert s["counts"] == {
            "pending": 0,
            "running": 0,
            "applied": 0,
            "blocked": 0,
            "error": 0,
        }
        assert s["blocked_groups"] == {}

    def test_counts_split_by_status(self) -> None:
        from bollhav.postgres.state import read_status_summary

        rows = [
            ("pending", None),
            ("pending", None),
            ("running", None),
            ("applied", None),
            ("blocked", "STATE_001: upstream 'a.b' not registered"),
            ("error", None),
            ("error", None),
        ]
        m = _pg_model()
        with patch("bollhav.postgres.state._connect", return_value=self._conn(rows)):
            s = read_status_summary(m)
        assert s["counts"] == {
            "pending": 2,
            "running": 1,
            "applied": 1,
            "blocked": 1,
            "error": 2,
        }

    def test_blocked_groups_split_by_code_and_upstream(self) -> None:
        """Same code with different upstreams → separate groups."""
        from bollhav.postgres.state import read_status_summary

        rows = [
            ("blocked", "STATE_001: upstream 'a.b' not registered"),
            ("blocked", "STATE_001: upstream 'a.b' not registered"),
            ("blocked", "STATE_001: upstream 'c.d' not registered"),
            ("blocked", "STATE_002: upstream 'a.b' has no applied row covering …"),
        ]
        m = _pg_model()
        with patch("bollhav.postgres.state._connect", return_value=self._conn(rows)):
            s = read_status_summary(m)
        # (code, upstream) → count
        assert s["blocked_groups"] == {
            ("STATE_001", "a.b"): 2,
            ("STATE_001", "c.d"): 1,
            ("STATE_002", "a.b"): 1,
        }

    def test_state_002_with_many_windows_collapses_to_one_group(self) -> None:
        """STATE_002's message includes a time window — many windows
        for the same (code, upstream) should still be ONE row in the
        banner. Verifies the group key ignores the window."""
        from bollhav.postgres.state import read_status_summary

        rows = [
            (
                "blocked",
                "STATE_002: upstream 'a.b' has no applied row covering "
                "2024-01-01T00:00:00+00:00 → 2024-01-02T00:00:00+00:00",
            ),
            (
                "blocked",
                "STATE_002: upstream 'a.b' has no applied row covering "
                "2024-01-02T00:00:00+00:00 → 2024-01-03T00:00:00+00:00",
            ),
            (
                "blocked",
                "STATE_002: upstream 'a.b' has no applied row covering "
                "2024-01-03T00:00:00+00:00 → 2024-01-04T00:00:00+00:00",
            ),
        ]
        m = _pg_model()
        with patch("bollhav.postgres.state._connect", return_value=self._conn(rows)):
            s = read_status_summary(m)
        assert s["blocked_groups"] == {("STATE_002", "a.b"): 3}

    def test_blocked_reason_without_upstream_in_message(self) -> None:
        """Falls back to None for upstream when message lacks the
        `upstream 'X'` shape — banner will render '(upstream unknown)'."""
        from bollhav.postgres.state import read_status_summary

        rows = [("blocked", "STATE_999: something custom")]
        m = _pg_model()
        with patch("bollhav.postgres.state._connect", return_value=self._conn(rows)):
            s = read_status_summary(m)
        assert s["blocked_groups"] == {("STATE_999", None): 1}

    def test_blocked_without_reason_is_ignored_in_groups(self) -> None:
        from bollhav.postgres.state import read_status_summary

        rows = [
            ("blocked", None),
            ("blocked", ""),
            ("blocked", "STATE_001: upstream 'a.b' not registered"),
        ]
        m = _pg_model()
        with patch("bollhav.postgres.state._connect", return_value=self._conn(rows)):
            s = read_status_summary(m)
        assert s["counts"]["blocked"] == 3
        assert s["blocked_groups"] == {("STATE_001", "a.b"): 1}


class TestStateBanner:
    """`_print_state_banner` queries the state for each staged model
    and prints a one-line summary. No-op when no model has staging."""

    def _staged_model(self, full_name, summary, upstream=None):
        from bollhav.model.staging import Staging

        model = MagicMock()
        model.target.staging = Staging()
        model.target.full_name = full_name
        model.upstream = list(upstream) if upstream is not None else []
        return model, summary

    def test_skips_non_staged_models(self, capsys) -> None:
        from bollhav.model.load_models import _print_state_banner

        m = MagicMock()
        m.target.staging = None
        with patch("bollhav.postgres.state.read_status_summary") as rss:
            _print_state_banner([m])

        rss.assert_not_called()
        assert capsys.readouterr().out == ""

    def test_no_upstreams_declared(self, capsys) -> None:
        """A model with no upstream= still prints the upstream section,
        labelled '(none declared)' for clarity."""
        from bollhav.model.load_models import _print_state_banner

        m, _ = self._staged_model("warehouse.orders", None, upstream=[])
        with patch(
            "bollhav.postgres.state.read_status_summary",
            return_value={
                "counts": {"pending": 3, "applied": 0, "blocked": 0},
                "blocked_groups": {},
            },
        ):
            _print_state_banner([m])

        out = capsys.readouterr().out
        assert "warehouse.orders" in out
        assert "upstream:  (none declared)" in out
        assert "state:" in out
        assert "3 pending" in out

    def test_each_declared_upstream_gets_its_own_line(self, capsys) -> None:
        """The upstream section iterates `model.upstream` — every
        declared dependency shows up with its status, so the operator
        can see at a glance which ones block and which don't."""
        from bollhav.model.load_models import _print_state_banner

        m, _ = self._staged_model(
            "warehouse.dashboard",
            None,
            upstream=["warehouse.orders", "warehouse.payments", "raw.events"],
        )
        with patch(
            "bollhav.postgres.state.read_status_summary",
            return_value={
                "counts": {"pending": 0, "applied": 0, "blocked": 5},
                "blocked_groups": {("STATE_002", "warehouse.payments"): 5},
            },
        ):
            _print_state_banner([m])

        out = capsys.readouterr().out
        # Every declared upstream appears on its own line.
        assert "warehouse.orders" in out
        assert "warehouse.payments" in out
        assert "raw.events" in out
        # Only payments is blocking.
        assert "blocked · STATE_002 × 5" in out
        # Orders and raw.events are fulfilled.
        # Verify by counting "fulfilled" occurrences.
        assert out.count("fulfilled") == 2

    def test_one_upstream_blocked_by_multiple_codes(self, capsys) -> None:
        """When the same upstream has more than one blocking code,
        they're comma-joined on the same line."""
        from bollhav.model.load_models import _print_state_banner

        m, _ = self._staged_model("warehouse.x", None, upstream=["warehouse.weird"])
        with patch(
            "bollhav.postgres.state.read_status_summary",
            return_value={
                "counts": {"pending": 0, "applied": 0, "blocked": 8},
                "blocked_groups": {
                    ("STATE_001", "warehouse.weird"): 3,
                    ("STATE_002", "warehouse.weird"): 5,
                },
            },
        ):
            _print_state_banner([m])

        out = capsys.readouterr().out
        assert "warehouse.weird" in out
        assert "STATE_001 × 3" in out
        assert "STATE_002 × 5" in out

    def test_fulfilled_when_no_blocks(self, capsys) -> None:
        from bollhav.model.load_models import _print_state_banner

        m, _ = self._staged_model(
            "warehouse.enriched", None, upstream=["warehouse.orders"]
        )
        with patch(
            "bollhav.postgres.state.read_status_summary",
            return_value={
                "counts": {"pending": 3, "applied": 0, "blocked": 0, "error": 0},
                "blocked_groups": {},
            },
        ):
            _print_state_banner([m])

        out = capsys.readouterr().out
        assert "warehouse.orders" in out
        assert "fulfilled" in out
        assert "3 pending" in out

    def test_state_line_shows_error_count(self, capsys) -> None:
        from bollhav.model.load_models import _print_state_banner

        m, _ = self._staged_model("warehouse.orders", None, upstream=[])
        with patch(
            "bollhav.postgres.state.read_status_summary",
            return_value={
                "counts": {"pending": 1, "applied": 5, "blocked": 0, "error": 2},
                "blocked_groups": {},
            },
        ):
            _print_state_banner([m])

        out = capsys.readouterr().out
        assert "1 pending" in out
        assert "5 applied" in out
        assert "0 blocked" in out
        assert "2 error" in out

    def test_db_failure_renders_unavailable_line(self, capsys) -> None:
        from bollhav.model.load_models import _print_state_banner

        m, _ = self._staged_model("warehouse.orders", None)
        with patch(
            "bollhav.postgres.state.read_status_summary",
            side_effect=ConnectionError("DB down"),
        ):
            _print_state_banner([m])

        out = capsys.readouterr().out
        assert "warehouse.orders" in out
        assert "state unavailable" in out
        assert "DB down" in out


class TestPeekShortCircuits:
    """`PEEK=true` runs the bootstrap (so the banner is accurate)
    and then exits before calling the wrapped main()."""

    def test_peek_skips_func(self) -> None:
        from bollhav.model.load_models import load_models

        ran = []

        @load_models
        def main(models, debug):  # pragma: no cover — should NOT be called
            ran.append(True)

        with (
            patch(
                "bollhav.model.load_models._read_env",
                return_value=_make_cfg(peek=True),
            ),
            patch(
                "bollhav.model.load_models.apply_runtime_overrides",
                return_value=[],
            ),
            patch("bollhav.model.load_models._print_summary"),
            patch("bollhav.model.load_models._bootstrap_state_for_staged_models"),
            patch("bollhav.model.load_models._print_state_banner"),
        ):
            main()

        assert ran == []

    def test_no_peek_calls_func(self) -> None:
        from bollhav.model.load_models import load_models

        ran = []

        @load_models
        def main(models, debug):
            ran.append(True)

        with (
            patch(
                "bollhav.model.load_models._read_env",
                return_value=_make_cfg(peek=False),
            ),
            patch(
                "bollhav.model.load_models.apply_runtime_overrides",
                return_value=[],
            ),
            patch("bollhav.model.load_models._print_summary"),
            patch("bollhav.model.load_models._bootstrap_state_for_staged_models"),
            patch("bollhav.model.load_models._print_state_banner"),
        ):
            main()

        assert ran == [True]


def _make_cfg(**overrides):
    """Build a minimal _RuntimeConfig for tests that drive the wrapper."""
    from bollhav.model.load_models import _RuntimeConfig
    from bollhav.model.ordering import UpstreamMode

    defaults = dict(
        tags="x",
        schema_suffix="",
        upstream_mode=UpstreamMode.ENFORCE,
        latest=False,
        backfill_enabled=True,
        backfill_since=None,
        backfill_until=None,
        interval_expression_override=None,
        window_expression_override=None,
        lookback_override=None,
        tz_override=None,
        dry_run=False,
        dry_run_extra=False,
        state_mode=StateMode.RESPECT,
        state_disabled=False,
        peek=False,
        debug=False,
    )
    defaults.update(overrides)
    return _RuntimeConfig(**defaults)


class TestStateDsnResolution:
    def test_falls_back_to_target_dsn_when_state_dsn_unset(self) -> None:
        from bollhav.postgres.state import _resolve_dsn

        m = _pg_model(target_dsn="TARGET_DSN")
        with patch.dict("os.environ", {"TARGET_DSN": "postgresql://t/db"}, clear=False):
            assert _resolve_dsn(m) == "postgresql://t/db"

    def test_uses_state_dsn_when_set(self) -> None:
        from bollhav.model.state import State
        from bollhav.postgres.state import _resolve_dsn

        m = _pg_model(state_cfg=State(dsn_env_var="STATE_DSN"))
        m.target.dsn_env_var = "TARGET_DSN"  # should be ignored
        with patch.dict(
            "os.environ",
            {"STATE_DSN": "postgresql://s/db", "TARGET_DSN": "postgresql://t/db"},
            clear=False,
        ):
            assert _resolve_dsn(m) == "postgresql://s/db"

    def test_raises_when_neither_dsn_env_var_set(self) -> None:
        from bollhav.postgres.state import _resolve_dsn

        m = _pg_model(target_dsn=None)
        with pytest.raises(ValueError, match="state.dsn_env_var or target.dsn_env_var"):
            _resolve_dsn(m)

    def test_raises_when_env_var_unset_in_environment(self) -> None:
        from bollhav.postgres.state import _resolve_dsn

        m = _pg_model(target_dsn="UNSET_ENV_FOR_THIS_TEST")
        with patch.dict("os.environ", {}, clear=False):
            # Make sure the var really isn't there.
            import os

            os.environ.pop("UNSET_ENV_FOR_THIS_TEST", None)
            with pytest.raises(ValueError, match="UNSET_ENV_FOR_THIS_TEST.*unset"):
                _resolve_dsn(m)


class TestConnectErrorMessage:
    def test_connection_failure_message_names_model_and_env_var(self) -> None:
        """When psycopg.connect fails, the error must clearly say which
        model the state operation was for and which env var was used."""
        import psycopg

        from bollhav.postgres.state import _connect

        m = _pg_model(target_dsn="TARGET_DSN")
        with (
            patch.dict("os.environ", {"TARGET_DSN": "postgresql://nowhere"}),
            patch(
                "psycopg.connect",
                side_effect=psycopg.OperationalError("connection refused"),
            ),
        ):
            with pytest.raises(ConnectionError) as excinfo:
                _connect(m)

        msg = str(excinfo.value)
        assert "public.orders" in msg
        assert "TARGET_DSN" in msg
        assert "connection refused" in msg
        assert "state.dsn_env_var" in msg  # mentions which knob to check

    def test_malformed_dsn_also_wrapped(self) -> None:
        """psycopg raises ProgrammingError (not OperationalError) when
        the DSN string itself is malformed — e.g. a placeholder like
        '...' got copy-pasted instead of a real DSN. The wrapper must
        catch the whole psycopg.Error hierarchy so the bootstrap's
        skip-with-warning path triggers instead of a raw traceback."""
        import psycopg

        from bollhav.postgres.state import _connect

        m = _pg_model(target_dsn="TARGET_DSN")
        with (
            patch.dict("os.environ", {"TARGET_DSN": "..."}),
            patch(
                "psycopg.connect",
                side_effect=psycopg.ProgrammingError(
                    'missing "=" after "..." in connection info string'
                ),
            ),
        ):
            with pytest.raises(ConnectionError) as excinfo:
                _connect(m)

        msg = str(excinfo.value)
        assert "public.orders" in msg
        assert "TARGET_DSN" in msg
        assert 'missing "=" after "..."' in msg

    def test_connection_failure_mentions_state_dsn_when_used(self) -> None:
        import psycopg

        from bollhav.model.state import State
        from bollhav.postgres.state import _connect

        m = _pg_model(state_cfg=State(dsn_env_var="STATE_DSN"))
        with (
            patch.dict("os.environ", {"STATE_DSN": "postgresql://nowhere"}),
            patch(
                "psycopg.connect",
                side_effect=psycopg.OperationalError("connection refused"),
            ),
        ):
            with pytest.raises(ConnectionError) as excinfo:
                _connect(m)

        msg = str(excinfo.value)
        assert "STATE_DSN" in msg
        # Doesn't claim it fell back to target — it didn't.
        assert "falling back" not in msg


# ── @load_models bootstrap: contract → state → filter ───────────────


class TestLoadModelsStateBootstrap:
    """`_bootstrap_state_for_staged_models` runs after
    apply_runtime_overrides. For each model with `target.staging` set:
      1. Compute contract (model.intervals from bounds/batching)
      2. Ensure state tables + prefill
      3. Replace model.intervals with read_pending result"""

    def _staged_model(self, *, contract=None, upstream=None):
        from bollhav.model.intervals import TZInterval
        from bollhav.model.staging import Staging

        model = MagicMock()
        model.target.is_view = False
        model.library = False
        model.target.staging = Staging()
        model.target.full_name = "public.orders"
        model.upstream = upstream if upstream is not None else []
        # Default contract: real TZInterval objects so .since/.until work.
        model.intervals = (
            contract if contract is not None else [TZInterval(SINCE, UNTIL)]
        )
        return model

    def _plain_model(self):
        model = MagicMock()
        model.target.is_view = False
        model.library = False
        model.target.staging = None
        model.target.full_name = "public.other"
        return model

    def _mock_conn(self):
        """psycopg-shaped MagicMock so the bootstrap's `with _connect()
        as conn` block runs without touching a real DB."""
        conn = MagicMock()
        conn.__enter__ = MagicMock(return_value=conn)
        conn.__exit__ = MagicMock(return_value=None)
        return conn

    def _patch_db(self, conn=None):
        """All the patches the bootstrap needs to bypass DB I/O."""
        if conn is None:
            conn = self._mock_conn()
        return [
            patch("bollhav.postgres.state._connect", return_value=conn),
            patch("bollhav.postgres.library.ensure_library"),
            patch("bollhav.postgres.library.register"),
        ]

    def test_no_staging_means_no_bootstrap(self) -> None:
        from bollhav.model.load_models import _bootstrap_state_for_staged_models

        m = self._plain_model()
        with (
            patch("bollhav.postgres.state.ensure_tables") as ensure,
            patch("bollhav.postgres.state.prefill") as pf,
            patch("bollhav.postgres.state.read_actionable") as rp,
        ):
            _bootstrap_state_for_staged_models([m], state_mode=StateMode.RESPECT)

        ensure.assert_not_called()
        pf.assert_not_called()
        rp.assert_not_called()

    def test_staged_model_bootstraps_and_filters_to_pending(self) -> None:
        from bollhav.model.intervals import TZInterval
        from bollhav.model.load_models import _bootstrap_state_for_staged_models

        contract = [TZInterval(SINCE, UNTIL)]
        pending = [TZInterval(SINCE, UNTIL)]
        m = self._staged_model(contract=contract)

        patches = self._patch_db()
        with (
            patches[0],
            patches[1],
            patches[2],
            patch("bollhav.postgres.state.ensure_tables") as ensure,
            patch("bollhav.postgres.state.prefill") as pf,
            patch("bollhav.postgres.state.read_actionable", return_value=pending) as rp,
        ):
            _bootstrap_state_for_staged_models([m], state_mode=StateMode.RESPECT)

        ensure.assert_called_once_with(m)
        pf.assert_called_once()
        # Pre-fill rows are 3-tuples (interval, status, reason).
        rows = pf.call_args.kwargs["intervals"]
        assert all(isinstance(r, tuple) and len(r) == 3 for r in rows)
        assert [r[0] for r in rows] == contract
        # No upstreams to check → everything is pending.
        assert all(r[1] == "pending" and r[2] is None for r in rows)
        assert pf.call_args.kwargs["state_mode"] is StateMode.RESPECT
        rp.assert_called_once_with(m)
        assert m.intervals == pending

    def test_state_mode_propagates(self) -> None:
        from bollhav.model.load_models import _bootstrap_state_for_staged_models

        m = self._staged_model()
        patches = self._patch_db()
        with (
            patches[0],
            patches[1],
            patches[2],
            patch("bollhav.postgres.state.ensure_tables"),
            patch("bollhav.postgres.state.prefill") as pf,
            patch("bollhav.postgres.state.read_actionable", return_value=[]),
        ):
            _bootstrap_state_for_staged_models([m], state_mode=StateMode.DISRESPECT)

        assert pf.call_args.kwargs["state_mode"] is StateMode.DISRESPECT

    def test_connection_failure_skips_model_with_empty_intervals(self) -> None:
        from bollhav.model.load_models import _bootstrap_state_for_staged_models

        m1 = self._staged_model()
        m2 = self._staged_model()
        m2.target.full_name = "public.other"

        patches = self._patch_db()
        with (
            patches[0],
            patches[1],
            patches[2],
            patch(
                "bollhav.postgres.state.ensure_tables",
                side_effect=[ConnectionError("DB down"), None],
            ),
            patch("bollhav.postgres.state.prefill"),
            patch("bollhav.postgres.state.read_actionable", return_value=["p1"]),
        ):
            _bootstrap_state_for_staged_models([m1, m2], state_mode=StateMode.RESPECT)

        # m1 gets intervals=[] (skipped), m2 still bootstraps.
        assert m1.intervals == []
        assert m2.intervals == ["p1"]

    def test_run_id_stashed_on_model(self) -> None:
        from bollhav.model.load_models import _bootstrap_state_for_staged_models

        m = self._staged_model()
        patches = self._patch_db()
        with (
            patches[0],
            patches[1],
            patches[2],
            patch("bollhav.postgres.state.ensure_tables"),
            patch("bollhav.postgres.state.prefill"),
            patch("bollhav.postgres.state.read_actionable", return_value=[]),
        ):
            _bootstrap_state_for_staged_models([m], state_mode=StateMode.RESPECT)

        assert m._state_run_id is not None


class TestStateMode_EnvVar:
    """STATE_MODE env var → cfg.state_mode → bootstrap argument."""

    def test_default_is_respect(self) -> None:
        from bollhav.model.load_models import _resolve_state_mode

        with patch(
            "bollhav.model.load_models.env_var",
            lambda name, **kw: None,
        ):
            assert _resolve_state_mode() is StateMode.RESPECT

    def test_respect_explicit(self) -> None:
        from bollhav.model.load_models import _resolve_state_mode

        with patch(
            "bollhav.model.load_models.env_var",
            lambda name, **kw: "respect" if name == "STATE_MODE" else None,
        ):
            assert _resolve_state_mode() is StateMode.RESPECT

    def test_disrespect(self) -> None:
        from bollhav.model.load_models import _resolve_state_mode

        with patch(
            "bollhav.model.load_models.env_var",
            lambda name, **kw: "disrespect" if name == "STATE_MODE" else None,
        ):
            assert _resolve_state_mode() is StateMode.DISRESPECT

    def test_unknown_raises(self) -> None:
        from bollhav.model.load_models import _resolve_state_mode

        with patch(
            "bollhav.model.load_models.env_var",
            lambda name, **kw: "bogus" if name == "STATE_MODE" else None,
        ):
            with pytest.raises(ValueError, match="STATE_MODE must be one of"):
                _resolve_state_mode()


class TestModelIntervalsSetter:
    """The cached-list setter lets @load_models stash a filtered list
    that the user's loop reads via `model.intervals`."""

    def test_assignment_returns_stashed_list(self) -> None:
        from bollhav.model import Batch, IntervalChunks, Model, Target, TargetSchema

        m = Model(
            target=Target(name="orders", schema=TargetSchema(name="public")),
            batching=Batch(interval=IntervalChunks(expression="@daily")),
        )
        m.intervals = ["fake1", "fake2"]
        assert m.intervals == ["fake1", "fake2"]

    def test_assigning_None_falls_back_to_live_compute(self) -> None:
        from datetime import timezone

        from bollhav.model import (
            Batch,
            Bounds,
            IntervalChunks,
            Model,
            Target,
            TargetSchema,
        )

        m = Model(
            target=Target(name="orders", schema=TargetSchema(name="public")),
            batching=Batch(interval=IntervalChunks(expression="@daily")),
            bounds=Bounds(begin=datetime(2024, 1, 1, tzinfo=timezone.utc)),
        )
        # Pre-stash, then clear: setter must drop back to computed.
        m.intervals = []
        assert m.intervals == []
        m.intervals = None
        assert len(m.intervals) > 0  # live-computed
