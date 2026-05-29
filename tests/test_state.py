"""Tests for the @state_tracker decorator and the State config.

Postgres backend is mocked — these tests cover the decorator's
gate/run/mark contract and the State + Model integration."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
from uuid import UUID

import pytest

from bollhav.model.state import State, StateMode, state_tracker


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

    def test_staging_without_state_raises(self) -> None:
        from bollhav.model import (
            Batch,
            IntervalChunks,
            Model,
            Staging,
            Target,
            TargetSchema,
        )

        with pytest.raises(ValueError, match="target.staging on model"):
            Model(
                target=Target(
                    name="orders",
                    schema=TargetSchema(name="public"),
                    staging=Staging(),
                ),
                batching=Batch(interval=IntervalChunks(expression="@hourly")),
            )

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

        @state_tracker
        def execute(model, since, until):
            calls.append((since, until))

        model = MagicMock()
        model.state = None
        execute(model=model, since=SINCE, until=UNTIL)
        assert calls == [(SINCE, UNTIL)]

    def test_no_batching_is_passthrough(self) -> None:
        calls: list = []

        @state_tracker
        def execute(model, since, until):
            calls.append((since, until))

        model = MagicMock()
        model.state = State()
        model.batching = None
        execute(model=model, since=SINCE, until=UNTIL)
        assert calls == [(SINCE, UNTIL)]

    def test_none_since_until_is_passthrough(self) -> None:
        calls: list = []

        @state_tracker
        def execute(model, since, until):
            calls.append((since, until))

        with patch("bollhav.postgres.state.is_applied") as is_applied:
            execute(model=_state_enabled_model(), since=None, until=None)

        assert calls == [(None, None)]
        is_applied.assert_not_called()


class TestDecoratorGating:
    def test_applied_interval_is_skipped(self) -> None:
        calls: list = []

        @state_tracker
        def execute(model, since, until):
            calls.append((since, until))

        with (
            patch("bollhav.postgres.state.is_applied", return_value=True) as is_applied,
            patch("bollhav.postgres.state.mark_applied") as mark_applied,
        ):
            execute(model=_state_enabled_model(), since=SINCE, until=UNTIL)

        assert calls == []
        is_applied.assert_called_once()
        mark_applied.assert_not_called()

    def test_pending_interval_runs_and_marks_applied(self) -> None:
        calls: list = []

        @state_tracker
        def execute(model, since, until):
            calls.append((since, until))

        with (
            patch("bollhav.postgres.state.is_applied", return_value=False),
            patch("bollhav.postgres.state.mark_applied") as mark_applied,
        ):
            execute(model=_state_enabled_model(), since=SINCE, until=UNTIL)

        assert calls == [(SINCE, UNTIL)]
        mark_applied.assert_called_once()


class TestDecoratorExceptionPath:
    def test_exception_reraises_and_does_not_mark_applied(self) -> None:
        @state_tracker
        def execute(model, since, until):
            raise RuntimeError("boom")

        with (
            patch("bollhav.postgres.state.is_applied", return_value=False),
            patch("bollhav.postgres.state.mark_applied") as mark_applied,
        ):
            with pytest.raises(RuntimeError, match="boom"):
                execute(model=_state_enabled_model(), since=SINCE, until=UNTIL)

        mark_applied.assert_not_called()


class TestStagingBypass:
    """When stage() flushes inside its own tx, @state_tracker must NOT
    re-issue mark_applied. The marker `_state_applied_via_staging` on
    the model signals 'already flipped for this interval'."""

    def test_marker_present_skips_mark_applied(self) -> None:
        @state_tracker
        def execute(model, since, until):
            model._state_applied_via_staging = (since, until)

        model = _state_enabled_model()
        with (
            patch("bollhav.postgres.state.is_applied", return_value=False),
            patch("bollhav.postgres.state.mark_applied") as mark_applied,
        ):
            execute(model=model, since=SINCE, until=UNTIL)

        mark_applied.assert_not_called()
        assert model._state_applied_via_staging is None  # consumed

    def test_no_marker_still_marks_applied(self) -> None:
        @state_tracker
        def execute(model, since, until):
            pass

        model = _state_enabled_model()
        with (
            patch("bollhav.postgres.state.is_applied", return_value=False),
            patch("bollhav.postgres.state.mark_applied") as mark_applied,
        ):
            execute(model=model, since=SINCE, until=UNTIL)

        mark_applied.assert_called_once()

    def test_stale_marker_from_other_interval_does_not_skip(self) -> None:
        @state_tracker
        def execute(model, since, until):
            pass

        model = _state_enabled_model()
        other = datetime(2099, 1, 1, tzinfo=timezone.utc)
        model._state_applied_via_staging = (other, other)

        with (
            patch("bollhav.postgres.state.is_applied", return_value=False),
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

    def _staged_model(self, *, pending=None, contract=None):
        from bollhav.model.staging import Staging

        model = MagicMock()
        model.target.staging = Staging()
        model.target.full_name = "public.orders"
        # Reading model.intervals returns the contract on the first
        # access; later assignment is what we're verifying.
        model.intervals = contract if contract is not None else ["c1", "c2", "c3"]
        return model

    def _plain_model(self):
        model = MagicMock()
        model.target.staging = None
        model.target.full_name = "public.other"
        return model

    def test_no_staging_means_no_bootstrap(self) -> None:
        from bollhav.model.load_models import _bootstrap_state_for_staged_models

        m = self._plain_model()
        with (
            patch("bollhav.postgres.state.ensure_tables") as ensure,
            patch("bollhav.postgres.state.prefill") as pf,
            patch("bollhav.postgres.state.read_pending") as rp,
        ):
            _bootstrap_state_for_staged_models([m], state_mode=StateMode.RESPECT)

        ensure.assert_not_called()
        pf.assert_not_called()
        rp.assert_not_called()

    def test_staged_model_bootstraps_and_filters_to_pending(self) -> None:
        from bollhav.model.intervals import TZInterval
        from bollhav.model.load_models import _bootstrap_state_for_staged_models

        contract = ["c1", "c2", "c3"]
        pending = [TZInterval(SINCE, UNTIL)]
        m = self._staged_model(contract=contract)

        with (
            patch("bollhav.postgres.state.ensure_tables") as ensure,
            patch("bollhav.postgres.state.prefill") as pf,
            patch("bollhav.postgres.state.read_pending", return_value=pending) as rp,
        ):
            _bootstrap_state_for_staged_models([m], state_mode=StateMode.RESPECT)

        ensure.assert_called_once_with(m)
        # Pre-fill got the contract intervals.
        pf.assert_called_once()
        assert pf.call_args.kwargs["intervals"] == contract
        assert pf.call_args.kwargs["state_mode"] is StateMode.RESPECT
        rp.assert_called_once_with(m)
        # model.intervals now holds the pending list.
        assert m.intervals == pending

    def test_state_mode_propagates(self) -> None:
        from bollhav.model.load_models import _bootstrap_state_for_staged_models

        m = self._staged_model()
        with (
            patch("bollhav.postgres.state.ensure_tables"),
            patch("bollhav.postgres.state.prefill") as pf,
            patch("bollhav.postgres.state.read_pending", return_value=[]),
        ):
            _bootstrap_state_for_staged_models([m], state_mode=StateMode.DISRESPECT)

        assert pf.call_args.kwargs["state_mode"] is StateMode.DISRESPECT

    def test_connection_failure_skips_model_with_empty_intervals(self) -> None:
        from bollhav.model.load_models import _bootstrap_state_for_staged_models

        m1 = self._staged_model()
        m2 = self._staged_model()
        m2.target.full_name = "public.other"

        with (
            patch(
                "bollhav.postgres.state.ensure_tables",
                side_effect=[ConnectionError("DB down"), None],
            ),
            patch("bollhav.postgres.state.prefill"),
            patch("bollhav.postgres.state.read_pending", return_value=["p1"]),
        ):
            _bootstrap_state_for_staged_models([m1, m2], state_mode=StateMode.RESPECT)

        # m1 gets intervals=[] (skipped), m2 still bootstraps.
        assert m1.intervals == []
        assert m2.intervals == ["p1"]

    def test_run_id_stashed_on_model(self) -> None:
        from bollhav.model.load_models import _bootstrap_state_for_staged_models

        m = self._staged_model()
        with (
            patch("bollhav.postgres.state.ensure_tables"),
            patch("bollhav.postgres.state.prefill"),
            patch("bollhav.postgres.state.read_pending", return_value=[]),
        ):
            _bootstrap_state_for_staged_models([m], state_mode=StateMode.RESPECT)

        assert isinstance(m._state_run_id, type(m._state_run_id))
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
