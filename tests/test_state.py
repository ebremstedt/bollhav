"""Tests for the @state_tracker decorator and the State config.

The Postgres backend itself is mocked — these tests cover the
decorator's gate/run/mark/error contract and the State + Model
integration. End-to-end DB exercise lives in the example pipeline."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
from uuid import UUID

import pytest


from bollhav.model.state import State, StateMode, state_tracker


# ── State config + Model integration ─────────────────────────────────


class TestStateConfig:
    def test_defaults(self) -> None:
        s = State()
        assert s.dsn_env_var is None
        assert s.log_errors is True

    def test_explicit_values(self) -> None:
        s = State(dsn_env_var="STATE_DSN", log_errors=False)
        assert s.dsn_env_var == "STATE_DSN"
        assert s.log_errors is False


class TestStateMode:
    def test_values(self) -> None:
        assert StateMode.RESPECT.value == "respect"
        assert StateMode.DISRESPECT.value == "disrespect"


class TestModelStateField:
    def _model_kwargs(self):
        from bollhav.model import (
            Batch,
            IntervalChunks,
            Target,
            TargetSchema,
        )

        return dict(
            target=Target(name="orders", schema=TargetSchema(name="public")),
            batching=Batch(interval=IntervalChunks(expression="@hourly")),
        )

    def test_state_field_defaults_to_none(self) -> None:
        from bollhav.model import Model

        m = Model(**self._model_kwargs())
        assert m.state is None

    def test_state_field_accepts_state(self) -> None:
        from bollhav.model import Model

        s = State()
        m = Model(state=s, **self._model_kwargs())
        assert m.state is s

    def test_intervals_starts_unset(self) -> None:
        from bollhav.model import Model

        m = Model(**self._model_kwargs())
        assert m.intervals is None

    def test_state_without_batching_raises(self) -> None:
        from bollhav.model import Model, Target, TargetSchema

        with pytest.raises(ValueError, match="state tracking on model"):
            Model(
                target=Target(name="orders", schema=TargetSchema(name="public")),
                state=State(),
            )


# ── decorator behavior ───────────────────────────────────────────────


def _state_enabled_model(*, log_errors: bool = True):
    """Build a minimal model-shaped MagicMock that the decorator
    recognizes as state-enabled."""
    model = MagicMock()
    model.state = State(log_errors=log_errors)
    model.batching = MagicMock()  # non-None → state-enabled
    model.target.full_name = "public.orders"
    model._state_run_id = UUID("00000000-0000-0000-0000-00000000beef")
    return model


SINCE = datetime(2024, 1, 1, tzinfo=timezone.utc)
UNTIL = datetime(2024, 1, 2, tzinfo=timezone.utc)


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
        """Models without batching can't be state-tracked at all — the
        decorator should treat them as passthrough even if state is set."""
        calls: list = []

        @state_tracker
        def execute(model, since, until):
            calls.append((since, until))

        model = MagicMock()
        model.state = State()
        model.batching = None
        execute(model=model, since=SINCE, until=UNTIL)
        assert calls == [(SINCE, UNTIL)]


class TestDecoratorGating:
    def test_applied_interval_is_skipped(self) -> None:
        calls: list = []

        @state_tracker
        def execute(model, since, until):
            calls.append((since, until))

        with patch(
            "bollhav.postgres.state.is_applied", return_value=True
        ) as is_applied, patch(
            "bollhav.postgres.state.mark_applied"
        ) as mark_applied, patch(
            "bollhav.postgres.state.record_error"
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

        with patch("bollhav.postgres.state.is_applied", return_value=False), patch(
            "bollhav.postgres.state.mark_applied"
        ) as mark_applied, patch(
            "bollhav.postgres.state.record_error"
        ) as record_error:
            execute(model=_state_enabled_model(), since=SINCE, until=UNTIL)

        assert calls == [(SINCE, UNTIL)]
        mark_applied.assert_called_once()
        record_error.assert_not_called()


class TestDecoratorErrorHandling:
    def test_exception_records_error_and_reraises(self) -> None:
        @state_tracker
        def execute(model, since, until):
            raise RuntimeError("boom")

        with patch("bollhav.postgres.state.is_applied", return_value=False), patch(
            "bollhav.postgres.state.mark_applied"
        ) as mark_applied, patch(
            "bollhav.postgres.state.record_error"
        ) as record_error:
            with pytest.raises(RuntimeError, match="boom"):
                execute(model=_state_enabled_model(), since=SINCE, until=UNTIL)

        record_error.assert_called_once()
        kwargs = record_error.call_args.kwargs
        assert kwargs["error_type"] == "RuntimeError"
        assert kwargs["error_message"] == "boom"
        assert kwargs["traceback_text"]  # non-empty
        mark_applied.assert_not_called()

    def test_log_errors_false_skips_error_table(self) -> None:
        @state_tracker
        def execute(model, since, until):
            raise RuntimeError("boom")

        with patch("bollhav.postgres.state.is_applied", return_value=False), patch(
            "bollhav.postgres.state.mark_applied"
        ), patch(
            "bollhav.postgres.state.record_error"
        ) as record_error:
            with pytest.raises(RuntimeError):
                execute(
                    model=_state_enabled_model(log_errors=False),
                    since=SINCE,
                    until=UNTIL,
                )

        record_error.assert_not_called()


class TestDiscoverFlow:
    """_compute_intervals_and_prefill_state branches on cfg.discover.
    These tests verify both branches without touching a real DB."""

    def _cfg(self, *, discover: bool, state_mode: StateMode = StateMode.RESPECT):
        from bollhav.model.load_models import _RuntimeConfig
        from bollhav.model.ordering import UpstreamMode

        return _RuntimeConfig(
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
            state_mode=state_mode,
            discover=discover,
            debug=False,
        )

    def test_discover_with_no_state_yields_empty_intervals(self) -> None:
        from bollhav.model.load_models import _compute_intervals_and_prefill_state

        model = MagicMock()
        model.state = None
        model.intervals = "untouched"
        _compute_intervals_and_prefill_state([model], self._cfg(discover=True))
        assert model.intervals == []

    def test_discover_respect_reads_pending(self) -> None:
        from bollhav.model.load_models import _compute_intervals_and_prefill_state
        from bollhav.model.intervals import TZInterval

        pending = [TZInterval(SINCE, UNTIL)]
        model = _state_enabled_model()

        with patch(
            "bollhav.postgres.state.ensure_tables"
        ) as ensure_tables, patch(
            "bollhav.postgres.state.read_pending", return_value=pending
        ) as read_pending, patch(
            "bollhav.postgres.state.reset_all_to_pending"
        ) as reset:
            _compute_intervals_and_prefill_state(
                [model], self._cfg(discover=True, state_mode=StateMode.RESPECT)
            )

        ensure_tables.assert_called_once_with(model)
        read_pending.assert_called_once_with(model)
        reset.assert_not_called()
        assert model.intervals == pending

    def test_discover_disrespect_resets_then_reads(self) -> None:
        from bollhav.model.load_models import _compute_intervals_and_prefill_state
        from bollhav.model.intervals import TZInterval

        pending = [TZInterval(SINCE, UNTIL)]
        model = _state_enabled_model()

        with patch("bollhav.postgres.state.ensure_tables"), patch(
            "bollhav.postgres.state.read_pending", return_value=pending
        ), patch(
            "bollhav.postgres.state.reset_all_to_pending"
        ) as reset:
            _compute_intervals_and_prefill_state(
                [model], self._cfg(discover=True, state_mode=StateMode.DISRESPECT)
            )

        reset.assert_called_once()
        assert model.intervals == pending

    def test_discover_skips_prefill(self) -> None:
        from bollhav.model.load_models import _compute_intervals_and_prefill_state

        model = _state_enabled_model()

        with patch("bollhav.postgres.state.ensure_tables"), patch(
            "bollhav.postgres.state.read_pending", return_value=[]
        ), patch(
            "bollhav.postgres.state.prefill"
        ) as prefill:
            _compute_intervals_and_prefill_state(
                [model], self._cfg(discover=True)
            )

        prefill.assert_not_called()


class TestDecoratorNoneIntervals:
    """No-batching models call execute with since/until=None. The
    decorator must passthrough — there's no interval to gate on."""

    def test_none_since_until_is_passthrough(self) -> None:
        calls: list = []

        @state_tracker
        def execute(model, since, until):
            calls.append((since, until))

        with patch("bollhav.postgres.state.is_applied") as is_applied:
            execute(model=_state_enabled_model(), since=None, until=None)

        assert calls == [(None, None)]
        is_applied.assert_not_called()
