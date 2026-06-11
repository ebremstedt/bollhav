"""Tests for the State config + the Postgres state backend.

The decorator/lifecycle behaviour is covered in test_lifecycle.py;
these tests cover the `State` config, the Model integration, and the
mocked Postgres backend helpers (naming, record_failure, status
summary, DSN resolution)."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
from uuid import UUID

import pytest

from bollhav.model.state import State, StateMode


SINCE = datetime(2024, 1, 1, tzinfo=timezone.utc)
UNTIL = datetime(2024, 1, 2, tzinfo=timezone.utc)
RUN_ID = UUID("00000000-0000-0000-0000-00000000beef")


class TestStateConfig:
    def test_defaults(self) -> None:
        from bollhav.model.state import StateBackend

        s = State()
        assert s.backend is StateBackend.POSTGRES
        assert s.allow_concurrent_runs is True

    def test_explicit_values(self) -> None:
        s = State(
            allow_concurrent_runs=False,
        )
        assert s.allow_concurrent_runs is False


class TestStateMode:
    def test_values(self) -> None:
        assert StateMode.DISCOVER.value == "discover"
        assert StateMode.BULLDOZER.value == "bulldozer"


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
        from bollhav.model import Batch, TimeChunking, Kind, Target

        return dict(
            target=Target(name="orders", schema="public"),
            batching=Batch(time=TimeChunking(chunk="@hourly")),
            kind=Kind.INTERVAL,
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
        from bollhav.model import Kind, Model, Target

        with pytest.raises(ValueError, match="has no batching"):
            Model(
                target=Target(name="orders", schema="public"),
                state=State(),
                kind=Kind.INTERVAL,
            )

    def test_staging_without_state_is_allowed(self) -> None:
        """Staging without state is a supported combo: memory-bounded
        chunked writes + atomic per-interval finalization, without
        the state-row durability layer. Re-runs re-process every
        interval (no `applied` gate). The orphan-staging GC handles
        crashed prior runs."""
        from bollhav.model import (
            Batch,
            TimeChunking,
            Kind,
            Model,
            Staging,
            Target,
        )

        m = Model(
            target=Target(
                name="orders",
                schema="public",
                staging=Staging(),
            ),
            batching=Batch(time=TimeChunking(chunk="@hourly")),
            kind=Kind.INTERVAL,
        )
        assert m.state is None
        assert m.target.staging is not None

    def test_staging_with_state_ok(self) -> None:
        from bollhav.model import (
            Batch,
            TimeChunking,
            Kind,
            Model,
            Staging,
            Target,
        )

        m = Model(
            target=Target(
                name="orders",
                schema="public",
                staging=Staging(),
            ),
            batching=Batch(time=TimeChunking(chunk="@hourly")),
            state=State(),
            kind=Kind.INTERVAL,
        )
        assert m.target.staging is not None
        assert m.state is not None


# ── decorator ────────────────────────────────────────────────────────


def _pg_model(*, state_cfg=None, target_dsn="TARGET_DSN"):
    """Model-shaped MagicMock for pg_state helper tests."""
    from bollhav.model.state import State

    model = MagicMock()
    model.state = state_cfg if state_cfg is not None else State()
    model.target.name = "orders"
    model.target.full_name = "public.orders"
    model.target.schema_resolved = "public"
    model.target.schema_suffix = ""
    model.target.schema_suffix_appendix = None
    model.target.dsn_env_var = target_dsn
    return model


class TestStateSchemaName:
    def test_state_schema_is_fixed_central(self) -> None:
        from bollhav.postgres.state import LIBRARY_SCHEMA, PostgresState

        assert PostgresState(_pg_model())._state_schema() == LIBRARY_SCHEMA
        assert LIBRARY_SCHEMA == "z_bollhav"

    def test_schema_suffix_isolates_state_schema(self) -> None:
        from bollhav.postgres.state import PostgresState

        # A SCHEMA_SUFFIX run gets its own bollhav environment, so state
        # tables land in z_bollhav_<suffix>, never touching prod's z_bollhav.
        m = _pg_model()
        m.target.schema_suffix = "pr123"
        m.target.schema_suffix_appendix = None
        assert PostgresState(m)._state_schema() == "z_bollhav_pr123"


class TestStateTableName:
    def test_state_table_is_deterministic_hash(self) -> None:
        from bollhav.postgres.state import PostgresState, state_table_name

        assert PostgresState(_pg_model())._state_table() == state_table_name(
            "public.orders"
        )

    def test_name_fits_postgres_limit_and_carries_table(self) -> None:
        from bollhav.postgres.state import state_table_name

        name = state_table_name(
            "Intelligence.intelligence_raw_dan.some_very_long_clinical_view_name"
        )
        assert len(name) <= 63
        # de-vowelled context, readable table fragment, hash tail
        assert name.startswith("ntllgnc_")


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
        from bollhav.model.intervals import TZInterval
        from bollhav.postgres.state import PostgresState

        m = _pg_model()
        m.target.full_name = "warehouse.orders"
        conn = self._conn()

        PostgresState(m, conn).record_failure(
            run_id=RUN_ID,
            interval=TZInterval(SINCE, UNTIL),
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
        from bollhav.model.intervals import TZInterval
        from bollhav.postgres.state import PostgresState

        m = _pg_model()
        conn = self._conn()
        PostgresState(m, conn).record_failure(
            run_id=RUN_ID,
            interval=TZInterval(SINCE, UNTIL),
            error_type="X",
            error_message="m",
            traceback_text=None,
            update_state=True,
        )

        executed = [str(c.args[0]) for c in conn.execute.call_args_list]
        assert any("UPDATE" in q and "status = 'error'" in q for q in executed)

    def test_update_state_false_logs_only(self) -> None:
        from bollhav.model.intervals import TZInterval
        from bollhav.postgres.state import PostgresState

        m = _pg_model()
        conn = self._conn()
        PostgresState(m, conn).record_failure(
            run_id=RUN_ID,
            interval=TZInterval(SINCE, UNTIL),
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
        from bollhav.postgres.state import PostgresState

        m = _pg_model()
        s = PostgresState(m, self._conn([])).read_status_summary()
        assert s["counts"] == {
            "pending": 0,
            "running": 0,
            "applied": 0,
            "blocked": 0,
            "error": 0,
        }
        assert s["blocked_groups"] == {}

    def test_counts_split_by_status(self) -> None:
        from bollhav.postgres.state import PostgresState

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
        s = PostgresState(m, self._conn(rows)).read_status_summary()
        assert s["counts"] == {
            "pending": 2,
            "running": 1,
            "applied": 1,
            "blocked": 1,
            "error": 2,
        }

    def test_blocked_groups_split_by_code_and_upstream(self) -> None:
        """Same code with different upstreams → separate groups."""
        from bollhav.postgres.state import PostgresState

        rows = [
            ("blocked", "STATE_001: upstream 'a.b' not registered"),
            ("blocked", "STATE_001: upstream 'a.b' not registered"),
            ("blocked", "STATE_001: upstream 'c.d' not registered"),
            ("blocked", "STATE_002: upstream 'a.b' has no applied row covering …"),
        ]
        m = _pg_model()
        s = PostgresState(m, self._conn(rows)).read_status_summary()
        # (code, upstream, kind) → count — these reasons carry no `(kind)`
        # descriptor, so kind is None.
        assert s["blocked_groups"] == {
            ("STATE_001", "a.b", None): 2,
            ("STATE_001", "c.d", None): 1,
            ("STATE_002", "a.b", None): 1,
        }

    def test_state_002_with_many_windows_collapses_to_one_group(self) -> None:
        """STATE_002's message includes a time window — many windows
        for the same (code, upstream) should still be ONE row in the
        banner. Verifies the group key ignores the window."""
        from bollhav.postgres.state import PostgresState

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
        s = PostgresState(m, self._conn(rows)).read_status_summary()
        assert s["blocked_groups"] == {("STATE_002", "a.b", None): 3}

    def test_blocked_reason_without_upstream_in_message(self) -> None:
        """Falls back to None for upstream when message lacks the
        `upstream 'X'` shape — banner will render '(upstream unknown)'."""
        from bollhav.postgres.state import PostgresState

        rows = [("blocked", "STATE_999: something custom")]
        m = _pg_model()
        s = PostgresState(m, self._conn(rows)).read_status_summary()
        assert s["blocked_groups"] == {("STATE_999", None, None): 1}

    def test_blocked_without_reason_is_ignored_in_groups(self) -> None:
        from bollhav.postgres.state import PostgresState

        rows = [
            ("blocked", None),
            ("blocked", ""),
            ("blocked", "STATE_001: upstream 'a.b' not registered"),
        ]
        m = _pg_model()
        s = PostgresState(m, self._conn(rows)).read_status_summary()
        assert s["counts"]["blocked"] == 3
        assert s["blocked_groups"] == {("STATE_001", "a.b", None): 1}


# ── @load_models bootstrap: contract → state → filter ───────────────


class TestStateMode_EnvVar:
    """STATE_MODE env var → cfg.state_mode → bootstrap argument."""

    def test_default_is_respect(self) -> None:
        from bollhav.model.load_models import _resolve_state_mode

        with patch(
            "bollhav.model.load_models.env_var",
            lambda name, **kw: None,
        ):
            assert _resolve_state_mode() is StateMode.DISCOVER

    def test_respect_explicit(self) -> None:
        from bollhav.model.load_models import _resolve_state_mode

        with patch(
            "bollhav.model.load_models.env_var",
            lambda name, **kw: "discover" if name == "STATE_MODE" else None,
        ):
            assert _resolve_state_mode() is StateMode.DISCOVER

    def test_disrespect(self) -> None:
        from bollhav.model.load_models import _resolve_state_mode

        with patch(
            "bollhav.model.load_models.env_var",
            lambda name, **kw: "bulldozer" if name == "STATE_MODE" else None,
        ):
            assert _resolve_state_mode() is StateMode.BULLDOZER

    def test_unknown_raises(self) -> None:
        from bollhav.model.load_models import _resolve_state_mode

        with patch(
            "bollhav.model.load_models.env_var",
            lambda name, **kw: "bogus" if name == "STATE_MODE" else None,
        ):
            with pytest.raises(ValueError, match="STATE_MODE must be one of"):
                _resolve_state_mode()

    def test_case_insensitive(self) -> None:
        from bollhav.model.load_models import _resolve_state_mode

        with patch(
            "bollhav.model.load_models.env_var",
            lambda name, **kw: "  BullDozer  " if name == "STATE_MODE" else None,
        ):
            assert _resolve_state_mode() is StateMode.BULLDOZER


class TestModelIntervals:
    """`ModelRun.intervals` is a plain attribute: the bootstrap computes the
    contract via `compute_intervals(run)` and assigns the actionable subset
    onto it; the user's loop reads it back. The `Model` definition is frozen."""

    def test_assignment_holds(self) -> None:
        from bollhav.model import (
            Batch,
            TimeChunking,
            Kind,
            Model,
            ModelRun,
            Target,
        )

        m = Model(
            target=Target(name="orders", schema="public"),
            batching=Batch(time=TimeChunking(chunk="@daily")),
            kind=Kind.INTERVAL,
        )
        run = ModelRun(model=m)
        run.intervals = ("fake1", "fake2")
        assert run.intervals == ("fake1", "fake2")

    def test_model_is_frozen(self) -> None:
        from bollhav.model import Kind, Model, Target

        m = Model(target=Target(name="orders", schema="public"), kind=Kind.MONOLITHIC)
        with pytest.raises(AttributeError, match="frozen"):
            m.state = None  # type: ignore[misc]

    def test_compute_intervals_is_independent_of_assignment(self) -> None:
        from datetime import timezone

        from bollhav.model import (
            Batch,
            Bounds,
            TimeChunking,
            Kind,
            Model,
            Target,
        )
        from bollhav.model.modelrun import ModelRun
        from bollhav.model.window import compute_intervals, resolve_window

        batching = Batch(time=TimeChunking(chunk="@daily"))
        bounds = Bounds(begin=datetime(2024, 1, 1, tzinfo=timezone.utc))
        m = Model(
            target=Target(name="orders", schema="public"),
            batching=batching,
            bounds=bounds,
            kind=Kind.INTERVAL,
        )
        run = ModelRun(
            model=m,
            window=resolve_window(
                batching, bounds, until=datetime(2024, 1, 3, tzinfo=timezone.utc)
            ),
        )
        # Assigning the attribute doesn't perturb the pure computation.
        run.intervals = ()
        assert run.intervals == ()
        assert len(compute_intervals(run)) > 0


class TestClearStateGuard:
    """`clear_state` wipes a model's state (table + library + error rows). It
    refuses on a model with no schema suffix — that state lives in prod
    `z_bollhav`, and clearing prod state isn't offered (do it by hand). The
    refusal fires before any DB access, so no connection is needed here. The
    actual wipe is covered against a real Postgres in test_e2e."""

    def test_refuses_without_schema_suffix(self) -> None:
        from bollhav.postgres.state import PostgresState

        with pytest.raises(ValueError, match="no schema suffix"):
            PostgresState(_pg_model(), conn=None).clear_state()


class TestDropEnvironmentGuard:
    """`drop_environment` tears down a suffixed environment's schemas. It
    refuses unless at least one model carries a schema suffix — otherwise it
    would target prod. The refusal fires before any DB access."""

    def test_refuses_when_no_model_has_suffix(self) -> None:
        from bollhav.postgres.state import drop_environment

        with pytest.raises(ValueError, match="no model carries a schema suffix"):
            drop_environment(MagicMock(), [_pg_model()])


class TestAssumeOkUpstream:
    """A gated upstream declared `assume_ok=True` is a shared/prod dependency.
    In a SUFFIXED (dev/PR) run gating assumes its state is okay and never looks
    it up; in a prod (unsuffixed) run it gates normally — so the flag needs no
    flipping between environments. Read off the Source; nothing is mutated."""

    def _src(self):
        from bollhav.model import IntervalContract, Source, SourceModel

        return Source(
            "warehouse.orders",
            type=SourceModel(),
            contract=IntervalContract(),
            assume_ok=True,
        )

    def test_suffixed_run_assumes_okay_and_skips_lookup(self) -> None:
        from bollhav.postgres.state import PostgresState

        model = MagicMock()
        model.target.schema_suffix = "pr123"  # dev/PR env
        model.gated_upstreams = [self._src()]
        with patch.object(PostgresState, "lookup_model") as lookup:
            check = PostgresState(model, conn=MagicMock()).is_upstream_satisfied_live(
                None
            )
        assert check.satisfied is True  # not a blocker
        lookup.assert_not_called()  # never consulted the library

    def test_prod_run_gates_normally(self) -> None:
        from bollhav.postgres.state import PostgresState

        model = MagicMock()
        model.target.schema_suffix = ""  # prod — no suffix
        model.target.schema_suffix_appendix = None
        model.gated_upstreams = [self._src()]
        with (
            patch.object(
                PostgresState, "lookup_model", return_value=MagicMock()
            ) as lookup,
            patch.object(PostgresState, "is_satisfied", return_value=True),
        ):
            check = PostgresState(model, conn=MagicMock()).is_upstream_satisfied_live(
                None
            )
        assert lookup.called  # gating engaged — the upstream IS looked up in prod
        assert check.satisfied is True
