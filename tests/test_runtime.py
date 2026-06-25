from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from time_machine import travel

from bollhav.model.runtime import _apply_to_model
from bollhav.model.batch import Batch, TimeChunking
from bollhav.model.contract import Contract
from bollhav.model.intervals import TZInterval
from bollhav.model.temporality import Temporality
from bollhav.model.model import Model
from bollhav.model.target import Target
from bollhav.model.window import latest_complete_interval


CET = ZoneInfo("Europe/Stockholm")
UTC = timezone.utc

# Default backfill window for `_apply` — deterministic, so tests that don't
# care about the window still resolve one (interval models always need a
# window now). Window-specific tests override these.
_SINCE = datetime(2024, 1, 1, tzinfo=UTC)
_UNTIL = datetime(2024, 2, 1, tzinfo=UTC)


def _apply(
    model: Model,
    *,
    reload: bool = False,
    schema_suffix: str = "dev",
    latest: bool = False,
    backfill_since: datetime | None = _SINCE,
    backfill_until: datetime | None = _UNTIL,
    interval_override: str | None = None,
    window_override: str | None = None,
    lookback_override: int | None = None,
    tz_override=None,
    state_mode=None,
) -> Model:
    from bollhav.model.state import StateMode

    return _apply_to_model(
        model,
        reload=reload,
        schema_suffix=schema_suffix,
        latest=latest,
        backfill_since=backfill_since,
        backfill_until=backfill_until,
        interval_override=interval_override,
        window_override=window_override,
        lookback_override=lookback_override,
        tz_override=tz_override,
        state_mode=state_mode or StateMode.DISCOVER,
    )


def _model(**batch_kwargs) -> Model:
    iv = {"chunk": "@hourly", "tz": UTC}
    iv.update(batch_kwargs)
    return Model(
        target=Target(name="orders", schema="public", schema_suffix_appendix=None),
        batching=Batch(time=TimeChunking(**iv)),
        temporality=Temporality.TEMPORAL,
    )


class TestApplyDoesNotMutate:
    def test_original_untouched(self) -> None:
        m = _model()
        _apply(m, schema_suffix="stg")
        assert m.target.schema_suffix == ""
        assert m.target.schema == "public"

    def test_returns_new_model(self) -> None:
        m = _model()
        out = _apply(m)
        assert out is not m


class TestSchemaSuffix:
    def test_schema_suffix_baked_in(self) -> None:
        m = _apply(_model(), schema_suffix="pr123").model
        assert m.target.schema_suffix == "pr123"
        # schema.name stays as the base; .resolved is the suffixed form.
        assert m.target.schema == "public"
        assert m.target.schema_resolved == "public_pr123"


class TestPipeOverrides:
    def test_interval_override_applies_to_flexible(self) -> None:
        # INTERVAL_OVERRIDE re-chunks only a flexible model (it can absorb it).
        m = _apply(
            _model(chunk="@daily", fixed_intervals=False),
            interval_override="0 * * * *",
        ).model
        assert m.batching.time.chunk == "0 * * * *"

    def test_interval_override_ignored_on_fixed(self) -> None:
        # A fixed grid can't be re-chunked at runtime without forking state, so
        # the override is ignored (logged at INFO) — the chunk is left as-is.
        m = _apply(
            _model(chunk="@daily"),  # fixed_intervals defaults to True
            interval_override="0 * * * *",
        ).model
        assert m.batching.time.chunk == "@daily"

    def test_window_override(self) -> None:
        m = _apply(_model(), latest=True, window_override="@daily").model
        assert m.batching.time.window == "@daily"

    def test_lookback_override(self) -> None:
        m = _apply(_model(lookback=2), lookback_override=5).model
        assert m.batching.time.lookback == 5

    def test_lookback_override_zero_clears(self) -> None:
        # 0 is a valid explicit value: "no lookback", and must win over a
        # model-set non-None lookback.
        m = _apply(_model(lookback=3), lookback_override=0).model
        assert m.batching.time.lookback == 0

    def test_tz_override(self) -> None:
        m = _apply(_model(tz=UTC), tz_override=CET).model
        assert m.batching.time.tz == CET

    def test_overrides_skipped_when_unset(self) -> None:
        m = _apply(_model(chunk="@daily", tz=CET, lookback=2)).model
        assert m.batching.time.chunk == "@daily"
        assert m.batching.time.tz == CET
        assert m.batching.time.lookback == 2


class TestWindowResolution:
    """`_apply_to_model` resolves the run window from contract + the run
    instruction and bakes it onto the new model (no directives, no stamping)."""

    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_latest_resolves_latest_complete_window(self) -> None:
        m = _apply(_model(), latest=True)
        assert m.window == latest_complete_interval("@hourly", UTC)

    @travel(datetime(2024, 6, 15, 14, 35, tzinfo=UTC))
    def test_reload_beats_latest(self) -> None:
        begin = datetime(2024, 1, 1, tzinfo=UTC)
        m = Model(
            target=Target(name="orders", schema="public", schema_suffix_appendix=None),
            batching=Batch(time=TimeChunking(chunk="@hourly", tz=UTC)),
            contract=Contract(begin=begin),
            temporality=Temporality.TEMPORAL,
        )
        out = _apply(m, reload=True, latest=True)
        # reload wins: window spans contract.begin .. latest complete tick,
        # not the single latest tick that `latest`-only would give.
        assert out.window.since == begin
        assert out.window.until == latest_complete_interval("@hourly", UTC).until

    def test_backfill_window_from_args(self) -> None:
        since = datetime(2024, 1, 1, tzinfo=UTC)
        until = datetime(2024, 2, 1, tzinfo=UTC)
        m = _apply(_model(), backfill_since=since, backfill_until=until)
        assert m.window == TZInterval(since, until)


class TestBatchingCarryThrough:
    """`_batching_with_overrides` rebuilds `TimeChunking` / `Batch` every run, so
    any field NOT overridden this run must survive. It uses `replace` (not an
    enumerating constructor), so a forgotten field can't silently revert to its
    default — the bug-class that made STATE_MODE a no-op."""

    def test_size_carried_through(self) -> None:
        m = Model(
            target=Target(name="events", schema="raw", schema_suffix_appendix=None),
            batching=Batch(
                time=TimeChunking(chunk="@hourly", tz=UTC),
                size=5000,
                retries=7,
            ),
            contract=Contract(begin=datetime(2024, 1, 1, tzinfo=UTC)),
            temporality=Temporality.TEMPORAL,
        )
        out = _apply(m).model
        assert out.batching.size == 5000
        assert out.batching.retries == 7  # Batch-level fields carry too

    def test_pipe_override_sets_interval_expression(self) -> None:
        m = _model(
            chunk="@hourly", fixed_intervals=False
        )  # flexible → override applies
        out = _apply(m, interval_override="*/15 * * * *").model
        assert out.batching.time.chunk == "*/15 * * * *"

    def test_timechunking_fields_survive_no_override(self) -> None:
        # Every TimeChunking field a no-override run doesn't touch is preserved.
        sthlm = ZoneInfo("Europe/Stockholm")
        m = _apply(_model(chunk="@daily", window="@weekly", lookback=3, tz=sthlm)).model
        t = m.batching.time
        assert (t.chunk, t.window, t.lookback, t.tz) == ("@daily", "@weekly", 3, sthlm)

    def test_lookback_zero_survives_no_override(self) -> None:
        # 0 is meaningful ("no lookback") and must not be dropped to the default.
        assert _apply(_model(lookback=0)).model.batching.time.lookback == 0

    def test_fixed_intervals_defaults_true_and_survives(self) -> None:
        # The attestation defaults True (grid), and a declared False carries
        # through the rebuild even though _batching_with_overrides never names
        # it — exactly what the `replace` refactor guarantees.
        assert _model().batching.time.fixed_intervals is True
        out = _apply(_model(fixed_intervals=False)).model
        assert out.batching.time.fixed_intervals is False


class TestBatchingNone:
    def test_no_batching_skips_interval_baking(self) -> None:
        m = Model(
            target=Target(name="t", schema="", schema_suffix_appendix=None),
            temporality=Temporality.TIMELESS,
        )
        out = _apply(m, interval_override="@daily").model
        assert out.batching is None


class TestStateAndStagingCarryThrough:
    """Regression: `apply_runtime_overrides` rebuilds the model + target,
    and used to silently drop `model.state` and `target.staging`. The
    runtime path is what `@load_models` calls, so dropping them meant
    the lifecycle hooks and the staged write path were unreachable
    after `@load_models` — the example pipeline appeared to do nothing."""

    def test_state_carries_through(self) -> None:
        from bollhav.model.state import State

        s = State()
        m = Model(
            target=Target(name="orders", schema="public", schema_suffix_appendix=None),
            batching=Batch(time=TimeChunking(chunk="@hourly", tz=UTC)),
            state=s,
            temporality=Temporality.TEMPORAL,
        )
        out = _apply(m).model
        # Rebuilt (to stamp STATE_MODE on), so value-equal rather than identical.
        assert out.state == s

    def test_state_None_stays_None(self) -> None:
        out = _apply(_model()).model
        assert out.state is None

    def test_state_mode_is_applied(self) -> None:
        # Regression: STATE_MODE used to be resolved + displayed but never
        # stamped onto model.state.mode, so bulldozer / torch were silent no-ops.
        from bollhav.model.state import State, StateMode

        m = Model(
            target=Target(name="orders", schema="public", schema_suffix_appendix=None),
            batching=Batch(time=TimeChunking(chunk="@hourly", tz=UTC)),
            state=State(),  # defaults to DISCOVER
            temporality=Temporality.TEMPORAL,
        )
        out = _apply(m, state_mode=StateMode.BULLDOZER).model
        assert out.state is not None
        assert out.state.mode is StateMode.BULLDOZER

    def test_state_mode_ignored_when_no_state(self) -> None:
        # A stateless model stays stateless regardless of STATE_MODE.
        from bollhav.model.state import StateMode

        out = _apply(_model(), state_mode=StateMode.BULLDOZER).model
        assert out.state is None

    def test_staging_carries_through(self) -> None:
        from bollhav.model.state import State
        from bollhav.postgres.staging import PostgresStaging

        staging_cfg = PostgresStaging(
            schema="ops", table_prefix="stg_", logged=True, keep_after_apply=True
        )
        m = Model(
            target=Target(
                name="orders",
                schema="public",
                schema_suffix_appendix=None,
                staging=staging_cfg,
            ),
            batching=Batch(time=TimeChunking(chunk="@hourly", tz=UTC)),
            state=State(),  # staging requires state
            temporality=Temporality.TEMPORAL,
        )
        out = _apply(m).model
        out_staging = out.target.staging
        assert out_staging is not None
        assert out_staging is staging_cfg
        assert out_staging.schema == "ops"
        assert out_staging.table_prefix == "stg_"
        assert out_staging.logged is True
        assert out_staging.keep_after_apply is True

    def test_staging_None_stays_None(self) -> None:
        out = _apply(_model()).model
        assert out.target.staging is None


class TestRunMode:
    """`is_reload` / `is_latest` / `is_backfill` on the ModelRun record which
    mode resolved the window — exactly one True, precedence reload > latest >
    backfill (matching resolve_window)."""

    def _bounded(self) -> Model:
        # reload needs contract.begin; latest/backfill don't mind it being set.
        return Model(
            target=Target(name="orders", schema="public", schema_suffix_appendix=None),
            batching=Batch(time=TimeChunking(chunk="@hourly", tz=UTC)),
            contract=Contract(begin=_SINCE, end=_UNTIL),
            temporality=Temporality.TEMPORAL,
        )

    def test_backfill_is_the_default(self):
        run = _apply(self._bounded())  # no reload, no latest; since/until present
        assert (run.is_reload, run.is_latest, run.is_backfill) == (False, False, True)

    def test_latest(self):
        run = _apply(self._bounded(), latest=True)
        assert (run.is_reload, run.is_latest, run.is_backfill) == (False, True, False)

    def test_reload(self):
        run = _apply(self._bounded(), reload=True)
        assert (run.is_reload, run.is_latest, run.is_backfill) == (True, False, False)

    def test_reload_wins_over_latest(self):
        run = _apply(self._bounded(), reload=True, latest=True)
        assert (run.is_reload, run.is_latest, run.is_backfill) == (True, False, False)

    def test_exactly_one_is_true(self):
        for kw in (
            {},
            {"latest": True},
            {"reload": True},
            {"reload": True, "latest": True},
        ):
            run = _apply(self._bounded(), **kw)
            assert [run.is_reload, run.is_latest, run.is_backfill].count(True) == 1, kw

    def test_bare_modelrun_has_all_false(self):
        from bollhav.model.modelrun import ModelRun

        run = ModelRun(model=self._bounded())
        assert (run.is_reload, run.is_latest, run.is_backfill) == (False, False, False)


class TestCurfewPreservedThroughOverrides:
    def test_curfew_survives_the_runtime_copy(self):
        from datetime import time

        from bollhav.model import Curfew

        m = Model(
            target=Target(name="orders", schema="public", schema_suffix_appendix=None),
            batching=Batch(time=TimeChunking(chunk="@hourly", tz=UTC)),
            contract=Contract(begin=_SINCE, end=_UNTIL),
            temporality=Temporality.TEMPORAL,
            curfew=Curfew.work_hours(),
        )
        run = _apply(m)
        assert run.model.curfew is not None
        assert run.model.curfew.windows == [(time(9), time(17))]


class TestRunModeWindowMatrix:
    """The run-mode combinatorics at the resolution layer: STATE_MODE
    (bulldozer / discover / torch) × window source (latest / explicit backfill /
    contract range). torch is the constrained one — it forbids an explicit
    window and always reloads the contract range."""

    def _m(self):
        from bollhav.model.state import State

        return Model(
            target=Target(name="t", schema="s", schema_suffix_appendix=None),
            batching=Batch(time=TimeChunking(chunk="@daily", tz=UTC)),
            state=State(),
            temporality=Temporality.TEMPORAL,
            contract=Contract(begin=_SINCE, end=_UNTIL),
        )

    @travel(datetime(2024, 1, 15, 12, 0, tzinfo=UTC))
    def test_latest_resolves_to_the_tick(self) -> None:
        from bollhav.model.state import StateMode

        for mode in (StateMode.BULLDOZER, StateMode.DISCOVER):
            run = _apply(
                self._m(),
                state_mode=mode,
                latest=True,
                backfill_since=None,
                backfill_until=None,
            )
            assert run.window.since == datetime(2024, 1, 14, tzinfo=UTC)
            assert run.window.until == datetime(2024, 1, 15, tzinfo=UTC)

    def test_explicit_backfill_resolves_to_the_range(self) -> None:
        from bollhav.model.state import StateMode

        s, u = datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 3, tzinfo=UTC)
        for mode in (StateMode.BULLDOZER, StateMode.DISCOVER):
            run = _apply(self._m(), state_mode=mode, backfill_since=s, backfill_until=u)
            assert (run.window.since, run.window.until) == (s, u)

    def test_backfill_no_dates_resolves_to_contract_range(self) -> None:
        from bollhav.model.state import StateMode

        for mode in (StateMode.BULLDOZER, StateMode.DISCOVER):
            run = _apply(
                self._m(), state_mode=mode, backfill_since=None, backfill_until=None
            )
            assert (run.window.since, run.window.until) == (_SINCE, _UNTIL)

    def test_torch_forbids_an_explicit_window(self) -> None:
        import pytest

        from bollhav.model.messages.error import TorchWithWindowError
        from bollhav.model.state import StateMode

        with pytest.raises(TorchWithWindowError):
            _apply(
                self._m(),
                state_mode=StateMode.TORCH,
                backfill_since=_SINCE,
                backfill_until=_UNTIL,
            )

    def test_torch_always_reloads_the_contract_range(self) -> None:
        from bollhav.model.state import StateMode

        # latest is ignored; no explicit dates → the contract range
        run = _apply(
            self._m(),
            state_mode=StateMode.TORCH,
            latest=True,
            backfill_since=None,
            backfill_until=None,
        )
        assert (run.window.since, run.window.until) == (_SINCE, _UNTIL)
