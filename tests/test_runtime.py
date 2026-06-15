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
) -> Model:
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
    def test_interval_override(self) -> None:
        m = _apply(
            _model(chunk="@daily"),
            interval_override="0 * * * *",
        ).model
        assert m.batching.time.chunk == "0 * * * *"

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
    def test_size_carried_through(self) -> None:
        m = Model(
            target=Target(name="events", schema="raw", schema_suffix_appendix=None),
            batching=Batch(
                time=TimeChunking(chunk="@hourly", tz=UTC),
                size=5000,
            ),
            contract=Contract(begin=datetime(2024, 1, 1, tzinfo=UTC)),
            temporality=Temporality.TEMPORAL,
        )
        out = _apply(m).model
        assert out.batching.size == 5000

    def test_pipe_override_sets_interval_expression(self) -> None:
        m = _model(chunk="@hourly")
        out = _apply(m, interval_override="*/15 * * * *").model
        assert out.batching.time.chunk == "*/15 * * * *"


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
        assert out.state is s

    def test_state_None_stays_None(self) -> None:
        out = _apply(_model()).model
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
