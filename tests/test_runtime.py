from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from bollhav.model.runtime import _apply_to_model
from bollhav.model.batch import Batch, IntervalChunks
from bollhav.model.bounds import Bounds
from bollhav.model.model import Model
from bollhav.model.target import Target
from bollhav.model.target_schema import TargetSchema


CET = ZoneInfo("Europe/Stockholm")
UTC = timezone.utc


def _apply(
    model: Model,
    *,
    schema_suffix: str = "dev",
    latest: bool = False,
    backfill_since: datetime | None = None,
    backfill_until: datetime | None = None,
    interval_expression_override: str | None = None,
    window_expression_override: str | None = None,
    lookback_override: int | None = None,
    tz_override=None,
) -> Model:
    return _apply_to_model(
        model,
        schema_suffix=schema_suffix,
        latest=latest,
        backfill_since=backfill_since,
        backfill_until=backfill_until,
        interval_expression_override=interval_expression_override,
        window_expression_override=window_expression_override,
        lookback_override=lookback_override,
        tz_override=tz_override,
    )


def _model(**batch_kwargs) -> Model:
    iv = {"expression": "@hourly", "tz": UTC}
    iv.update(batch_kwargs)
    return Model(
        target=Target(
            name="orders", schema=TargetSchema(name="public", suffix_appendix=None)
        ),
        batching=Batch(interval=IntervalChunks(**iv)),
    )


class TestApplyDoesNotMutate:
    def test_original_untouched(self) -> None:
        m = _model()
        _apply(m, schema_suffix="stg")
        assert m.target.schema.suffix == ""
        assert m.target.schema.name == "public"

    def test_returns_new_model(self) -> None:
        m = _model()
        out = _apply(m)
        assert out is not m


class TestSchemaSuffix:
    def test_schema_suffix_baked_in(self) -> None:
        m = _apply(_model(), schema_suffix="pr123")
        assert m.target.schema.suffix == "pr123"
        # schema.name stays as the base; .resolved is the suffixed form.
        assert m.target.schema.name == "public"
        assert m.target.schema.resolved == "public_pr123"


class TestPipeOverrides:
    def test_interval_expression_override(self) -> None:
        m = _apply(
            _model(expression="@daily"),
            interval_expression_override="0 * * * *",
        )
        assert m.batching.interval.expression == "0 * * * *"

    def test_window_expression_override(self) -> None:
        m = _apply(_model(), latest=True, window_expression_override="@daily")
        assert m.batching.interval.window_expression == "@daily"

    def test_lookback_override(self) -> None:
        m = _apply(_model(lookback=2), lookback_override=5)
        assert m.batching.interval.lookback == 5

    def test_lookback_override_zero_clears(self) -> None:
        # 0 is a valid explicit value: "no lookback", and must win over a
        # model-set non-None lookback.
        m = _apply(_model(lookback=3), lookback_override=0)
        assert m.batching.interval.lookback == 0

    def test_tz_override(self) -> None:
        m = _apply(_model(tz=UTC), tz_override=CET)
        assert m.batching.interval.tz == CET

    def test_overrides_skipped_when_unset(self) -> None:
        m = _apply(_model(expression="@daily", tz=CET, lookback=2))
        assert m.batching.interval.expression == "@daily"
        assert m.batching.interval.tz == CET
        assert m.batching.interval.lookback == 2


class TestDirectives:
    def test_latest_set_from_arg(self) -> None:
        m = _apply(_model(), latest=True)
        assert m.directives.latest is True

    def test_latest_forced_off_when_reload(self) -> None:
        m = _model()
        m.directives.reload = True
        out = _apply(m, latest=True)
        assert out.directives.latest is False
        assert out.directives.reload is True

    def test_since_and_until_from_args(self) -> None:
        since = datetime(2024, 1, 1, tzinfo=UTC)
        until = datetime(2024, 2, 1, tzinfo=UTC)
        m = _apply(_model(), backfill_since=since, backfill_until=until)
        assert m.directives.since == since
        assert m.directives.until == until


class TestBatchingCarryThrough:
    def test_size_carried_through(self) -> None:
        m = Model(
            target=Target(
                name="events", schema=TargetSchema(name="raw", suffix_appendix=None)
            ),
            batching=Batch(
                interval=IntervalChunks(expression="@hourly", tz=UTC),
                size=5000,
            ),
            bounds=Bounds(begin=datetime(2024, 1, 1, tzinfo=UTC)),
        )
        out = _apply(m)
        assert out.batching.size == 5000

    def test_pipe_override_sets_interval_expression(self) -> None:
        m = _model(expression="@hourly")
        out = _apply(m, interval_expression_override="*/15 * * * *")
        assert out.batching.interval.expression == "*/15 * * * *"


class TestBatchingNone:
    def test_no_batching_skips_interval_baking(self) -> None:
        m = Model(target=Target(name="t", schema=TargetSchema(suffix_appendix=None)))
        out = _apply(m, interval_expression_override="@daily")
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
            target=Target(
                name="orders", schema=TargetSchema(name="public", suffix_appendix=None)
            ),
            batching=Batch(interval=IntervalChunks(expression="@hourly", tz=UTC)),
            state=s,
        )
        out = _apply(m)
        assert out.state is s

    def test_state_None_stays_None(self) -> None:
        out = _apply(_model())
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
                schema=TargetSchema(name="public", suffix_appendix=None),
                staging=staging_cfg,
            ),
            batching=Batch(interval=IntervalChunks(expression="@hourly", tz=UTC)),
            state=State(),  # staging requires state
        )
        out = _apply(m)
        out_staging = out.target.staging
        assert out_staging is not None
        assert out_staging is staging_cfg
        assert out_staging.schema == "ops"
        assert out_staging.table_prefix == "stg_"
        assert out_staging.logged is True
        assert out_staging.keep_after_apply is True

    def test_staging_None_stays_None(self) -> None:
        out = _apply(_model())
        assert out.target.staging is None
