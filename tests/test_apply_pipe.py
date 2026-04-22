from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from bollhav.model.apply_pipe import _apply_to_model
from bollhav.model.batch import Batch, ChunkMode, IntervalChunks, RowChunks
from bollhav.model.bounds import Bounds
from bollhav.model.model import Model
from bollhav.model.target_schema import TargetSchema
from bollhav.model.target import Target
from bollhav.pipe.pipe_config import PipeConfig, LatestConfig, BackfillConfig


CET = ZoneInfo("Europe/Stockholm")
UTC = timezone.utc


def _pipe(
    schema_suffix: str = "dev",
    latest: bool = False,
    backfill_since: datetime | None = None,
    backfill_until: datetime | None = None,
    interval_expression_override: str | None = None,
    window_expression_override: str | None = None,
    tz_override=None,
) -> PipeConfig:
    return PipeConfig(
        tags="test",
        latest=LatestConfig(enabled=latest),
        backfill=BackfillConfig(
            enabled=not latest, since=backfill_since, until=backfill_until
        ),
        schema_suffix=schema_suffix,
        interval_expression_override=interval_expression_override,
        window_expression_override=window_expression_override,
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


class TestApplyPipeDoesNotMutate:
    def test_original_untouched(self) -> None:
        m = _model()
        _apply_to_model(m, _pipe(schema_suffix="stg"))
        assert m.target.schema.suffix == ""
        assert m.target.schema.name == "public"

    def test_returns_new_model(self) -> None:
        m = _model()
        out = _apply_to_model(m, _pipe())
        assert out is not m


class TestSchemaSuffix:
    def test_schema_suffix_baked_in(self) -> None:
        m = _apply_to_model(_model(), _pipe(schema_suffix="pr123"))
        assert m.target.schema.suffix == "pr123"
        # schema.name stays as the base; .resolved is the suffixed form.
        assert m.target.schema.name == "public"
        assert m.target.schema.resolved == "public_pr123"


class TestPipeOverrides:
    def test_interval_expression_override(self) -> None:
        m = _apply_to_model(
            _model(expression="@daily"),
            _pipe(interval_expression_override="0 * * * *"),
        )
        assert m.batching.interval.expression == "0 * * * *"

    def test_window_expression_override(self) -> None:
        m = _apply_to_model(
            _model(), _pipe(latest=True, window_expression_override="@daily")
        )
        assert m.batching.interval.window_expression == "@daily"

    def test_tz_override(self) -> None:
        m = _apply_to_model(_model(tz=UTC), _pipe(tz_override=CET))
        assert m.batching.interval.tz == CET

    def test_overrides_skipped_when_unset(self) -> None:
        m = _apply_to_model(_model(expression="@daily", tz=CET), _pipe())
        assert m.batching.interval.expression == "@daily"
        assert m.batching.interval.tz == CET


class TestDirectives:
    def test_latest_set_from_pipe(self) -> None:
        m = _apply_to_model(_model(), _pipe(latest=True))
        assert m.directives.latest is True

    def test_latest_forced_off_when_reload(self) -> None:
        m = _model()
        m.directives.reload = True
        out = _apply_to_model(m, _pipe(latest=True))
        assert out.directives.latest is False
        assert out.directives.reload is True

    def test_since_and_until_from_pipe(self) -> None:
        since = datetime(2024, 1, 1, tzinfo=UTC)
        until = datetime(2024, 2, 1, tzinfo=UTC)
        m = _apply_to_model(
            _model(),
            _pipe(backfill_since=since, backfill_until=until),
        )
        assert m.directives.since == since
        assert m.directives.until == until


class TestTagReloadBakedIntoBatching:
    def test_reload_mode_baked_into_batching(self) -> None:
        m = Model(
            target=Target(
                name="events", schema=TargetSchema(name="raw", suffix_appendix=None)
            ),
            batching=Batch(
                mode=ChunkMode.INTERVAL,
                interval=IntervalChunks(expression="@hourly", tz=UTC),
                row=RowChunks(batch_size=5000),
            ),
            bounds=Bounds(begin=datetime(2024, 1, 1, tzinfo=UTC)),
        )
        m.directives.reload = True
        m.directives.reload_mode = ChunkMode.ROW
        m.directives.reload_batch_size = 100

        out = _apply_to_model(m, _pipe())
        assert out.batching.mode is ChunkMode.ROW
        assert out.batching.row.batch_size == 100
        # Overrides absorbed — directives fields cleared.
        assert out.directives.reload_mode is None
        assert out.directives.reload_batch_size is None

    def test_reload_interval_expression_baked_into_batching(self) -> None:
        m = _model(expression="@hourly")
        m.directives.reload = True
        m.directives.reload_interval_expression = "@daily"

        out = _apply_to_model(m, _pipe())
        assert out.batching.interval.expression == "@daily"
        assert out.directives.reload_interval_expression is None

    def test_pipe_override_wins_over_tag_reload_expression(self) -> None:
        """If both a tag and the pipe supply an interval expression, the
        pipe-level env override wins — the explicit runtime knob beats the
        tag annotation."""
        m = _model(expression="@hourly")
        m.directives.reload = True
        m.directives.reload_interval_expression = "@daily"

        out = _apply_to_model(m, _pipe(interval_expression_override="*/15 * * * *"))
        assert out.batching.interval.expression == "*/15 * * * *"


class TestBatchingNone:
    def test_no_batching_skips_interval_baking(self) -> None:
        m = Model(target=Target(name="t", schema=TargetSchema(suffix_appendix=None)))
        out = _apply_to_model(m, _pipe(interval_expression_override="@daily"))
        assert out.batching is None
