from datetime import timezone
from zoneinfo import ZoneInfo

from bollhav.model.runtime_override import RuntimeOverride
from bollhav.pipe.pipe_config import PipeConfig, LatestConfig, BackfillConfig


CET = ZoneInfo("Europe/Stockholm")
UTC = timezone.utc


def _pipe(
    schema_suffix="dev",
    batch_expression_override=None,
    tz_override=None,
) -> PipeConfig:
    return PipeConfig(
        tags="test",
        latest=LatestConfig(enabled=False),
        backfill=BackfillConfig(enabled=True, since=None, until=None),
        schema_suffix=schema_suffix,
        batch_expression_override=batch_expression_override,
        tz_override=tz_override,
    )


class TestRuntimeOverrideDefaults:
    def test_defaults(self) -> None:
        rt = RuntimeOverride()
        assert rt.reload is False
        assert rt.schema_suffix == ""
        assert rt.batch_expression is None
        assert rt.tz is None


class TestApplyPipe:
    def test_applies_schema_suffix(self) -> None:
        rt = RuntimeOverride()
        rt.apply_pipe(_pipe(schema_suffix="staging"))
        assert rt.schema_suffix == "staging"

    def test_applies_batch_expression(self) -> None:
        rt = RuntimeOverride()
        rt.apply_pipe(_pipe(batch_expression_override="0 * * * *"))
        assert rt.batch_expression == "0 * * * *"

    def test_applies_tz(self) -> None:
        rt = RuntimeOverride()
        rt.apply_pipe(_pipe(tz_override=CET))
        assert rt.tz == CET

    def test_none_values_clear_previous(self) -> None:
        rt = RuntimeOverride(batch_expression="0 0 * * *", tz=CET)
        rt.apply_pipe(_pipe())
        assert rt.batch_expression is None
        assert rt.tz is None

    def test_preserves_reload(self) -> None:
        rt = RuntimeOverride(reload=True)
        rt.apply_pipe(_pipe(schema_suffix="prod", tz_override=UTC))
        assert rt.reload is True
        assert rt.schema_suffix == "prod"
        assert rt.tz == UTC
