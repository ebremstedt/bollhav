"""Tests for the dry-run module + the @load_models DRY_RUN short-circuit."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from bollhav.model.dry_run import print_summary


SINCE = datetime(2024, 1, 1, tzinfo=timezone.utc)
UNTIL = datetime(2024, 1, 2, tzinfo=timezone.utc)


def _cfg(**overrides):
    from bollhav.model.load_models import _RuntimeConfig
    from bollhav.model.ordering import UpstreamMode
    from bollhav.model.state import StateMode

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
        dry_run=True,
        dry_run_extra=False,
        state_mode=StateMode.DISCOVER,
        state_disabled=False,
        peek=False,
        debug=False,
    )
    defaults.update(overrides)
    return _RuntimeConfig(**defaults)


def _mk_model(
    *,
    full_name: str = "public.orders",
    name: str = "orders",
    schema: str = "public",
    with_batching: bool = True,
):
    """Build a model-shaped MagicMock that the dry-run module accepts."""
    from bollhav.model.intervals import TZInterval

    model = MagicMock()
    model.target.full_name = full_name
    model.target.name = name
    model.target.name_resolved = name
    model.target.schema.resolved = schema
    model.target.write_mode.value = "APPEND"
    model.bounds.begin = None
    model.bounds.end = None
    model.tags = set()
    model.upstream = []
    model.source = None
    model.description = None
    if with_batching:
        model.batching = MagicMock()
        model.batching.interval.expression = "@daily"
        model.batching.interval.lookback = None
        model.batching.size = 10000
        # `intervals` is a @property on Model; MagicMock instances accept
        # arbitrary attribute assignment so we just set the value directly.
        model.intervals = [TZInterval(SINCE, UNTIL)]
    else:
        model.batching = None
    return model


class TestConcise:
    def test_renders_grouped_by_schema(self, capsys) -> None:
        a = _mk_model(full_name="public.orders", name="orders", schema="public")
        b = _mk_model(full_name="public.products", name="products", schema="public")
        c = _mk_model(full_name="analytics.events", name="events", schema="analytics")
        print_summary([a, b, c], _cfg())
        out = capsys.readouterr().out
        # Schema headers present
        assert "public:" in out
        assert "analytics:" in out
        # Table names without redundant schema prefix
        assert "  orders" in out
        assert "  products" in out
        assert "  events" in out

    def test_schemas_in_alphabetical_order(self, capsys) -> None:
        z = _mk_model(full_name="zeta.t", name="t", schema="zeta")
        a = _mk_model(full_name="alpha.t", name="t", schema="alpha")
        print_summary([z, a], _cfg())
        out = capsys.readouterr().out
        assert out.index("alpha:") < out.index("zeta:")

    def test_unbatched_model_renders_name_only(self, capsys) -> None:
        m = _mk_model(name="active_customers", with_batching=False)
        print_summary([m], _cfg())
        out = capsys.readouterr().out
        # Name appears, no trailing batching/cron column.
        assert "active_customers" in out
        assert "@" not in out  # no cron expression on the model's line
        assert "×" not in out

    def test_batched_model_shows_count_and_cron(self, capsys) -> None:
        m = _mk_model()
        print_summary([m], _cfg())
        out = capsys.readouterr().out
        assert "1 × @daily" in out

    def test_mixed_batched_and_unbatched_in_one_schema(self, capsys) -> None:
        """A single schema with batched and no-batching models — both
        should render side by side without crashing."""
        interval = _mk_model(full_name="public.a", name="a", schema="public")
        view = _mk_model(
            full_name="public.c", name="c", schema="public", with_batching=False
        )

        print_summary([interval, view], _cfg())
        out = capsys.readouterr().out
        assert "1 × @daily" in out  # interval
        # view: just the name, no tail
        assert "\n  c\n" in out  # padding-respecting: just the name on its own

    def test_single_model_in_schema(self, capsys) -> None:
        """One-model schema should still render cleanly (no padding issue)."""
        m = _mk_model(full_name="solo.t", name="t", schema="solo")
        print_summary([m], _cfg())
        out = capsys.readouterr().out
        assert "solo:" in out
        assert "  t   1 × @daily" in out


class TestExtra:
    def test_renders_full_block(self, capsys) -> None:
        m = _mk_model()
        print_summary([m], _cfg(dry_run_extra=True))
        out = capsys.readouterr().out
        assert "dry run (extra)" in out
        assert "schema       :" in out
        assert "write mode   :" in out
        assert "cron         : @daily" in out
        assert "intervals    : 1" in out
        assert "bounds       :" in out
        assert "tags         :" in out
        assert "upstream     :" in out

    def test_models_in_alphabetical_order(self, capsys) -> None:
        z = _mk_model(full_name="zeta.t")
        a = _mk_model(full_name="alpha.t")
        print_summary([z, a], _cfg(dry_run_extra=True))
        out = capsys.readouterr().out
        assert out.index("▸ alpha.t") < out.index("▸ zeta.t")

    def test_unbatched_model_extra_skips_batching_lines(self, capsys) -> None:
        m = _mk_model(with_batching=False)
        print_summary([m], _cfg(dry_run_extra=True))
        out = capsys.readouterr().out
        # No batching-related lines at all.
        assert "cron" not in out
        assert "intervals" not in out
        assert "batching" not in out


class TestTagTable:
    """The tag table should render in both concise + extra, alphabetical
    by group order in the expression. Parser errors are swallowed so the
    summary still produces something."""

    def test_tag_table_in_concise(self, capsys) -> None:
        print_summary([_mk_model()], _cfg(tags="[clean]|[orders]"))
        out = capsys.readouterr().out
        assert "tags (matches any):" in out
        assert "[clean]" in out
        assert "→  clean" in out
        assert "[orders]" in out

    def test_tag_table_in_extra(self, capsys) -> None:
        print_summary([_mk_model()], _cfg(dry_run_extra=True, tags="[clean]"))
        out = capsys.readouterr().out
        assert "tags:" in out
        assert "→  clean" in out

    def test_invalid_tag_expression_does_not_crash(self, capsys) -> None:
        # Bare string (no brackets) is invalid — should be silently
        # skipped rather than crashing the whole summary.
        print_summary([_mk_model()], _cfg(tags="not_a_valid_expression"))
        out = capsys.readouterr().out
        # Summary still rendered (schema + table appear).
        assert "public:" in out
        assert "orders" in out
        # Tag header is omitted because parsing failed.
        assert "tags" not in out


class TestHeader:
    def test_concise_header(self, capsys) -> None:
        print_summary([_mk_model()], _cfg())
        out = capsys.readouterr().out
        assert "── dry run ─" in out
        assert "extra" not in out.split("\n")[1]  # second line is the header

    def test_extra_header(self, capsys) -> None:
        print_summary([_mk_model()], _cfg(dry_run_extra=True))
        out = capsys.readouterr().out
        assert "── dry run (extra)" in out

    def test_mode_label_latest(self, capsys) -> None:
        print_summary([_mk_model()], _cfg(latest=True, backfill_enabled=False))
        out = capsys.readouterr().out
        assert "mode = latest" in out

    def test_pluralization(self, capsys) -> None:
        print_summary([_mk_model()], _cfg())
        assert "1 model matched" in capsys.readouterr().out
        print_summary([_mk_model(full_name="a.b"), _mk_model(full_name="c.d")], _cfg())
        assert "2 models matched" in capsys.readouterr().out


class TestLoadModelsShortCircuit:
    """Both DRY_RUN=true and DRY_RUN_EXTRA=true must skip user main()."""

    def _run_with(self, *, dry_run: bool, dry_run_extra: bool) -> bool:
        from bollhav.model.load_models import load_models

        called = {"main": False}
        bools = {
            "LATEST_ENABLED": False,
            "DRY_RUN": dry_run,
            "DRY_RUN_EXTRA": dry_run_extra,
            "DEBUG": False,
            "USE_SCHEMA_SUFFIX": False,
        }
        strs = {"TAGS": "[x]", "SCHEMA_SUFFIX": ""}

        with (
            patch(
                "bollhav.model.load_models.env_var_bool",
                lambda name, default=False: bools.get(name, default),
            ),
            patch(
                "bollhav.model.load_models.env_var",
                lambda name, required=False, default=None, should_print_unset=True: (
                    strs.get(name, default)
                ),
            ),
            patch(
                "bollhav.model.load_models.env_var_interval_expression",
                lambda name, should_print_unset=True: None,
            ),
            patch(
                "bollhav.model.load_models.env_var_int",
                lambda name, should_print_unset=True: None,
            ),
            patch(
                "bollhav.model.load_models.env_var_iso8601_datetime",
                lambda name: None,
            ),
            patch("bollhav.model.load_models.apply_runtime_overrides", return_value=[]),
            patch("bollhav.model.load_models._print_summary", lambda cfg, models: None),
            patch("bollhav.model.dry_run.print_summary"),
        ):

            @load_models
            def main(models, debug):
                called["main"] = True

            main()

        return called["main"]

    def test_dry_run_true_skips_main(self) -> None:
        assert self._run_with(dry_run=True, dry_run_extra=False) is False

    def test_dry_run_extra_true_skips_main(self) -> None:
        """DRY_RUN_EXTRA alone (without DRY_RUN) still short-circuits."""
        assert self._run_with(dry_run=False, dry_run_extra=True) is False

    def test_both_false_calls_main(self) -> None:
        assert self._run_with(dry_run=False, dry_run_extra=False) is True
