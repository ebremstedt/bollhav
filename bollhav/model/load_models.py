from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, tzinfo
from functools import wraps
from typing import Callable
from zoneinfo import ZoneInfo

from roskarl import (
    env_var,
    env_var_bool,
    env_var_int,
    env_var_interval_expression,
    env_var_iso8601_datetime,
)

from uuid import uuid4

from bollhav.model.runtime import apply_runtime_overrides
from bollhav.model.model import Model
from bollhav.model.ordering import UpstreamMode
from bollhav.model.progress_bar import get_progress_level
from bollhav.model.state import StateMode


@dataclass
class _RuntimeConfig:
    """Internal: env-var read result. Not exposed."""

    tags: str
    schema_suffix: str
    upstream_mode: UpstreamMode
    latest: bool
    backfill_enabled: bool
    backfill_since: datetime | None
    backfill_until: datetime | None
    interval_expression_override: str | None
    window_expression_override: str | None
    lookback_override: int | None
    tz_override: tzinfo | None
    dry_run: bool
    dry_run_extra: bool
    state_mode: StateMode
    discover: bool
    debug: bool
    table_suffix: str = ""


def load_models(
    _func: Callable[..., None] | None = None,
    *,
    folder: str = "src/models",
) -> Callable[..., None]:
    """Decorator that reads runtime overrides from env vars, matches models
    in `folder` by `TAGS`, bakes the overrides in, and calls the wrapped
    function with `(models, debug)`.

    Usage:
        @load_models
        def main(models: list[Model], debug: bool) -> None:
            for model in models:
                ...

    or with a custom folder:

        @load_models(folder="custom/models")
        def main(models: list[Model], debug: bool) -> None:
            ...

    Env vars consumed:
        TAGS                          (required)
        SCHEMA_SUFFIX                 (required when USE_SCHEMA_SUFFIX=true)
        USE_SCHEMA_SUFFIX             default true
        TABLE_SUFFIX                  (required when USE_TABLE_SUFFIX=true)
        USE_TABLE_SUFFIX              default false
        DEBUG                         default false
        TIMEZONE_OVERRIDE             IANA timezone name
        LATEST_ENABLED                default false
        BACKFILL_ENABLED              default !LATEST_ENABLED
        BACKFILL_SINCE                ISO 8601 datetime
        BACKFILL_UNTIL                ISO 8601 datetime
        INTERVAL_EXPRESSION_OVERRIDE  cron / @alias
        WINDOW_EXPRESSION_OVERRIDE    cron / @alias (latest mode only)
        LOOKBACK_OVERRIDE             non-negative int (cron-ticks)
        UPSTREAM                      enforce | ignore_views | ignore_completely
        DRY_RUN                       bool; when true, print a concise summary
                                      of matched models (cron, interval count)
                                      and exit without invoking the wrapped
                                      function
        DRY_RUN_EXTRA                 bool; same short-circuit as DRY_RUN but
                                      prints the exhaustive per-model block
                                      (schema, bounds, tags, source, upstream,
                                      …). Implies DRY_RUN=true
        STATE_MODE                    respect (default) | disrespect
        DISCOVER                      bool; when true, intervals come from the
                                      state table (pending rows) instead of from
                                      bounds/backfill — only state-enabled
                                      models do any work
    """

    def decorator(func: Callable[..., None]) -> Callable[[], None]:
        @wraps(func)
        def wrapper() -> None:
            cfg = _read_env()
            _print_summary(cfg)
            models = apply_runtime_overrides(
                folder=folder,
                tags=cfg.tags,
                schema_suffix=cfg.schema_suffix,
                table_suffix=cfg.table_suffix,
                upstream_mode=cfg.upstream_mode,
                latest=cfg.latest,
                backfill_since=cfg.backfill_since,
                backfill_until=cfg.backfill_until,
                interval_expression_override=cfg.interval_expression_override,
                window_expression_override=cfg.window_expression_override,
                lookback_override=cfg.lookback_override,
                tz_override=cfg.tz_override,
            )
            if cfg.dry_run:
                from bollhav.model.dry_run import print_summary

                print_summary(models, cfg)
                return
            _compute_intervals_and_prefill_state(models, cfg)
            func(models=models, debug=cfg.debug)

        return wrapper

    # Allow both @load_models and @load_models(folder=...).
    if _func is None:
        return decorator
    return decorator(_func)


def _read_env() -> _RuntimeConfig:
    latest = env_var_bool(name="LATEST_ENABLED", default=False)
    backfill_enabled = env_var_bool(name="BACKFILL_ENABLED", default=not latest)
    if latest and backfill_enabled:
        raise ValueError("LATEST_ENABLED and BACKFILL_ENABLED cannot both be true")

    discover = env_var_bool(name="DISCOVER", default=False)
    if discover and latest:
        raise ValueError("DISCOVER and LATEST_ENABLED cannot both be true")

    tz_override = _resolve_tz_override()

    def _backfill_dt(name: str) -> datetime | None:
        if not backfill_enabled:
            return None
        dt = env_var_iso8601_datetime(name=name)
        return _apply_tz(dt, tz_override) if tz_override else dt

    use_schema_suffix = env_var_bool(name="USE_SCHEMA_SUFFIX", default=True)
    raw_suffix = env_var(name="SCHEMA_SUFFIX", default="")
    if use_schema_suffix and raw_suffix == "":
        raise ValueError("USE_SCHEMA_SUFFIX=True requires non-empty SCHEMA_SUFFIX")
    schema_suffix = raw_suffix if use_schema_suffix else ""

    use_table_suffix = env_var_bool(name="USE_TABLE_SUFFIX", default=False)
    raw_table_suffix = env_var(name="TABLE_SUFFIX", default="")
    if use_table_suffix and raw_table_suffix == "":
        raise ValueError("USE_TABLE_SUFFIX=True requires non-empty TABLE_SUFFIX")
    table_suffix = raw_table_suffix if use_table_suffix else ""

    window_expression_override = env_var_interval_expression(
        name="WINDOW_EXPRESSION_OVERRIDE", should_print_unset=False
    )
    if window_expression_override and not latest:
        raise ValueError(
            "WINDOW_EXPRESSION_OVERRIDE only applies when LATEST_ENABLED=True — "
            "in backfill mode since/until are set explicitly and no window is inferred"
        )

    lookback_override = env_var_int(name="LOOKBACK_OVERRIDE", should_print_unset=False)
    if lookback_override is not None and lookback_override < 0:
        raise ValueError(
            f"LOOKBACK_OVERRIDE must be non-negative, got {lookback_override}"
        )

    return _RuntimeConfig(
        tags=env_var(name="TAGS", required=True),
        schema_suffix=schema_suffix,
        table_suffix=table_suffix,
        upstream_mode=_resolve_upstream_mode(),
        latest=latest,
        backfill_enabled=backfill_enabled,
        backfill_since=_backfill_dt("BACKFILL_SINCE"),
        backfill_until=_backfill_dt("BACKFILL_UNTIL"),
        interval_expression_override=env_var_interval_expression(
            name="INTERVAL_EXPRESSION_OVERRIDE", should_print_unset=False
        ),
        window_expression_override=window_expression_override,
        lookback_override=lookback_override,
        tz_override=tz_override,
        dry_run=_resolve_dry_run(),
        dry_run_extra=env_var_bool(name="DRY_RUN_EXTRA", default=False),
        state_mode=_resolve_state_mode(),
        discover=discover,
        debug=env_var_bool(name="DEBUG", default=False),
    )


def _resolve_dry_run() -> bool:
    """DRY_RUN_EXTRA implies DRY_RUN — setting just the verbose flag
    should still short-circuit the wrapper."""
    return env_var_bool(name="DRY_RUN", default=False) or env_var_bool(
        name="DRY_RUN_EXTRA", default=False
    )


def _resolve_state_mode() -> StateMode:
    raw = env_var(name="STATE_MODE", should_print_unset=False)
    if raw is None:
        return StateMode.RESPECT
    valid = {m.value: m for m in StateMode}
    if raw not in valid:
        raise ValueError(
            f"STATE_MODE must be one of {list(valid.keys())}, got {raw!r}"
        )
    return valid[raw]


def _resolve_tz_override() -> tzinfo | None:
    raw = env_var(name="TIMEZONE_OVERRIDE", should_print_unset=False)
    if raw is None:
        return None
    try:
        return ZoneInfo(raw)
    except KeyError:
        raise ValueError(f"TIMEZONE_OVERRIDE is not a valid IANA timezone: {raw!r}")


def _apply_tz(dt: datetime | None, tz: tzinfo) -> datetime | None:
    if dt is None:
        return None
    return dt.replace(tzinfo=tz)


def _resolve_upstream_mode() -> UpstreamMode:
    raw = env_var(name="UPSTREAM", should_print_unset=False)
    if raw is None:
        return UpstreamMode.ENFORCE
    valid = {m.value: m for m in UpstreamMode}
    if raw not in valid:
        raise ValueError(f"UPSTREAM must be one of {list(valid.keys())}, got {raw!r}")
    return valid[raw]


def _compute_intervals_and_prefill_state(
    models: list[Model], cfg: _RuntimeConfig
) -> None:
    """Compute each model's intervals once (cached on the model so the
    user's loop doesn't recompute) and, for state-enabled models, ensure
    the state tables exist and pre-fill pending rows.

    Carries `cfg.state_mode` through to the backend so a fresh
    invocation can pick `respect` (preserve applied) or `disrespect`
    (reset everything to pending).

    Under DISCOVER, the normal flow flips: state-enabled models get
    their intervals straight from the state table (no bounds/backfill
    computation, no fresh pre-fill). STATE_MODE still applies on top:

        DISCOVER + RESPECT    — read pending rows as-is, run them
        DISCOVER + DISRESPECT — reset every row to pending first, then
                                run the entire state table

    Non-state-enabled models get empty intervals — they show up in the
    user's loop but do no work."""
    from bollhav.postgres import state as pg_state

    directive_mode = _directive_mode_label(cfg)
    for model in models:
        if cfg.discover:
            if model.state is None:
                model.intervals = []
                continue
            run_id = uuid4()
            model._state_run_id = run_id
            pg_state.ensure_tables(model)
            if cfg.state_mode is StateMode.DISRESPECT:
                pg_state.reset_all_to_pending(model, run_id=run_id)
            model.intervals = pg_state.read_pending(model)
            continue

        model.intervals = model.infer_intervals()
        if model.state is None:
            continue
        # Stamp this invocation's run_id once per state-enabled model so
        # both pre-fill rows and later applied/error writes share it.
        run_id = uuid4()
        model._state_run_id = run_id

        pg_state.ensure_tables(model)
        intervals = [iv for iv in model.intervals if iv is not None]
        pg_state.prefill(
            model,
            run_id=run_id,
            intervals=intervals,
            directive_mode=directive_mode,
            state_mode=cfg.state_mode,
        )


def _directive_mode_label(cfg: _RuntimeConfig) -> str:
    if cfg.latest:
        return "latest"
    if cfg.backfill_enabled:
        return "backfill"
    return "reload"


def _print_summary(cfg: _RuntimeConfig) -> None:
    def _row(key: str, val: str) -> None:
        print(f"  {key:<18}{val}")

    def _date(dt: datetime | None) -> str:
        return dt.isoformat() if dt else "—"

    width = 28
    title = f"── runtime ── ( {get_progress_level().value} ) "
    print(title + "─" * max(0, width - len(title)))
    _row("tags", cfg.tags or "—")
    if cfg.latest:
        _row("mode", "latest")
    elif cfg.backfill_enabled:
        _row(
            "mode",
            f"backfill  {_date(cfg.backfill_since)} → {_date(cfg.backfill_until)}",
        )
    else:
        _row("mode", "off")
    if cfg.debug:
        _row("debug", "on")
    if cfg.dry_run:
        _row("dry run", "extra" if cfg.dry_run_extra else "concise")
    if cfg.schema_suffix:
        _row("schema suffix", cfg.schema_suffix)
    if cfg.table_suffix:
        _row("table suffix", cfg.table_suffix)
    if cfg.upstream_mode != UpstreamMode.ENFORCE:
        _row("upstream", cfg.upstream_mode.value)
    if cfg.discover or cfg.state_mode is not StateMode.RESPECT:
        _row("state", "discover" if cfg.discover else cfg.state_mode.value)
    if cfg.tz_override is not None:
        _row("tz override", str(cfg.tz_override))
    if cfg.interval_expression_override:
        _row("interval override", cfg.interval_expression_override)
    if cfg.lookback_override is not None:
        _row("lookback override", str(cfg.lookback_override))
    if cfg.latest and cfg.window_expression_override:
        _row("window override", cfg.window_expression_override)
    print("────────────────────────────")


__all__ = ["load_models", "Model"]
