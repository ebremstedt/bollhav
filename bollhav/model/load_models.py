from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, tzinfo
from functools import wraps
from typing import Callable, cast, overload
from zoneinfo import ZoneInfo

from roskarl import (
    env_var,
    env_var_bool,
    env_var_int,
    env_var_interval_expression,
    env_var_iso8601_datetime,
)

from bollhav.model.runtime import apply_runtime_overrides
from bollhav.model.modelrun import ModelRun
from bollhav.model.window import compute_intervals
from bollhav.model.progress_bar import get_progress_level, PROGRESS, ProgressLevel
from bollhav.model.state import StateMode

import logging

logger = logging.getLogger(__name__)


# ── errors ──


class ConflictingRunModeError(ValueError):
    """`LATEST_ENABLED` and `BACKFILL_ENABLED` were both set — they pick
    mutually exclusive run modes, so exactly one (or neither) may be true."""

    def __init__(self) -> None:
        super().__init__("LATEST_ENABLED and BACKFILL_ENABLED cannot both be true")


class MissingSchemaSuffixError(ValueError):
    """`USE_SCHEMA_SUFFIX=True` was set without a non-empty `SCHEMA_SUFFIX` to
    apply, so there's nothing to suffix the schema with."""

    def __init__(self) -> None:
        super().__init__("USE_SCHEMA_SUFFIX=True requires non-empty SCHEMA_SUFFIX")


class MissingTableSuffixError(ValueError):
    """`USE_TABLE_SUFFIX=True` was set without a non-empty `TABLE_SUFFIX` to
    apply, so there's nothing to suffix the table with."""

    def __init__(self) -> None:
        super().__init__("USE_TABLE_SUFFIX=True requires non-empty TABLE_SUFFIX")


class WindowOverrideWithoutLatestError(ValueError):
    """`WINDOW_OVERRIDE` was set outside `LATEST_ENABLED` mode. It only adjusts
    the inferred latest-window; in backfill mode since/until are explicit and
    no window is inferred, so the override has nothing to act on."""

    def __init__(self) -> None:
        super().__init__(
            "WINDOW_OVERRIDE only applies when LATEST_ENABLED=True — "
            "in backfill mode since/until are set explicitly and no window is inferred"
        )


class NegativeLookbackError(ValueError):
    """`LOOKBACK_OVERRIDE` was given a negative value — a lookback extends the
    window backwards by a non-negative amount, so negatives are meaningless."""

    def __init__(self, value: int) -> None:
        super().__init__(f"LOOKBACK_OVERRIDE must be non-negative, got {value}")


class InvalidStateModeError(ValueError):
    """`STATE_MODE` was set to a value outside the known modes
    (`discover` / `bulldozer` / `torch`)."""

    def __init__(self, value: str, valid: list[str]) -> None:
        super().__init__(f"STATE_MODE must be one of {valid}, got {value!r}")


class InvalidTimezoneError(ValueError):
    """`TIMEZONE_OVERRIDE` was not a valid IANA timezone name (e.g.
    `Europe/Stockholm`), so it can't be resolved to a zone."""

    def __init__(self, value: str) -> None:
        super().__init__(f"TIMEZONE_OVERRIDE is not a valid IANA timezone: {value!r}")


@dataclass
class _RuntimeConfig:
    """Internal: env-var read result. Not exposed."""

    tags: str
    schema_suffix: str
    latest: bool
    backfill_enabled: bool
    backfill_since: datetime | None
    backfill_until: datetime | None
    interval_override: str | None
    window_override: str | None
    lookback_override: int | None
    tz_override: tzinfo | None
    dry_run: bool
    dry_run_extra: bool
    state_mode: StateMode
    state_disabled: bool
    debug: bool
    table_suffix: str = ""
    # DRY_STATE is resolved by `@model_lifecycle` (it needs a live connection),
    # but `@load_models` reads it too so it can keep the progress bar silent —
    # a dry run executes nothing, so its `▸ … 0ms` rows are pure noise.
    dry_state: bool = False


@overload
def load_models(_func: Callable[..., None]) -> Callable[[], None]: ...


@overload
def load_models(
    _func: None = None, *, folder: str = ...
) -> Callable[[Callable[..., None]], Callable[[], None]]: ...


def load_models(
    _func: Callable[..., None] | None = None,
    *,
    folder: str = "src/models",
) -> Callable[[], None] | Callable[[Callable[..., None]], Callable[[], None]]:
    """Decorator that reads runtime overrides from env vars, matches models
    in `folder` by `TAGS`, bakes the overrides in, resolves each run's window,
    and calls the wrapped function with `(runs, debug)` — one `ModelRun` per
    matched model.

    Usage:
        @load_models
        def main(runs: list[ModelRun], debug: bool) -> None:
            for run in runs:
                run_model(run, conn)

    or with a custom folder:

        @load_models(folder="custom/models")
        def main(runs: list[ModelRun], debug: bool) -> None:
            ...

    Env vars consumed:
        TAGS                          (required)
        SCHEMA_SUFFIX                 (required when USE_SCHEMA_SUFFIX=true)
        USE_SCHEMA_SUFFIX             default true
        TABLE_SUFFIX                  (required when USE_TABLE_SUFFIX=true)
        USE_TABLE_SUFFIX              default false
        DEBUG                         default false
        TIMEZONE_OVERRIDE             IANA timezone name
        LATEST_ENABLED                run the latest complete tick. The DEFAULT
                                      run mode (when neither flag is set).
        BACKFILL_ENABLED              a windowed range run. Mutually exclusive
                                      with latest. The window is RUN_SINCE/
                                      RUN_UNTIL, each falling back to the contract
                                      (begin / end-or-latest) — so a no-dates
                                      backfill runs the declared range.
        RUN_SINCE                     ISO 8601 datetime (optional; ?? contract.begin).
                                      The run window's start, used by any state
                                      mode. (alias: deprecated BACKFILL_SINCE)
        RUN_UNTIL                     ISO 8601 datetime (optional; ?? contract.end
                                      ?? latest complete tick). (alias: BACKFILL_UNTIL)
        INTERVAL_OVERRIDE  cron / @alias — re-chunks at runtime. Applies ONLY
                                      to flexible models (ChunkFlex);
                                      ignored (logged at INFO) on fixed models,
                                      since re-chunking a fixed grid forks its
                                      state — use STATE_MODE=torch for that.
        WINDOW_OVERRIDE    cron / @alias (latest mode only)
        LOOKBACK_OVERRIDE             non-negative int (cron-ticks)
        DRY_RUN                       bool; when true, print a concise summary
                                      of matched models (cron, interval count)
                                      and exit without invoking the wrapped
                                      function
        DRY_RUN_EXTRA                 bool; same short-circuit as DRY_RUN but
                                      prints the exhaustive per-model block
                                      (schema, bounds, tags, source, upstream,
                                      …). Implies DRY_RUN=true
        STATE_MODE                    bulldozer (default) | discover | torch.
                                      How much state a run invalidates within its
                                      window. bulldozer = reset the window's rows
                                      to pending and run exactly them (leave the
                                      rest); discover = skip already-applied (run
                                      only the window's outstanding); torch =
                                      DELETE all rows + reload the contract range
                                      (forbids an explicit window — destructive).
        STATE_DISABLED                bool; when true, force the pipeline to
                                      run with NO state tracking — even for
                                      models that declare state=State(...).
                                      Useful for ad-hoc/dev runs against a DB
                                      where the state tables don't exist or
                                      you don't want to touch them. Nulls
                                      `state` + `target.staging` so the
                                      lifecycle hooks pass through and write()
                                      uses the direct path.
        STATE_MARK_APPLIED            bool; when true, stamp the matched models'
                                      window intervals `applied` in state WITHOUT
                                      running them — for recording an out-of-band
                                      load (a manual / STATE_DISABLED bulk load)
                                      so the daily incremental won't reload it.
                                      Scoped to `compute_intervals(run)` (the
                                      supplied window + chunk), never the backlog.
                                      No DDL, no data writes. An assertion, not a
                                      verification — only what you select
    """

    def decorator(func: Callable[..., None]) -> Callable[[], None]:
        @wraps(func)
        def wrapper() -> None:
            cfg = _read_env()
            if (
                "LATEST_ENABLED" not in os.environ
                and "BACKFILL_ENABLED" not in os.environ
            ):
                logger.debug(
                    "no run mode set — defaulting to latest (the latest complete "
                    "tick); set BACKFILL_ENABLED=true for a range run"
                )
            runs = apply_runtime_overrides(
                folder=folder,
                tags=cfg.tags,
                schema_suffix=cfg.schema_suffix,
                table_suffix=cfg.table_suffix,
                latest=cfg.latest,
                backfill_since=cfg.backfill_since,
                backfill_until=cfg.backfill_until,
                interval_override=cfg.interval_override,
                window_override=cfg.window_override,
                lookback_override=cfg.lookback_override,
                tz_override=cfg.tz_override,
                state_disabled=cfg.state_disabled,
                state_mode=cfg.state_mode,
            )
            if cfg.state_disabled:
                logger.info(
                    "STATE_DISABLED: state + staging cleared on %d matched model(s)",
                    len(runs),
                )

            # Split each run's resolved window into its interval contract once
            # (apply_runtime_overrides already baked the window in) — and stash
            # it on `run.intervals`. Everyone downstream reads the attribute;
            # for state models, `@model_lifecycle` later narrows it to the
            # actionable subset.
            for run in runs:
                run.intervals = compute_intervals(run)

            _print_summary(cfg, runs)
            if cfg.dry_run:
                from bollhav.model.dry_run import print_summary

                print_summary(runs, cfg)
                return

            # Enable the progress bar for the run. A live (spinner) bar and
            # the interval-level DEBUG logs can't share the cursor, so under
            # DEBUG the batch level downgrades to model (newline-only rows
            # that interleave cleanly with log lines).
            # DRY_STATE prints its own per-model plan and executes nothing, so
            # leave the progress bar inert — otherwise its `▸ … 0ms` rows and
            # the `✓ N models done` footer stack on top of the plan as noise.
            if not cfg.dry_state:
                level = get_progress_level()
                if cfg.debug and level == ProgressLevel.EXECUTE:
                    level = ProgressLevel.MODEL
                PROGRESS.init([run.model for run in runs], level)

            # @load_models is discovery only: read env, apply overrides, match
            # models, hand the resulting ModelRuns to the user. State bootstrap
            # (state tables, library registration, prefill, interval filtering)
            # and the POST-model sweep now live in `@model_lifecycle`, which
            # runs with the connection the user opens in `main()`.
            func(runs=runs, debug=cfg.debug)
            if not cfg.dry_state:
                PROGRESS.finish()

        return wrapper

    # Allow both @load_models and @load_models(folder=...).
    if _func is None:
        return decorator
    return decorator(_func)


def _read_env() -> _RuntimeConfig:
    backfill_enabled = _resolve_backfill_enabled()
    latest = _resolve_latest(backfill=backfill_enabled)
    tz_override = _resolve_tz_override()

    return _RuntimeConfig(
        tags=_resolve_tags(),
        schema_suffix=_resolve_schema_suffix(),
        table_suffix=_resolve_table_suffix(),
        latest=latest,
        backfill_enabled=backfill_enabled,
        backfill_since=_window_dt(
            "RUN_SINCE", "BACKFILL_SINCE", backfill_enabled, tz_override
        ),
        backfill_until=_window_dt(
            "RUN_UNTIL", "BACKFILL_UNTIL", backfill_enabled, tz_override
        ),
        interval_override=_resolve_interval_override(),
        window_override=_resolve_window_override(latest=latest),
        lookback_override=_resolve_lookback_override(),
        tz_override=tz_override,
        dry_run=_resolve_dry_run(),
        dry_run_extra=_resolve_dry_run_extra(),
        state_mode=_resolve_state_mode(),
        state_disabled=_resolve_state_disabled(),
        debug=_resolve_debug(),
        dry_state=_resolve_dry_state(),
    )


def _resolve_tags() -> str:
    return cast(str, env_var(name="TAGS", required=True))


def _resolve_backfill_enabled() -> bool:
    """`BACKFILL_ENABLED` — a windowed *range* run (`RUN_SINCE`/`RUN_UNTIL`, or
    the contract's declared range when no dates are given). Defaults False; the
    unset run mode is `latest`."""
    # env_var_bool is typed `bool | None` even when `default` is given;
    # `default=...` guarantees a non-None result at runtime, so cast.
    return cast(bool, env_var_bool(name="BACKFILL_ENABLED", default=False))


def _resolve_latest(*, backfill: bool) -> bool:
    """`LATEST_ENABLED` — run the latest complete tick. This is the **default**
    run mode (when neither flag is set), so it defaults to `not backfill`.
    Mutually exclusive with backfill — raises if both are explicitly on."""
    latest = cast(bool, env_var_bool(name="LATEST_ENABLED", default=not backfill))
    if latest and backfill:
        raise ConflictingRunModeError()
    return latest


def _window_dt(
    name: str, legacy_name: str, backfill_enabled: bool, tz_override: tzinfo | None
) -> datetime | None:
    """One run-window boundary (`RUN_SINCE` / `RUN_UNTIL`) — the explicit window
    a run targets, regardless of state mode. `None` when not in a windowed run,
    else the ISO-8601 env value with the timezone override stamped on (when set).

    `legacy_name` (`BACKFILL_SINCE` / `BACKFILL_UNTIL`) is honoured as a
    **deprecated alias** when the new var is unset — it warns and will be
    removed."""
    if not backfill_enabled:
        return None
    if os.environ.get(name) is None and os.environ.get(legacy_name) is not None:
        logger.warning("env var %s is deprecated — rename it to %s", legacy_name, name)
        name = legacy_name
    dt = env_var_iso8601_datetime(name=name)
    return _apply_tz(dt, tz_override) if tz_override else dt


def _resolve_interval_override() -> str | None:
    return env_var_interval_expression(
        name="INTERVAL_OVERRIDE", should_print_unset=False
    )


def _resolve_dry_run_extra() -> bool:
    return cast(bool, env_var_bool(name="DRY_RUN_EXTRA", default=False))


def _resolve_state_disabled() -> bool:
    return cast(bool, env_var_bool(name="STATE_DISABLED", default=False))


def _resolve_debug() -> bool:
    return cast(bool, env_var_bool(name="DEBUG", default=False))


def _resolve_dry_state() -> bool:
    """DRY_STATE_EXTRA implies DRY_STATE — mirrors `_resolve_dry_run`. Kept in
    sync with `lifecycle._dry_state`, which owns the actual dry-state plan."""
    return cast(bool, env_var_bool(name="DRY_STATE", default=False)) or cast(
        bool, env_var_bool(name="DRY_STATE_EXTRA", default=False)
    )


def _resolve_dry_run() -> bool:
    """DRY_RUN_EXTRA implies DRY_RUN — setting just the verbose flag
    should still short-circuit the wrapper."""
    return cast(bool, env_var_bool(name="DRY_RUN", default=False)) or cast(
        bool, env_var_bool(name="DRY_RUN_EXTRA", default=False)
    )


def _resolve_schema_suffix() -> str:
    """`SCHEMA_SUFFIX` when `USE_SCHEMA_SUFFIX` (default true), else `""`.
    Raises if enabled but the value is empty."""
    use = cast(bool, env_var_bool(name="USE_SCHEMA_SUFFIX", default=True))
    raw = cast(str, env_var(name="SCHEMA_SUFFIX", default=""))
    if use and raw == "":
        raise MissingSchemaSuffixError()
    return raw if use else ""


def _resolve_table_suffix() -> str:
    """`TABLE_SUFFIX` when `USE_TABLE_SUFFIX` (default false), else `""`.
    Raises if enabled but the value is empty."""
    use = cast(bool, env_var_bool(name="USE_TABLE_SUFFIX", default=False))
    raw = cast(str, env_var(name="TABLE_SUFFIX", default=""))
    if use and raw == "":
        raise MissingTableSuffixError()
    return raw if use else ""


def _resolve_window_override(*, latest: bool) -> str | None:
    """`WINDOW_OVERRIDE`, which only applies in latest mode. Raises
    if set while not in latest mode (backfill pins since/until explicitly)."""
    override = env_var_interval_expression(
        name="WINDOW_OVERRIDE", should_print_unset=False
    )
    if override and not latest:
        raise WindowOverrideWithoutLatestError()
    return override


def _resolve_lookback_override() -> int | None:
    """`LOOKBACK_OVERRIDE` (cron ticks to shift each interval's start back), or
    None when unset. Raises if negative."""
    override = env_var_int(name="LOOKBACK_OVERRIDE", should_print_unset=False)
    if override is not None and override < 0:
        raise NegativeLookbackError(override)
    return override


def _resolve_state_mode() -> StateMode:
    raw = env_var(name="STATE_MODE", should_print_unset=False)
    if raw is None:
        # bulldozer is the default: run *exactly* the resolved window (reset its
        # rows, leave the rest). discover (skip-applied / reconcile) is opt-in.
        return StateMode.BULLDOZER
    valid = {m.value: m for m in StateMode}
    mode = valid.get(raw.strip().lower())
    if mode is None:
        raise InvalidStateModeError(raw, list(valid.keys()))
    return mode


def _resolve_interval_status(
    conn,
    *,
    interval,
    upstream_names: list[str],
) -> tuple[str, str | None]:
    """Decide whether one interval should be `pending` or `blocked`.
    Returns `(status, blocked_reason)` — reason is None for pending,
    otherwise a `S###: explanation` string keyed by `BlockCode`."""
    from bollhav.model.state import BlockCode, format_block_reason
    from bollhav.postgres.state import PostgresState

    for upstream_name in upstream_names:
        entry = PostgresState.lookup_model(conn, upstream_name)
        if entry is None:
            # Not in the library → not an enforced dependency. Only
            # state-tracked models register, so an unregistered name
            # (a view, a state-less table, or a typo) is treated as
            # documentation and does not block.
            continue
        if not PostgresState.is_satisfied(conn, entry=entry, interval=interval):
            return (
                "blocked",
                format_block_reason(
                    BlockCode.UPSTREAM_NOT_SATISFIED,
                    (
                        f"upstream {upstream_name!r} ({entry.model_type}) has no "
                        f"applied row covering {interval.since.isoformat()} → "
                        f"{interval.until.isoformat()}"
                    ),
                ),
            )
    return ("pending", None)


def _resolve_tz_override() -> tzinfo | None:
    raw = env_var(name="TIMEZONE_OVERRIDE", should_print_unset=False)
    if raw is None:
        return None
    try:
        return ZoneInfo(raw)
    except KeyError:
        raise InvalidTimezoneError(raw)


def _apply_tz(dt: datetime | None, tz: tzinfo) -> datetime | None:
    if dt is None:
        return None
    return dt.replace(tzinfo=tz)


def _print_summary(cfg: _RuntimeConfig, runs: list[ModelRun]) -> None:
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
        _row("mode", "backfill")
        _row("from", _date(cfg.backfill_since))
        _row("until", _date(cfg.backfill_until))
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
    if cfg.tz_override is not None:
        _row("tz override", str(cfg.tz_override))
    if cfg.interval_override:
        _row("interval override", cfg.interval_override)
    if cfg.lookback_override is not None:
        _row("lookback override", str(cfg.lookback_override))
    if cfg.latest and cfg.window_override:
        _row("window override", cfg.window_override)
    # Show STATE_MODE only when at least one matched model actually
    # has state — otherwise the env var is a no-op and listing it
    # would be misleading clutter (a staging-only model has no state
    # table, prefill, or applied-gate). STATE_DISABLED overrides with a
    # plain "disabled" label.
    has_state = any(run.model.state is not None for run in runs)
    if cfg.state_disabled:
        _row("state", "disabled")
    elif has_state:
        _row("state", cfg.state_mode.value)
    # STATE_MARK_APPLIED is resolved in @model_lifecycle (like DRY_STATE), but flag it
    # loudly here so it's never run by accident — it writes `applied` without
    # running any data.
    from bollhav.model.lifecycle import _mark_applied

    if _mark_applied():
        _row("mark applied", "stamping window applied — NO data will run")
    # Close the runtime box with a lower border.
    print("────────────────────────────")


__all__ = ["load_models", "ModelRun"]
