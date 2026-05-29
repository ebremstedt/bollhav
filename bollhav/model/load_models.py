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

from bollhav.model.runtime import apply_runtime_overrides
from bollhav.model.model import Model
from bollhav.model.ordering import UpstreamMode
from bollhav.model.progress_bar import get_progress_level
from bollhav.model.state import StateMode

import logging
from uuid import uuid4

logger = logging.getLogger(__name__)


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
    state_disabled: bool
    peek: bool
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
        STATE_MODE                    respect (default) | disrespect. For
                                      state-enabled models, controls how
                                      pre-fill treats existing state rows.
                                      respect = preserve applied rows;
                                      disrespect = reset every row to pending.
        PEEK                          bool; when true, run the state bootstrap
                                      (so the state banner is accurate) and
                                      then EXIT without invoking the wrapped
                                      function. Distinct from DRY_RUN, which
                                      skips the DB entirely.
        STATE_DISABLED                bool; when true, force the pipeline to
                                      run with NO state tracking — even for
                                      models that declare state=State(...).
                                      Useful for ad-hoc/dev runs against a DB
                                      where the state tables don't exist or
                                      you don't want to touch them. Skips the
                                      bootstrap, banner, and any state-table
                                      writes; @state becomes a
                                      passthrough; write() uses the direct
                                      path even on staging-enabled models.
    """

    def decorator(func: Callable[..., None]) -> Callable[[], None]:
        @wraps(func)
        def wrapper() -> None:
            cfg = _read_env()
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
            # STATE_DISABLED forces no-state semantics on every matched
            # model — useful for ad-hoc/dev runs. Nulling `state` and
            # `target.staging` makes the rest of the wrapper cascade
            # naturally: bootstrap is a no-op, banner skips,
            # @state becomes passthrough, write() routes direct.
            if cfg.state_disabled:
                for m in models:
                    m.state = None
                    m.target.staging = None
                logger.info(
                    "STATE_DISABLED: state + staging cleared on %d matched model(s)",
                    len(models),
                )

            _print_summary(cfg, models)
            if cfg.dry_run:
                from bollhav.model.dry_run import print_summary

                print_summary(models, cfg)
                return

            # Bollhav owns the state bootstrap for fully-managed
            # (staging) models. Other models pass through unchanged —
            # the user owns ensure_tables / prefill / loop for those.
            _bootstrap_state_for_staged_models(models, state_mode=cfg.state_mode)

            # State banner: per-model breakdown of pending/applied/blocked
            # rows after the bootstrap settled. Always shown when any
            # matched model uses staging.
            _print_state_banner(models)

            if cfg.peek:
                # PEEK: bootstrap + banner + exit. Lets the operator
                # answer "what does state look like for this pipeline?"
                # without running anything.
                return

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
        state_disabled=env_var_bool(name="STATE_DISABLED", default=False),
        peek=env_var_bool(name="PEEK", default=False),
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
        return StateMode.DISCOVER
    valid = {m.value: m for m in StateMode}
    if raw not in valid:
        raise ValueError(f"STATE_MODE must be one of {list(valid.keys())}, got {raw!r}")
    return valid[raw]


def _bootstrap_state_for_staged_models(
    models: list[Model], *, state_mode: StateMode
) -> None:
    """For each model with `target.staging` set:

      1. Read the model's contract — the intervals it says should exist
         under the current bounds + batching.
      2. Ensure the state tables exist.
      3. Ensure the cross-pipeline library exists; register/refresh
         this model in it.
      4. For each contract interval, decide its status:
         * Upstreams in the matched set are assumed to run in topo
           order — don't block on them.
         * For out-of-pipeline upstreams: query the library for the
           upstream's state table, then look for an applied row that
           exactly matches or fully encapsulates `(since, until)`.
           If any upstream fails this check, the interval becomes
           `blocked` with a reason; otherwise `pending`.
      5. Pre-fill state with the per-interval statuses (respect/
         disrespect mode controls how existing rows are treated).
      6. Read pending-only intervals back, stash on `model.intervals`.

    State DB unreachable → warn and set `model.intervals = []`. Other
    models keep going."""
    from bollhav.postgres import library as pg_library
    from bollhav.postgres import staging as pg_staging
    from bollhav.postgres import state as pg_state

    matched_names = {m.target.full_name for m in models}

    for model in models:
        # Library-only registration path. Triggered when the user
        # explicitly opts in via `Model(library=True)` and the model
        # has no staging machinery — works the same for VIEW and
        # state-less TABLE models. No state table is created; the
        # library row alone makes the model claimable. Every
        # downstream interval that references one of these is
        # satisfied by mere presence in the library.
        is_register_only = model.library and model.target.staging is None
        if is_register_only:
            try:
                with pg_state._connect(model) as conn:
                    pg_library.ensure_library(conn)
                    pg_library.register(conn, model)
            except ConnectionError as exc:
                logger.warning(
                    "library: registration failed for %s — %s",
                    model.target.full_name,
                    exc,
                )
            continue

        if model.target.staging is None:
            continue

        # Every staging model — with or without state — needs:
        #   * a run_id stashed for per-interval staging table naming
        #   * orphan staging tables from earlier crashed runs GC'd
        run_id = uuid4()
        model._state_run_id = run_id
        try:
            pg_staging.gc_orphan_staging_tables(model)
        except ConnectionError as exc:
            logger.warning(
                "staging: orphan GC failed for %s — %s",
                model.target.full_name,
                exc,
            )

        if model.state is None:
            # Staging without state — register in library if opted in
            # (so downstreams can claim this table) and we're done.
            # No state-table ensure, no prefill, no interval filtering;
            # the user's loop runs every contract interval every time.
            if model.library:
                try:
                    with pg_state._connect(model) as conn:
                        pg_library.ensure_library(conn)
                        pg_library.register(conn, model)
                except ConnectionError as exc:
                    logger.warning(
                        "library: registration failed for %s — %s",
                        model.target.full_name,
                        exc,
                    )
            continue

        contract = list(model.intervals)

        try:
            pg_state.ensure_tables(model)

            with pg_state._connect(model) as conn:
                pg_library.ensure_library(conn)
                pg_library.register(conn, model)

                # The decorator additionally re-checks at runtime, so
                # blocked rows here are a snapshot that may flip back
                # to processable as upstreams catch up.
                upstreams_to_check = [
                    u for u in model.upstream if u not in matched_names
                ]
                prefill_rows = []
                for interval in contract:
                    status, reason = _resolve_interval_status(
                        conn,
                        interval=interval,
                        upstream_names=upstreams_to_check,
                    )
                    prefill_rows.append((interval, status, reason))

                pg_state.prefill(
                    model,
                    run_id=run_id,
                    intervals=prefill_rows,
                    state_mode=state_mode,
                    conn=conn,
                )

            # User's loop iterates EVERY non-applied row (pending,
            # blocked, running, error). The decorator re-evaluates
            # each one at runtime so blocked rows naturally unblock
            # as their upstream catches up.
            model.intervals = pg_state.read_actionable(model)
        except ConnectionError as exc:
            logger.warning(
                "state: bootstrap failed for %s — skipping (intervals=[]). %s",
                model.target.full_name,
                exc,
            )
            model.intervals = []


def _print_state_banner(models: list[Model]) -> None:
    """Print the post-bootstrap state banner.

    Per staged model, two sub-sections:

      upstream:  <model.upstream[0]>   fulfilled | blocked · CODE × N
                 <model.upstream[1]>   ...
                 (or "(none declared)" when the model has no upstreams)
      state:     N pending   N applied   N blocked

    The upstream section iterates `model.upstream` so the operator can
    see *every* declared dependency and its status side-by-side —
    upstreams not in `blocked_groups` show `fulfilled`; ones that
    blocked something show the code(s) responsible. Look codes up in
    docs/content/BLOCK_CODES.md.

    No-op when no matched model has staging."""
    from bollhav.postgres import state as pg_state

    staged = [m for m in models if m.target.staging is not None]
    if not staged:
        return

    width = 60
    title = "── state "
    print(title + "─" * max(0, width - len(title)))

    for i, model in enumerate(staged):
        if i > 0:
            print()
        print(f"  {model.target.full_name}")

        try:
            summary = pg_state.read_status_summary(model)
        except Exception as exc:
            print(f"    (state unavailable: {exc})")
            continue

        c = summary["counts"]
        groups = summary["blocked_groups"]

        # Map upstream name → list of (code, count) blocking it.
        blockers_by_upstream: dict[str | None, list[tuple[str, int]]] = {}
        for (code, up_name), n in groups.items():
            blockers_by_upstream.setdefault(up_name, []).append((code, n))

        upstreams = list(model.upstream)
        if not upstreams:
            print("    upstream:  (none declared)")
        else:
            up_w = max(len(u) for u in upstreams)
            for j, upstream in enumerate(upstreams):
                label = "upstream:  " if j == 0 else "           "
                blockers = blockers_by_upstream.get(upstream, [])
                if blockers:
                    codes_str = ", ".join(f"{c} × {n}" for c, n in sorted(blockers))
                    print(f"    {label}{upstream:<{up_w}}   blocked · {codes_str}")
                else:
                    print(f"    {label}{upstream:<{up_w}}   fulfilled")

        print(
            f"    state:     {c['pending']:>3} pending   "
            f"{c.get('running', 0):>3} running   "
            f"{c['applied']:>3} applied   "
            f"{c['blocked']:>3} blocked   "
            f"{c.get('error', 0):>3} error"
        )
    print("─" * width)


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
    from bollhav.postgres import library as pg_library

    for upstream_name in upstream_names:
        entry = pg_library.lookup(conn, upstream_name)
        if entry is None:
            return (
                "blocked",
                format_block_reason(
                    BlockCode.UPSTREAM_NOT_REGISTERED,
                    f"upstream {upstream_name!r} not registered",
                ),
            )
        if not pg_library.is_satisfied(
            conn,
            entry=entry,
            since=interval.since,
            until=interval.until,
        ):
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


def _print_summary(cfg: _RuntimeConfig, models: list[Model]) -> None:
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
    if cfg.tz_override is not None:
        _row("tz override", str(cfg.tz_override))
    if cfg.interval_expression_override:
        _row("interval override", cfg.interval_expression_override)
    if cfg.lookback_override is not None:
        _row("lookback override", str(cfg.lookback_override))
    if cfg.latest and cfg.window_expression_override:
        _row("window override", cfg.window_expression_override)
    # Show STATE_MODE only when at least one matched model actually
    # has state — otherwise the env var is a no-op and listing it
    # would be misleading clutter. STATE_DISABLED overrides with a
    # plain "disabled" label.
    has_staging = any(m.target.staging is not None for m in models)
    if cfg.state_disabled:
        _row("state", "disabled")
    elif has_staging:
        _row("state", cfg.state_mode.value)
    # Drop the trailing rule when the state banner will follow —
    # the two blocks should read as one without a divider in between.
    if not has_staging:
        print("────────────────────────────")


__all__ = ["load_models", "Model"]
