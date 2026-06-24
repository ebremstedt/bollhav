from __future__ import annotations

import inspect
import logging
import os
import sys
import traceback as _tb
from datetime import datetime, timezone
from functools import wraps
from typing import Callable

from bollhav.model.errors import RecreatePartitionWithoutWindowError
from bollhav.model.progress_bar import PROGRESS
from bollhav.model.window import compute_intervals
from bollhav.model.write_modes import WriteMode

logger = logging.getLogger(__name__)


def _sgr(s: str, code: str) -> str:
    """Wrap `s` in an ANSI SGR `code` for terminal output; left plain when
    stdout isn't a TTY (so piped/captured output has no escape codes)."""
    return f"\033[{code}m{s}\033[0m" if sys.stdout.isatty() else s


def _blue(s: str) -> str:
    return _sgr(s, "34")


def _red(s: str) -> str:
    return _sgr(s, "31")


def _lgreen(s: str) -> str:
    """Light green — actionable (`pending`) units in the DRY_STATE plan."""
    return _sgr(s, "38;2;120;215;120")


def _dgreen(s: str) -> str:
    """Deeper green — already-`applied` units in the DRY_STATE plan."""
    return _sgr(s, "38;2;35;150;70")


# Dark → light blue ramp (truecolor RGB) for rendering a dotted model name as a
# gradient: first segment (catalog) darkest, last (table) lightest. Every stop
# keeps green ≥ red with a dominant blue channel, so it stays squarely blue and
# never drifts toward purple/indigo (which is what the low xterm-256 navies do).
_NAME_RAMP = (
    (35, 130, 220),
    (75, 160, 235),
    (110, 185, 245),
    (140, 205, 252),
    (170, 220, 255),
)


def _gradient_name(full_name: str) -> str:
    """`catalog.schema.table` with each dotted segment a shade lighter than the
    one before it (darkest first). The dots stay the terminal default; plain
    text when stdout isn't a TTY."""
    parts = full_name.split(".")
    last = len(parts) - 1

    def _shade(text: str, rgb: tuple[int, int, int]) -> str:
        r, g, b = rgb
        return _sgr(text, f"38;2;{r};{g};{b}")

    if last <= 0:
        return _shade(full_name, _NAME_RAMP[0])
    return ".".join(
        _shade(part, _NAME_RAMP[round(i * (len(_NAME_RAMP) - 1) / last)])
        for i, part in enumerate(parts)
    )


def _truthy(name: str) -> bool:
    # Read straight off os.environ (not via roskarl) so it's independent of the
    # env-var library and robust under the test suite's roskarl mock.
    return os.environ.get(name, "").strip().lower() in ("true", "1", "yes", "on")


def _dry_state() -> bool:
    """`DRY_STATE=true` — run the state bootstrap and print each model's
    resolved plan (what would run / is applied / is blocked), then exit
    without creating target assets, writing data, or executing the model.

    Unlike `DRY_RUN` (model-level, no DB), this resolves against the real
    state: it initializes/refreshes the state bookkeeping (idempotent — the
    same thing a real run's bootstrap does), so it reflects current `applied`
    rows and upstream gates. It performs **no** target-schema DDL, **no** data
    writes, and runs **no** model logic. `DRY_STATE_EXTRA` implies it."""
    return _truthy("DRY_STATE") or _truthy("DRY_STATE_EXTRA")


def _dry_state_extra() -> bool:
    """`DRY_STATE_EXTRA=true` — same as `DRY_STATE`, but list every actionable
    interval individually (its window + would-run / blocked) instead of just
    the per-model counts. Mirrors `DRY_RUN_EXTRA` over `DRY_RUN`."""
    return _truthy("DRY_STATE_EXTRA")


def _mark_applied() -> bool:
    """`STATE_MARK_APPLIED=true` — stamp the run's window intervals `applied` in state
    WITHOUT running them (the data was loaded out of band, e.g. a STATE_DISABLED
    bulk load or a manual script). The complement of STATE_DISABLED: state
    without data. No target DDL, no read/write/staging — only the matched
    models' `compute_intervals(run)` (the supplied window, at its chunk) are
    marked, never the actionable backlog. An assertion, not a verification."""
    return _truthy("STATE_MARK_APPLIED")


def _fmt_window(interval) -> str:
    """A compact window label for one unit of work; `None` is the whole-table
    / view oneshot."""
    if interval is None:
        return "(whole table)"
    return f"{interval.since:%Y-%m-%d %H:%M} → {interval.until:%Y-%m-%d %H:%M}"


# DRY_STATE cascade accumulator: full_name → windows that would run this pass.
# Models are processed in dependency order (a real `@load_models` run topo-sorts
# them), so a downstream sees its upstreams' would-run windows here and can show
# "will run after <upstream>" instead of "blocked". Populated only under
# DRY_STATE; never consulted by real gating.
_DRY_STATE_RUNS: dict[str, list] = {}


def _print_state_plan(run, state_handler) -> None:
    """One run's state-resolved plan, for `DRY_STATE`. Each actionable unit is
    classified — **would run** (gates already satisfied), **will run after** an
    upstream that would itself run earlier in this pass (the cascade), or
    **blocked** by an upstream that would NOT run — then summarized (counts) or,
    with `DRY_STATE_EXTRA`, listed per interval. Read-only: gates are evaluated
    live, the cascade via the in-pass `_DRY_STATE_RUNS` overlay."""
    extra = _dry_state_extra()
    name = run.model.target.full_name  # raw — used as the `_DRY_STATE_RUNS` key
    display = _gradient_name(name)  # colorized for output only
    kind = run.model.temporality.value
    if state_handler is None:
        pending = _lgreen(f"pending {len(run.intervals)} unit(s)")
        print(f"  {display} ({kind})  ·  stateless → {pending}")
        if extra:
            for interval in run.intervals:
                print(f"      {_blue(_fmt_window(interval))}   {_lgreen('pending')}")
        _DRY_STATE_RUNS[name] = list(run.intervals)
        return

    # (interval, status, upstreams) — status in {"run", "after", "blocked"}.
    rows = [
        (interval, *state_handler.dry_state_classify(interval, _DRY_STATE_RUNS))
        for interval in run.intervals
    ]

    would_run = sum(1 for _, s, _ in rows if s in ("run", "after"))
    blocked = sum(1 for _, s, _ in rows if s == "blocked")
    applied = state_handler.read_status_summary()["counts"].get("applied", 0)
    # Show only the non-zero buckets — a `pending 0` / `blocked 0` / `applied 0`
    # segment is just noise. A model with nothing in any bucket falls back to a
    # dash so the line still reads.
    segs = []
    if would_run:
        segs.append(_lgreen(f"pending {would_run}"))
    if blocked:
        segs.append(_red(f"blocked {blocked}"))
    if applied:
        segs.append(_dgreen(f"applied {applied}"))
    print(f"  {display} ({kind})  ·  " + ("  ·  ".join(segs) if segs else "—"))

    # Record would-run (immediate + cascade) windows so downstreams resolve.
    _DRY_STATE_RUNS[name] = [iv for iv, s, _ in rows if s in ("run", "after")]

    if extra:
        for interval, status, ups in rows:
            if status == "run":
                tail = _lgreen("pending")
            elif status == "after":
                tail = f"pending after {', '.join(ups)}"
            else:
                tail = _red(f"blocked: {'; '.join(ups)}")
            print(f"      {_blue(_fmt_window(interval))}   {tail}")
    else:
        agg: dict[str, int] = {}
        for _, s, ups in rows:
            if s == "blocked":
                agg["; ".join(ups)] = agg.get("; ".join(ups), 0) + 1
        for reason, n in sorted(agg.items()):
            print(_red(f"      blocked by: {reason}{f'  ×{n}' if n > 1 else ''}"))


def _conns(arguments: dict):
    """Pull `data_conn` and `state_conn` out of the bound call args.

    `data_conn` is required and must not be None — open it in `main()`
    and thread it through. `state_conn` is optional and defaults to
    `data_conn` (co-located state); pass a separate one only for the
    cross-DB case — most notably **MSSQL data + Postgres state**, where the
    two connections are different drivers and a separate `state_conn` is
    mandatory."""
    data_conn = arguments.get("data_conn")
    if data_conn is None:
        raise ValueError(
            "data_conn is required and must not be None — open it in "
            "main() (autocommit) and pass it to the lifecycle-wrapped "
            "function."
        )
    state_conn = arguments.get("state_conn") or data_conn
    run = arguments.get("run")
    model = run.model if run is not None else None
    if model is not None and model.stateful:
        from bollhav.model.database import Database

        if model.target.database is Database.MSSQL and state_conn is data_conn:
            raise ValueError(
                f"{model.target.full_name!r} is an MSSQL model with state, so "
                f"state lives in Postgres — pass a separate Postgres "
                f"`state_conn=` (psycopg) alongside the MSSQL `data_conn=` "
                f"(pyodbc). State coordination can't run on the MSSQL connection."
            )
    return data_conn, state_conn


def model_lifecycle(func: Callable) -> Callable:
    """Bracket one model's run: assets + (when stateful) state-table
    ensure, library registration, prefill, and the optional exclusive
    model lock. Asset DDL runs on `data_conn`; state setup on
    `state_conn`. POST actions run on a clean return."""
    sig = inspect.signature(func)

    @wraps(func)
    def wrapper(*args, **kwargs):
        bound = sig.bind(*args, **kwargs)
        bound.apply_defaults()
        run = bound.arguments.get("run")
        data_conn, state_conn = _conns(bound.arguments)

        if run is None:
            raise ValueError(
                "@model_lifecycle-wrapped function was called without a "
                "`run` argument — it's required (the hook brackets one "
                "model's run)."
            )
        model = run.model

        from bollhav.model.database import Database
        from bollhav.model.state import StateBackend

        dry_state = _dry_state()
        mark_applied = _mark_applied()
        state_handler = None

        if (
            not dry_state
            and model.curfew is not None
            and model.curfew.blocks(datetime.now(timezone.utc))
        ):
            msg = "curfew: skipping model %s (stays pending)"
            logger.info(msg, model.target.full_name)
            return None

        locked = False
        if (
            not dry_state
            and model.stateful
            and model.state.backend == StateBackend.POSTGRES
        ):
            from bollhav.postgres.state import PostgresState

            state_handler = PostgresState(model=model, conn=state_conn)
            locked = state_handler.acquire_model_lock()

        # DRY_STATE and STATE_MARK_APPLIED do no target-schema work — no data backend,
        # so the asset-DDL block below is skipped entirely (no CREATE TABLE etc.).
        data_handler = None
        if not dry_state and not mark_applied:
            if model.target.database is Database.POSTGRES:
                from bollhav.postgres.data import PostgresData

                data_handler = PostgresData(model=model, conn=data_conn)
            elif model.target.database is Database.MSSQL:
                from bollhav.mssql.data import MssqlData

                data_handler = MssqlData(model=model, conn=data_conn)

        try:
            if data_handler is not None:
                data_handler.create_schema()
                if model.is_view:
                    data_handler.create_or_replace_view()
                else:
                    if model.target.recreate_table:
                        data_handler.recreate_table()
                    data_handler.create_table()
                    if model.target.truncate_table:
                        data_handler.truncate_table()
                    if model.target.partitioned_by is not None:
                        data_handler.create_indexes()
                    if model.target.unique_columns:
                        data_handler.add_unique_constraint()
                    if model.target.stage:
                        data_handler.create_staging_schema()
                        data_handler.gc_orphan_staging_tables()

            if model.stateful and model.state.backend == StateBackend.POSTGRES:
                from bollhav.postgres.state import PostgresState

                state_handler = PostgresState(model=model, conn=state_conn)
                state_handler.ensure_library()
                state_handler.register_model()
                state_handler.ensure_tables()

                if (
                    run.window is None
                    and model.target.write_mode is WriteMode.RECREATE_PARTITION
                ):
                    raise RecreatePartitionWithoutWindowError(
                        model.target.full_name
                    )

                # STATE_MODE=nuke: wipe this model's existing state rows before
                # prefill so a chunk-granularity change (or a stale backlog)
                # re-discovers from scratch. Never during DRY_STATE — a preview
                # must not delete.
                from bollhav.model.state import StateMode

                if model.state.mode is StateMode.NUKE and not dry_state:
                    state_handler.nuke_rows()

                # A NULL-window one-shot row when there's no window to track —
                # a timeless model, or a temporal one with no declared range.
                # Otherwise one row per window: a batched run splits its window
                # into chunks; an unbatched temporal run with a [begin, end]
                # contract records that single range as one row.
                if run.window is None:
                    state_handler.insert_oneshot(run_id=run.run_id)
                else:
                    state_handler.insert_intervals(
                        run_id=run.run_id,
                        intervals=compute_intervals(run),
                    )
                run.intervals = state_handler.get_actionable_intervals()

                # STATE_MARK_APPLIED: the data was loaded out of band — stamp exactly
                # THIS window's intervals applied (compute_intervals, NEVER the
                # actionable backlog) and run no model logic. Not during a
                # DRY_STATE preview.
                if mark_applied and not dry_state:
                    ivs = compute_intervals(run)
                    for interval in ivs:
                        state_handler.mark_applied(
                            run_id=run.run_id, interval=interval
                        )
                    logger.warning(
                        "STATE_MARK_APPLIED: stamped %d interval(s) applied for %s "
                        "WITHOUT running it",
                        len(ivs),
                        model.target.full_name,
                    )

            PROGRESS.begin_model_for(model, total=len(run.intervals))
            if dry_state:
                _print_state_plan(run, state_handler)
                return None
            if mark_applied:
                return None  # state stamped above; run no model logic
            return func(*args, **kwargs)
        finally:
            PROGRESS.finish_model()
            if (
                locked
                and model.stateful
                and model.state.backend == StateBackend.POSTGRES
            ):
                try:
                    from bollhav.postgres.state import PostgresState

                    state_handler = PostgresState(model=model, conn=state_conn)
                    state_handler.release_lock()
                except Exception:
                    # Released automatically when the session ends too.
                    pass

    return wrapper


def execute_lifecycle(func: Callable) -> Callable:
    """Bracket one call to the user's execute.

    Two independent switches decide what wraps the execute:
    `model.stateful` (does it track state?) and `model.target.stage`
    (does it write through a staging table?). Their four combinations:

        stateful | stage | what runs
        ---------+-------+--------------------------------------------
           no    |  no   | the execute, directly
           no    |  yes  | staged execute (create → write → merge →
                 |       | teardown); no state machine
           yes   |  no   | state machine around the direct execute
           yes   |  yes  | state machine around the staged execute

    "State machine" = gate on applied → take the interval lock → check
    upstreams → mark running → run → mark applied (or record failure) →
    release the lock. "Staged" brackets the execute with the staging
    lifecycle (interval-only). Staging runs even without state — the two
    switches are orthogonal.
    """
    sig = inspect.signature(func)

    @wraps(func)
    def wrapper(*args, **kwargs):
        bound = sig.bind(*args, **kwargs)
        bound.apply_defaults()
        run = bound.arguments.get("run")
        interval = bound.arguments.get("interval")
        data_conn, state_conn = _conns(bound.arguments)

        if run is None:
            raise ValueError("execute cannot be called if run is None")
        model = run.model

        # Curfew: a wall-clock gate on *starting* this interval. When the curfew
        # blocks now, skip the interval entirely — no execute, no staging, no
        # state flip — so a stateful model's interval stays pending for a later
        # (post-curfew) run. Checked per interval, so a run that crosses into a
        # curfew window stops cleanly on the next interval rather than mid-write.
        if model.curfew is not None and model.curfew.blocks(datetime.now(timezone.utc)):
            logger.info(
                "curfew: skipping %s for %s (stays pending)",
                _fmt_window(interval),
                model.target.full_name,
            )
            return None

        def staged_execute():
            """Run the user's execute bracketed by the staging lifecycle:
            create the table → execute writes rows into it → merge into the
            target → tear it down. The data backend swaps on
            `model.target.database`; both expose the same staging methods.

            A no-window run (interval=None) is allowed: it stages the whole
            read and applies it atomically (one MERGE/INSERT). Only
            RECREATE_PARTITION needs a window — it's partition-scoped."""
            if interval is None and (
                model.target.write_mode is WriteMode.RECREATE_PARTITION
            ):
                raise ValueError(
                    f"RECREATE_PARTITION is window-scoped, but "
                    f"{model.target.full_name!r} ran with no interval — run it "
                    f"windowed (LATEST_ENABLED or a BACKFILL window)."
                )
            from bollhav.model.database import Database

            if model.target.database is Database.MSSQL:
                from bollhav.mssql.data import MssqlData

                data_handler = MssqlData(model=model, conn=data_conn)
            else:
                from bollhav.postgres.data import PostgresData

                data_handler = PostgresData(model=model, conn=data_conn)

            data_handler.create_staging_table(run.run_id)
            result = func(*args, **kwargs)
            data_handler.apply_staging_to_target(run.run_id, interval)
            data_handler.drop_staging_table(run.run_id)
            return result

        def plain_execute():
            return func(*args, **kwargs)

        def run_with_state(execute):
            from bollhav.postgres.state import PostgresState

            state_handler = PostgresState(model=model, conn=state_conn)

            if state_handler.is_applied(interval):
                message = "state: gate skipped applied %s for %s"
                logger.debug(message, interval, model.target.full_name)
                return None

            if not state_handler.try_acquire_interval_lock(interval):
                message = "state: lock held by another worker, skipping %s on %s"
                logger.debug(message, interval, model.target.full_name)
                return None

            try:
                if model.gated_upstreams:
                    check = state_handler.is_upstream_satisfied_live(interval)
                    if not check.satisfied:
                        state_handler.mark_blocked(
                            run_id=run.run_id,
                            interval=interval,
                            reason=check.reason or "",
                        )
                        return None

                state_handler.mark_running(run_id=run.run_id, interval=interval)

                try:
                    result = execute()
                except Exception as exc:
                    state_handler.record_failure(
                        run_id=run.run_id,
                        interval=interval,
                        error_type=type(exc).__name__,
                        error_message=str(exc),
                        traceback_text=_tb.format_exc(),
                        update_state=True,
                    )
                    raise

                state_handler.mark_applied(run_id=run.run_id, interval=interval)
                return result
            finally:
                try:
                    state_handler.release_interval_lock(interval)
                except Exception:
                    pass

        with PROGRESS.interval():
            if not model.stateful and not model.target.stage:
                return func(*args, **kwargs)

            if not model.stateful and model.target.stage:
                return staged_execute()

            if model.stateful and not model.target.stage:
                return run_with_state(plain_execute)

            return run_with_state(staged_execute)

    return wrapper


__all__ = ["model_lifecycle", "execute_lifecycle"]
