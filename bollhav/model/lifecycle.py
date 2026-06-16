from __future__ import annotations

import inspect
import logging
import os
import sys
import traceback as _tb
from datetime import datetime, timezone
from functools import wraps
from typing import Callable

from bollhav.model.progress_bar import PROGRESS
from bollhav.model.window import compute_intervals

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


def _print_state_plan(run, postgres_state) -> None:
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
    if postgres_state is None:
        pending = _lgreen(f"pending {len(run.intervals)} unit(s)")
        print(f"  {display} ({kind})  ·  stateless → {pending}")
        if extra:
            for interval in run.intervals:
                print(f"      {_blue(_fmt_window(interval))}   {_lgreen('pending')}")
        _DRY_STATE_RUNS[name] = list(run.intervals)
        return

    # (interval, status, upstreams) — status in {"run", "after", "blocked"}.
    rows = [
        (interval, *postgres_state.dry_state_classify(interval, _DRY_STATE_RUNS))
        for interval in run.intervals
    ]

    would_run = sum(1 for _, s, _ in rows if s in ("run", "after"))
    blocked = sum(1 for _, s, _ in rows if s == "blocked")
    applied = postgres_state.read_status_summary()["counts"].get("applied", 0)
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

    # State always runs in Postgres. If the data backend is MSSQL and the
    # model is state-tracked, the state machine can't run on the MSSQL
    # data_conn — a separate Postgres state_conn is required. Catch the
    # missing one here with a clear message instead of a driver error later.
    run = arguments.get("run")
    model = run.model if run is not None else None
    if model is not None and getattr(model, "stateful", False):
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
        postgres_state = None

        # Curfew early-out (real runs only): if the curfew is in effect right
        # now, skip the whole model — no lock, no asset DDL, no state bootstrap.
        # Every interval would be skipped per-interval anyway, so this avoids the
        # setup. A run that STARTS clear of the curfew but crosses into it mid-run
        # is still caught per interval in @execute_lifecycle. DRY_STATE keeps
        # planning the model (it does no expensive setup to skip).
        if (
            not dry_state
            and model.curfew is not None
            and model.curfew.blocks(datetime.now(timezone.utc))
        ):
            logger.info(
                "curfew: skipping model %s (stays pending)", model.target.full_name
            )
            return None

        locked = False
        if (
            not dry_state
            and model.stateful
            and model.state.backend == StateBackend.POSTGRES
        ):
            from bollhav.postgres.state import PostgresState

            postgres_state = PostgresState(model=model, conn=state_conn)
            locked = postgres_state.acquire_model_lock()

        # DRY_STATE does no target-schema work — no data backend, so the
        # asset-DDL block below is skipped entirely (no CREATE TABLE etc.).
        data = None
        if not dry_state:
            if model.target.database is Database.POSTGRES:
                from bollhav.postgres.data import PostgresData

                data = PostgresData(model=model, conn=data_conn)
            elif model.target.database is Database.MSSQL:
                from bollhav.mssql.data import MssqlData

                data = MssqlData(model=model, conn=data_conn)

        try:
            if data is not None:
                data.create_schema()
                if model.is_view:
                    # A view's asset IS its definition — create it here, in
                    # place of the table DDL. No table/index/staging applies.
                    data.create_or_replace_view()
                else:
                    if model.target.recreate_table:
                        data.recreate_table()
                    data.create_table()
                    if model.target.truncate_table:
                        data.truncate_table()
                    if model.target.partitioned_by is not None:
                        data.create_indexes()
                    if model.target.unique_columns:
                        data.add_unique_constraint()
                    if model.target.stage:
                        data.create_staging_schema()
                        data.gc_orphan_staging_tables()

            # State always lives in Postgres (the only backend). An MSSQL-data
            # model can be state-tracked too — its state rows live in Postgres,
            # on the separate `state_conn` (`_conns` enforces that a distinct
            # Postgres connection is passed). So this block runs for any
            # stateful model regardless of its data backend.
            if model.stateful and model.state.backend == StateBackend.POSTGRES:
                from bollhav.postgres.state import PostgresState

                postgres_state = PostgresState(model=model, conn=state_conn)
                postgres_state.ensure_library()
                postgres_state.register_model()
                postgres_state.ensure_tables()

                # A NULL-window one-shot row when there's no window to track —
                # a timeless model, or a temporal one with no declared range.
                # Otherwise one row per window: a batched run splits its window
                # into chunks; an unbatched temporal run with a [begin, end]
                # contract records that single range as one row.
                if run.window is None:
                    postgres_state.insert_oneshot(run_id=run.run_id)
                else:
                    postgres_state.insert_intervals(
                        run_id=run.run_id,
                        intervals=compute_intervals(run),
                    )
                run.intervals = postgres_state.get_actionable_intervals()

            PROGRESS.begin_model_for(model, total=len(run.intervals))
            if dry_state:
                _print_state_plan(run, postgres_state)
                return None
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

                    postgres_state = PostgresState(model=model, conn=state_conn)
                    postgres_state.release_lock()
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
            target → tear it down. Staging is interval-only. The data
            backend swaps on `model.target.database`; both expose the same
            staging methods."""
            if interval is None:
                raise ValueError(
                    f"staging is interval-only, but {model.target.full_name!r} "
                    f"ran with no interval — staging requires a batched model."
                )
            from bollhav.model.database import Database

            if model.target.database is Database.MSSQL:
                from bollhav.mssql.data import MssqlData

                data = MssqlData(model=model, conn=data_conn)
            else:
                from bollhav.postgres.data import PostgresData

                data = PostgresData(model=model, conn=data_conn)

            data.create_staging_table(run.run_id)
            result = func(*args, **kwargs)
            data.apply_staging_to_target(run.run_id, interval)
            data.drop_staging_table(run.run_id)
            return result

        def plain_execute():
            return func(*args, **kwargs)

        def run_with_state(execute):
            from bollhav.postgres.state import PostgresState

            postgres_state = PostgresState(model=model, conn=state_conn)

            if postgres_state.is_applied(interval):
                message = "state: gate skipped applied %s for %s"
                logger.debug(message, interval, model.target.full_name)
                return None

            if not postgres_state.try_acquire_interval_lock(interval):
                message = "state: lock held by another worker, skipping %s on %s"
                logger.debug(message, interval, model.target.full_name)
                return None

            try:
                if model.gated_upstreams:
                    check = postgres_state.is_upstream_satisfied_live(interval)
                    if not check.satisfied:
                        postgres_state.mark_blocked(
                            run_id=run.run_id,
                            interval=interval,
                            reason=check.reason or "",
                        )
                        return None

                postgres_state.mark_running(run_id=run.run_id, interval=interval)

                try:
                    result = execute()
                except Exception as exc:
                    postgres_state.record_failure(
                        run_id=run.run_id,
                        interval=interval,
                        error_type=type(exc).__name__,
                        error_message=str(exc),
                        traceback_text=_tb.format_exc(),
                        update_state=True,
                    )
                    raise

                postgres_state.mark_applied(run_id=run.run_id, interval=interval)
                return result
            finally:
                try:
                    postgres_state.release_interval_lock(interval)
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
