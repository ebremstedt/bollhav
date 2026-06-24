from __future__ import annotations

import inspect
import logging
import os
import traceback as _tb
from datetime import datetime, timezone
from functools import wraps
from typing import Callable

from bollhav.model.messages.error import (
    MissingDataConnError,
    MissingRunError,
    MssqlStateRequiresPostgresConnError,
    RecreatePartitionWithoutIntervalError,
    RecreatePartitionWithoutWindowError,
)
from bollhav.model.progress_bar import PROGRESS
from bollhav.model.state_dry_plan import _fmt_window, _print_state_plan
from bollhav.model.window import compute_intervals
from bollhav.model.write_modes import WriteMode

logger = logging.getLogger(__name__)


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
        raise MissingDataConnError()

    state_conn = arguments.get("state_conn") or data_conn
    run = arguments.get("run")
    model = run.model if run is not None else None
    if model is not None and model.stateful:
        from bollhav.model.database import Database
        if model.target.database is Database.MSSQL and state_conn is data_conn:
            raise MssqlStateRequiresPostgresConnError(model.target.full_name)
            
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
            raise MissingRunError("@model_lifecycle")
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
            logger.info("curfew: skipping model %s (stays pending)", model.target.full_name)
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

                from bollhav.model.state import StateMode
                if model.state.mode is StateMode.TORCH and not dry_state:
                    state_handler.torch_rows()

                if run.window is None:
                    state_handler.insert_oneshot(run_id=run.run_id)
                else:
                    state_handler.insert_intervals(
                        run_id=run.run_id,
                        intervals=compute_intervals(run),
                    )
                run.intervals = state_handler.get_actionable_intervals()

                if mark_applied and not dry_state:
                    intervals = compute_intervals(run)
                    for interval in intervals:
                        state_handler.mark_applied(
                            run_id=run.run_id, interval=interval
                        )
                    logger.warning(
                        "STATE_MARK_APPLIED: stamped %d interval(s) applied for %s "
                        "WITHOUT running it",
                        len(intervals),
                        model.target.full_name,
                    )

            PROGRESS.begin_model_for(model, total=len(run.intervals))
            if dry_state:
                _print_state_plan(run, state_handler, _dry_state_extra())
                return None
            if mark_applied:
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

                    state_handler = PostgresState(model=model, conn=state_conn)
                    state_handler.release_lock()
                except Exception:
                    logger.warning(
                        "state: failed to release model lock for %s (will release on session end)",
                        model.target.full_name,
                        exc_info=True,
                    )

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
            raise MissingRunError()
        model = run.model

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
            `model.target.database`; both expose the same staging methods."""
            if interval is None and (
                model.target.write_mode is WriteMode.RECREATE_PARTITION
            ):
                raise RecreatePartitionWithoutIntervalError(model.target.full_name)

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
                logger.debug("state: gate skipped applied %s for %s", interval, model.target.full_name)
                return None

            if not state_handler.try_acquire_interval_lock(interval):
                logger.debug(
                    "state: lock held by another worker, skipping %s on %s",
                    interval,
                    model.target.full_name,
                )
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
                    logger.warning(
                        "state: failed to release interval lock %s for %s "
                        "(will release on session end)",
                        interval,
                        model.target.full_name,
                        exc_info=True,
                    )

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
