"""Lifecycle hooks: `@model_lifecycle` and `@execute_lifecycle`.

These wrap the user's execute functions and bracket them with the
framework's setup/teardown + state machinery. Connections are passed as
**function parameters**, so the user creates them in `main()` and threads
them through:

    @model_lifecycle
    def execute_model(model, data_conn, state_conn=None):
        for interval in model.intervals:
            execute_interval(model, interval, data_conn, state_conn)

    @execute_lifecycle
    def execute_interval(model, interval, data_conn, state_conn=None):
        ...                       # read / transform / write(conn=data_conn, ...)

`interval` is a window for a batched model, or `None` for a monolithic
(whole-table) / view model — `model.intervals` yields a single `None` in
that case, so the same loop runs the unit of work once.

`data_conn` is required; `state_conn` is optional and defaults to
`data_conn` (co-located state). State work runs only when `model.state`
is set — a state-less model flows through untouched except for the
target-side asset DDL.

**Connections must be autocommit** (`psycopg.connect(dsn, autocommit=True)`).
The non-atomic data→state model commits each step independently — the
data write durably commits, *then* the state row flips. On a
non-autocommit connection the bare reads/writes open a dangling
transaction and the per-step `with conn.transaction()` blocks nest as
savepoints inside it, so nothing persists until an explicit commit (and
a crash/close rolls it all back). Staging's `with conn.transaction()`
still gives per-interval atomicity on top of an autocommit connection.
"""

from __future__ import annotations

import inspect
import logging
import traceback as _tb
from functools import wraps
from typing import Callable

logger = logging.getLogger(__name__)


def _conns(arguments: dict):
    """Pull `data_conn` and `state_conn` out of the bound call args.

    `data_conn` is required and must not be None — open it in `main()`
    and thread it through. `state_conn` is optional and defaults to
    `data_conn` (co-located state); pass a separate one only for the
    cross-DB case."""
    data_conn = arguments.get("data_conn")
    if data_conn is None:
        raise ValueError(
            "data_conn is required and must not be None — open it in "
            "main() (autocommit) and pass it to the lifecycle-wrapped "
            "function."
        )
    state_conn = arguments.get("state_conn") or data_conn
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
        model = bound.arguments.get("model")
        data_conn, state_conn = _conns(bound.arguments)

        if model is None:
            raise ValueError(
                "@model_lifecycle-wrapped function was called without a "
                "`model` argument — it's required (the hook brackets one "
                "model's run)."
            )

        from bollhav.model.database import Database
        from bollhav.model.state import StateBackend

        locked = False
        if model.stateful and model.state.backend == StateBackend.POSTGRES:
            from bollhav.postgres.state import PostgresState

            postgres_state = PostgresState(model=model, conn=state_conn)
            locked = postgres_state.acquire_model_lock()

        if model.target.database is Database.POSTGRES:
            try:
                from bollhav.postgres.data import PostgresData

                postgres_data = PostgresData(model=model, conn=data_conn)
                postgres_data.create_schema()
                if model.is_view:
                    # A view's asset IS its definition — create it here, in
                    # place of the table DDL. No table/index/staging applies.
                    postgres_data.create_or_replace_view()
                else:
                    if model.target.recreate_table:
                        postgres_data.recreate_table()
                    postgres_data.create_table()
                    if model.target.truncate_table:
                        postgres_data.truncate_table()
                    if model.target.partitioned_by is not None:
                        postgres_data.create_indexes()
                    if model.target.unique_columns:
                        postgres_data.add_unique_constraint()
                    if model.target.stage:
                        postgres_data.create_staging_schema()
                        postgres_data.gc_orphan_staging_tables()

                if model.stateful:
                    from bollhav.postgres.state import PostgresState

                    postgres_state = PostgresState(model=model, conn=state_conn)
                    postgres_state.ensure_library()
                    postgres_state.register_model()
                    postgres_state.ensure_tables()

                    if model.is_monolithic or model.is_view:
                        postgres_state.insert_singleton(run_id=model.run_id)
                    else:
                        postgres_state.insert_intervals(
                            run_id=model.run_id,
                            intervals=model.compute_intervals(),
                        )
                    model.intervals = postgres_state.get_actionable_intervals()

                result = func(*args, **kwargs)

                return result
            finally:
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

        # Non-Postgres target: no Postgres-side setup to do; just run.
        return func(*args, **kwargs)

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
        model = bound.arguments.get("model")
        interval = bound.arguments.get("interval")
        data_conn, state_conn = _conns(bound.arguments)

        if model is None:
            raise ValueError("execute cannot be called if Model is none")

        def staged_execute():
            """Run the user's execute bracketed by the staging lifecycle:
            create the table → execute writes rows into it → merge into the
            target → tear it down. Staging is interval-only."""
            if interval is None:
                raise ValueError(
                    f"staging is interval-only, but {model.target.full_name!r} "
                    f"ran with no interval — staging requires a batched model."
                )
            from bollhav.postgres.data import PostgresData

            postgres_data = PostgresData(model=model, conn=data_conn)
            postgres_data.create_staging_table(model.run_id)
            result = func(*args, **kwargs)
            postgres_data.apply_staging_to_target(model.run_id, interval)
            postgres_data.drop_staging_table(model.run_id)
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
                if model.upstream:
                    check = postgres_state.is_upstream_satisfied_live(interval)
                    if not check.satisfied:
                        postgres_state.mark_blocked(
                            run_id=model.run_id,
                            interval=interval,
                            reason=check.reason or "",
                        )
                        return None

                postgres_state.mark_running(run_id=model.run_id, interval=interval)

                try:
                    result = execute()
                except Exception as exc:
                    postgres_state.record_failure(
                        run_id=model.run_id,
                        interval=interval,
                        error_type=type(exc).__name__,
                        error_message=str(exc),
                        traceback_text=_tb.format_exc(),
                        update_state=True,
                    )
                    raise

                postgres_state.mark_applied(run_id=model.run_id, interval=interval)
                return result
            finally:
                try:
                    postgres_state.release_interval_lock(interval)
                except Exception:
                    pass

        if not model.stateful and not model.target.stage:
            return func(*args, **kwargs)

        if not model.stateful and model.target.stage:
            return staged_execute()

        if model.stateful and not model.target.stage:
            return run_with_state(plain_execute)

        if model.stateful and model.target.stage:
            return run_with_state(staged_execute)

    return wrapper


__all__ = ["model_lifecycle", "execute_lifecycle"]
