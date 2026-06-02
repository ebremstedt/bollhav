"""Lifecycle hooks: `@model_lifecycle` and `@interval_lifecycle`.

These wrap the user's execute functions and bracket them with the
framework's setup/teardown + state machinery. Connections are passed as
**function parameters**, so the user creates them in `main()` and threads
them through:

    @model_lifecycle
    def execute_model(model, data_conn, state_conn=None):
        for interval in model.intervals:
            execute_interval(model, interval, data_conn, state_conn)

    @interval_lifecycle
    def execute_interval(model, interval, data_conn, state_conn=None):
        ...                       # read / transform / write(conn=data_conn, ...)

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

from bollhav.model.state import ModelLockedError, _run_id_for

logger = logging.getLogger(__name__)


def _state_backend(model):
    """Resolve the state-backend module for `model` from its
    `State.backend` enum — so dispatch is driven by the model, not
    hardcoded. Lazy import to avoid an import cycle (the backend module
    imports from `bollhav.model`). Only call when `model.state` is set."""
    from bollhav.model.state import StateBackend

    if model.state.backend is StateBackend.POSTGRES:
        from bollhav.postgres import state as pg_state

        return pg_state
    raise NotImplementedError(
        f"state backend {model.state.backend!r} (on "
        f"{model.target.full_name!r}) is not implemented — Postgres is the "
        f"only `State.backend` supported. Add a backend module and extend "
        f"`_state_backend` to wire it up."
    )


def _data_backend(model):
    """Resolve the data-side backend module (target asset DDL + the
    MODEL-action runners) from `model.target.database` — model-driven and
    lazily imported, mirroring `_state_backend`. An unset `database`
    (e.g. a VIEW that didn't declare one) uses the Postgres default,
    the only backend the lifecycle hooks implement today."""
    from bollhav.model.database import Database

    db = model.target.database
    if db is None or db is Database.POSTGRES:
        from bollhav.postgres import actions as pg_actions

        return pg_actions
    raise NotImplementedError(
        f"data backend {db!r} on {model.target.full_name!r} is not "
        f"implemented by the lifecycle hooks — Postgres is the only target "
        f"database supported today."
    )


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

        from model.database import Database
        from model.state import StateBackend

        def postgres_state_is_model_locked(model=model) -> bool:
            from bollhav.postgres import state as postgres_state
        
            if not model.state.allow_concurrent_runs:
                if not postgres_state.try_acquire_lock(state_conn, model):
                    raise ModelLockedError(
                        f"another pipeline holds the lock on "
                        f"{model.target.full_name!r} — concurrent runs of "
                        f"the same model are not allowed"
                    )
                return True

            return False


        locked = False
        if model.state_activated and model.state.backend == StateBackend.POSTGRES:
            locked = postgres_state_is_model_locked(model=model)

        if model.target.database is Database.POSTGRES:

        try:
            if model.target.database is Database.POSTGRES:
                from bollhav.postgres.data import PostgresData
                postgres_data = PostgresData(model=model, conn=data_conn)
                postgres_data.run_pre_model_actions()

                if model.target.staging_activated:
                    from bollhav.postgres.staging import gc_orphan_staging_tables
                    try:
                        gc_orphan_staging_tables(data_conn, model)
                    except Exception as exc:  # best-effort; never blocks the run
                        logger.debug("staging: orphan GC skipped for %s — %s", model.target.full_name, exc)

                if model.state_activated:
                    from bollhav.postgres.library import PostgresLibrary
                    postgres_library = PostgresLibrary(conn=state_conn)
                    postgres_library.ensure()
                    postgres_library.register_model(model=model)

                    from bollhav.postgres.state import PostgresState
                    postgres_state = PostgresState(model=model, conn=state_conn)
                    postgres_state.ensure_tables()
                    postgres_state.insert_intervals(
                        run_id=_run_id_for(model),
                        intervals=model.compute_intervals(),
                    )
                    model.intervals = postgres_state.get_actionable_intervals()

                result = func(*args, **kwargs)
                postgres_data.run_post_model_actions()
                return result
        finally:
            if locked and pg is not None:
                try:
                    pg.release_lock(state_conn, model)
                except Exception:
                    # Released automatically when the session ends too.
                    pass

    return wrapper


# def _setup_non_state_model(model, *, data_conn) -> None:
#     """Model setup for a model WITHOUT state tracking.

#     The only thing to do is GC orphan staging tables from crashed runs
#     (staging tables live with the target data, so on `data_conn`).

#     No library registration — only state-tracked models register. A
#     state-less model (including a VIEW) doesn't enforce or get enforced:
#     its own `upstream` entries are documentation, and when a state model
#     references it as an upstream the live check finds no library entry
#     and treats it as documentation (satisfied), rather than blocking."""
#     from bollhav.postgres import staging as pg_staging

#     if model.target.staging is not None:
#         _run_id_for(model)  # stash run_id for staging table naming
#         try:
#             pg_staging.gc_orphan_staging_tables(data_conn, model)
#         except Exception as exc:  # best-effort; never blocks the run
#             logger.debug(
#                 "staging: orphan GC skipped for %s — %s",
#                 model.target.full_name,
#                 exc,
#             )


# def _bootstrap_model(pg, model, *, data_conn, state_conn, state_mode) -> None:
#     """State setup for one model, all on `state_conn` (staging GC on
#     `data_conn`): ensure the state table, register in the library,
#     prefill the contract intervals, and filter `model.intervals` down to
#     the actionable (non-applied) ones the user's loop will process.

#     `state_mode` controls how prefill treats existing rows (DISCOVER
#     preserves applied; BULLDOZER resets) — resolved by the caller, not
#     read from env here, so this stays a pure function of its args."""
#     from bollhav.model.load_models import _resolve_interval_status
#     from bollhav.postgres import library as pg_library
#     from bollhav.postgres import staging as pg_staging

#     run_id = _run_id_for(model)

#     if model.target.staging is not None:
#         try:
#             pg_staging.gc_orphan_staging_tables(data_conn, model)
#         except Exception as exc:  # best-effort; never blocks the run
#             logger.debug("staging: orphan GC skipped for %s — %s", model.target.full_name, exc)

#     pg.ensure_tables(model, conn=state_conn)
#     pg_library.ensure_library(state_conn)
#     pg_library.register(state_conn, model)

#     contract = list(model.compute_intervals())
#     upstreams = list(model.upstream)
#     rows = []
#     for interval in contract:
#         status, reason = _resolve_interval_status(
#             state_conn, interval=interval, upstream_names=upstreams
#         )
#         rows.append((interval, status, reason))
#     pg.prefill(
#         model,
#         run_id=run_id,
#         intervals=rows,
#         state_mode=state_mode,
#         conn=state_conn,
#     )
#     model.intervals = tuple(pg.read_actionable(model, conn=state_conn))


def interval_lifecycle(func: Callable) -> Callable:
    """Bracket one interval: gate on applied, take the per-interval
    advisory lock, mark running, run the user's work, then mark applied
    (or record failure). All state writes go on `state_conn`. A
    state-less model (or a call without an `interval`) is a pass-through.
    """
    sig = inspect.signature(func)

    @wraps(func)
    def wrapper(*args, **kwargs):
        bound = sig.bind(*args, **kwargs)
        bound.apply_defaults()
        model = bound.arguments.get("model")
        interval = bound.arguments.get("interval")
        _, state_conn = _conns(bound.arguments)

        if model is None or not model.state_activated or interval is None:
            return func(*args, **kwargs)

        pg = _state_backend(model)
        since, until = interval.since, interval.until
        run_id = _run_id_for(model)

        if pg.is_applied(model, since=since, until=until, conn=state_conn):
            logger.debug(
                "state: gate skipped applied %s..%s for %s",
                since,
                until,
                model.target.full_name,
            )
            return None

        if not pg.try_acquire_interval_lock(state_conn, model, since, until):
            logger.debug(
                "state: interval lock held by another worker, skipping "
                "%s..%s on %s",
                since,
                until,
                model.target.full_name,
            )
            return None
        try:
            if model.upstream:
                ok, reason = pg.is_upstream_satisfied_live(
                    state_conn, model, since, until
                )
                if not ok:
                    pg.mark_blocked(
                        model,
                        run_id=run_id,
                        since=since,
                        until=until,
                        reason=reason or "",
                        conn=state_conn,
                    )
                    return None

            pg.mark_running(
                model, run_id=run_id, since=since, until=until, conn=state_conn
            )

            try:
                result = func(*args, **kwargs)
            except Exception as exc:
                pg.record_failure(
                    model,
                    run_id=run_id,
                    since=since,
                    until=until,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                    traceback_text=_tb.format_exc(),
                    update_state=True,
                    conn=state_conn,
                )
                raise

            # Data is committed; flip the state row → applied (separate,
            # non-atomic step on the state connection).
            pg.mark_applied(
                model, run_id=run_id, since=since, until=until, conn=state_conn
            )
            return result
        finally:
            try:
                pg.release_interval_lock(state_conn, model, since, until)
            except Exception:
                pass

    return wrapper


__all__ = ["model_lifecycle", "interval_lifecycle"]
