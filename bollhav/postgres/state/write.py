"""Operator-facing state edits — the WRITE counterpart to `read.py`.

These are explicit, ad-hoc operator actions (the kind a GUI exposes), distinct
from the lifecycle's own state methods: those need a `Model`, carry run-id
semantics, and `clear_state` is heavier (drops the table + deregisters + refuses
prod). The functions here are addressed by `full_name`, resolved against the
library — they never reconstruct a `Model` (the GUI has names, not models),
and `conn`-first like `read.py` so a future HTTP endpoint is a thin wrapper.

Every operation **flips rows `applied` → `pending`** (clearing `applied_at` /
`blocked_reason`) so the next run re-backfills them. One uniform operation
covers **both** fixed and flexible models, because in each a non-applied row
means "not covered, re-run it":

- **fixed** (a grid): `get_actionable_intervals` selects `status <> 'applied'`,
  so the pending row is picked up.
- **flexible** (a coverage set): the gap query counts only `status = 'applied'`
  as covered, so a pending row is excluded from coverage → its range reads as
  uncovered and is re-discovered.

The row and its `(since, until)` boundary are kept (no delete, no re-chunk), and
the library registration survives (unlike `clear_state`). A `running` row is
never touched — a live run owns it.

Scoping: `library_schema` selects the environment's library (the registered
`state_schema` / `state_table` come from that env's row), defaulting to prod
`z_bollhav`. Resetting forces a reprocess, so callers targeting prod should
confirm intent — these functions log every mutation but do not themselves gate.

Note: resetting acts on whole rows. Once flexible coverage coalescing exists,
re-backfilling a *sub-range* of a wider applied row will need range subtraction
(splitting the row) — a follow-up tied to the coverage engine.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from psycopg import sql

from ._ddl import LIBRARY_SCHEMA
from .state import PostgresState

if TYPE_CHECKING:
    import psycopg

logger = logging.getLogger(__name__)

# Flip a row back to actionable without re-chunking: keep (since, until) and
# run_id (NOT NULL), just clear the applied/blocked marks. Never touch a row a
# live run is processing.
_RESET_SET = (
    "SET status = 'pending', applied_at = NULL, blocked_reason = NULL "
    "WHERE status <> 'running'"
)


def _resolve_state_table(
    conn: "psycopg.Connection", full_name: str, library_schema: str
) -> tuple[str, str, bool] | None:
    """`(state_schema, state_table, fixed_intervals)` for a registered, state-
    tracked model — or None if it isn't registered or carries no state table (a
    library-only row)."""
    entry = PostgresState.lookup_model(conn, full_name, library_schema)
    if entry is None or entry.state_schema is None or entry.state_table is None:
        return None
    return entry.state_schema, entry.state_table, entry.fixed_intervals


def reset_interval(
    conn: "psycopg.Connection",
    full_name: str,
    since,
    until,
    *,
    library_schema: str = LIBRARY_SCHEMA,
) -> int:
    """Make the next run re-backfill ONE interval, matched by `(since, until)`.

    - **fixed** model: flip the matching row `applied` → `pending` (pass
      `None, None` for the whole-table / one-shot row).
    - **flexible** model: `uncover` the span via range subtraction — a sub-range
      of a wider coalesced applied row is split out, not the whole row. Needs a
      real window (`since`/`until` non-None).

    Skips a `running` row. Returns rows affected (0 if no such interval, or the
    model isn't registered)."""
    resolved = _resolve_state_table(conn, full_name, library_schema)
    if resolved is None:
        logger.warning(
            "state.write: %s is not registered / has no state table", full_name
        )
        return 0
    schema, table, fixed = resolved

    if not fixed:
        if since is None or until is None:
            logger.warning(
                "state.write: reset_interval on flexible %s needs a window "
                "(since/until)",
                full_name,
            )
            return 0
        from .state_table import uncover_span

        n = uncover_span(conn, schema, table, since, until)
        logger.info(
            "state.write: uncovered [%s, %s) of %s — removed %d applied row(s)",
            since,
            until,
            full_name,
            n,
        )
        return n

    if since is None and until is None:
        window = sql.SQL("since IS NULL AND until IS NULL")
        params: list = []
    else:
        window = sql.SQL("since = %s AND until = %s")
        params = [since, until]
    n = conn.execute(
        sql.SQL("UPDATE {schema}.{table} " + _RESET_SET + " AND ({window})").format(
            schema=sql.Identifier(schema),
            table=sql.Identifier(table),
            window=window,
        ),
        params,
    ).rowcount
    logger.info(
        "state.write: reset %d interval(s) of %s to pending (since=%s, until=%s)",
        n,
        full_name,
        since,
        until,
    )
    return n


def reset_model(
    conn: "psycopg.Connection",
    full_name: str,
    *,
    library_schema: str = LIBRARY_SCHEMA,
) -> int:
    """Flip EVERY interval row of one model's state table `applied` → `pending`
    so the next run reprocesses the whole window. Keeps the table, the interval
    boundaries, and the library registration (unlike `clear_state`, which drops
    + deregisters). Skips `running` rows. Returns rows affected."""
    resolved = _resolve_state_table(conn, full_name, library_schema)
    if resolved is None:
        logger.warning(
            "state.write: %s is not registered / has no state table", full_name
        )
        return 0
    schema, table, _fixed = resolved
    n = conn.execute(
        sql.SQL("UPDATE {schema}.{table} " + _RESET_SET).format(
            schema=sql.Identifier(schema),
            table=sql.Identifier(table),
        )
    ).rowcount
    logger.info("state.write: reset all %d interval(s) of %s to pending", n, full_name)
    return n


def reset_models(
    conn: "psycopg.Connection",
    selector: "str | list[str]",
    *,
    library_schema: str = LIBRARY_SCHEMA,
) -> dict[str, int]:
    """Bulk `reset_model` across many models with one call. `selector` is either
    a **tag expression** (a `str`, e.g. `"[raw]"` or `"raw & clean"` — the same
    syntax the run selector uses) or an explicit **list of full_names**. Returns
    `{full_name: rows_affected}`. Runs atomically — either every model resets or
    none does."""
    if isinstance(selector, str):
        from .read import match_tags

        full_names = [m["name"] for m in match_tags(conn, selector, library_schema)]
    else:
        full_names = list(selector)

    out: dict[str, int] = {}
    with conn.transaction():
        for full_name in full_names:
            out[full_name] = reset_model(
                conn, full_name, library_schema=library_schema
            )
    total = sum(out.values())
    logger.info(
        "state.write: bulk reset %d interval(s) across %d model(s)", total, len(out)
    )
    return out
