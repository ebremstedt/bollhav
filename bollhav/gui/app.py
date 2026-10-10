import heapq
import json
import logging
import os
import threading
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Final, LiteralString

import psycopg
from fastapi import FastAPI, HTTPException, Response  # pyright: ignore[reportMissingImports]  # optional gui extra
from psycopg import sql
from pydantic import BaseModel  # pyright: ignore[reportMissingImports]  # optional gui extra
from bollhav.postgres.state import LIBRARY_SCHEMA, read, write

# Reports every precompute (catalog, duration, model count); `bollhav-gui`
# configures logging so this reaches the pod log (uvicorn only sets up its own).
logger = logging.getLogger("bollhav.gui")

# A catalog is one state database. A pipeline keeps its state and library in
# the database it writes to (unless it is given a separate state connection),
# so every database holds a library of its own and the GUI needs one
# connection per catalog to show them all. Configure one env var per catalog,
# BOLLHAV_STATE_DSN_<NAME>; the name, lowercased, is what the header's catalog
# switcher shows and what `?catalog=` selects. The plain BOLLHAV_STATE_DSN
# (the demo, single-database setups) is the catalog "default" when no named
# one is set.
DSN_PREFIX = "BOLLHAV_STATE_DSN_"


def _catalogs() -> dict[str, str]:
    named = {
        key[len(DSN_PREFIX) :].lower(): value
        for key, value in os.environ.items()
        if key.startswith(DSN_PREFIX) and value
    }
    if named:
        return dict(sorted(named.items()))
    return {
        "default": os.environ.get(
            "BOLLHAV_STATE_DSN",
            "postgresql://postgres:postgres@localhost:5432/postgres",
        )
    }


CATALOGS = _catalogs()
# The catalog read when a request names none: LINEAGE_DEFAULT_CATALOG, else
# the first by name.
DEFAULT_CATALOG = os.environ.get("LINEAGE_DEFAULT_CATALOG") or next(iter(CATALOGS))
if DEFAULT_CATALOG not in CATALOGS:
    DEFAULT_CATALOG = next(iter(CATALOGS))

# ── the cache ──────────────────────────────────────────────────────────────
# The graph's status lights, the gaps, the grid and the runs each ask every
# model's state table a question or three, so a big library costs thousands of
# queries. Every catalog's PROD library (`z_bollhav`) is therefore computed in
# the background, every LINEAGE_REFRESH_SECONDS, and served from memory; one
# model can be recomputed on its own (POST /refresh/{name}, and after a state
# reset). Suffixed dev / PR environments are small and always read live.
# LINEAGE_REFRESH_SECONDS=0 switches the cache off (everything live).
#
# The cache lives in the pod's memory, so its shape matters: a library of a
# few thousand models with hundreds of run rows each is close to a million
# rows. The grid, nearly all of it, is kept as one pre-serialized JSON string
# per row (a few hundred bytes) rather than a dict (about six times that),
# and the cross-model runs list is trimmed to its top rows model by model
# instead of being built in full and cut afterwards.
REFRESH_SECONDS = int(os.environ.get("LINEAGE_REFRESH_SECONDS", "300"))
CACHE_RUNS = 400  # run rows kept per model for the grid (the UI offers up to 365)
CACHE_RECENT = 500  # rows kept for the cross-model runs and errors lists


@dataclass(frozen=True)
class Snapshot:
    computed_at: str
    graph: dict
    gaps: list
    grid: list  # [(full_name, [row JSON, …]), …] newest window first, by name
    runs: list
    errors: list


_snapshots: dict[str, Snapshot] = {}  # catalog -> its prod library, precomputed
_refreshing: set[str] = set()  # catalogs being computed right now
_wanted: set[str] = set()  # catalogs a refresh was asked for (POST /refresh)
_wake = threading.Event()
_lock = threading.Lock()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# A model's state rows, the same shape bollhav's read helpers give (and the
# same two orderings): newest window first for the grid, newest run first
# for the runs list. Read here model by model so the precompute never holds
# more than one model's rows as dicts.
# Final so they stay literal strings, which is what psycopg's sql.SQL takes.
_STATE_COLUMNS: Final = (
    "status, since, until, applied_at, run_id, temporality, blocked_reason"
)
GRID_ORDER: Final = "since DESC NULLS LAST, applied_at DESC NULLS LAST"
RUNS_ORDER: Final = "applied_at DESC NULLS LAST, since DESC NULLS LAST"


def _stateful_models(conn, schema: str = LIBRARY_SCHEMA) -> list[tuple[str, str, str]]:
    """(full_name, state_schema, state_table) for every model in the library
    whose state table exists, by name."""
    if not read._table_exists(conn, schema, read.LIBRARY_TABLE):
        return []
    rows = conn.execute(
        sql.SQL(
            "SELECT full_name, state_schema, state_table FROM {schema}.{table} "
            "WHERE state_table IS NOT NULL ORDER BY full_name"
        ).format(
            schema=sql.Identifier(schema), table=sql.Identifier(read.LIBRARY_TABLE)
        )
    ).fetchall()
    return [
        (name, st_schema, st_table)
        for name, st_schema, st_table in rows
        if st_schema and st_table and read._table_exists(conn, st_schema, st_table)
    ]


def _state_rows(
    conn, st_schema: str, st_table: str, order: LiteralString, limit: int
) -> list[dict]:
    rows = conn.execute(
        sql.SQL(
            "SELECT "
            + _STATE_COLUMNS
            + " FROM {schema}.{table} ORDER BY "
            + order
            + " LIMIT %s"
        ).format(schema=sql.Identifier(st_schema), table=sql.Identifier(st_table)),
        [limit],
    ).fetchall()
    return [
        {
            "status": status,
            "since": read._iso(since),
            "until": read._iso(until),
            "applied_at": read._iso(applied_at),
            "run_id": str(run_id) if run_id is not None else None,
            "kind": kind,
            "blocked_reason": blocked_reason,
        }
        for status, since, until, applied_at, run_id, kind, blocked_reason in rows
    ]


def _row_json(row: dict) -> str:
    return json.dumps(row, separators=(",", ":"))


def _run_key(row: dict) -> tuple[str, str]:
    return (row["applied_at"] or "", row["since"] or "")


def _compute(catalog: str) -> Snapshot:
    with _conn(catalog) as c:
        graph = read.get_graph(c, schema=LIBRARY_SCHEMA)
        gaps = read.get_gaps_grouped(c, schema=LIBRARY_SCHEMA)
        grid: list[tuple[str, list[str]]] = []
        runs: list[dict] = []
        for full_name, st_schema, st_table in _stateful_models(c):
            grid.append(
                (
                    full_name,
                    [
                        _row_json(r)
                        for r in _state_rows(
                            c, st_schema, st_table, GRID_ORDER, CACHE_RUNS
                        )
                    ],
                )
            )
            recent = [
                {"full_name": full_name, **r}
                for r in _state_rows(c, st_schema, st_table, RUNS_ORDER, CACHE_RECENT)
            ]
            runs = heapq.nlargest(CACHE_RECENT, runs + recent, key=_run_key)
        errors = read.get_errors(c, limit=CACHE_RECENT, schema=LIBRARY_SCHEMA)
    return Snapshot(
        computed_at=_now(), graph=graph, gaps=gaps, grid=grid, runs=runs, errors=errors
    )


def _refresh(catalog: str) -> None:
    with _lock:
        _refreshing.add(catalog)
    started = time.monotonic()
    try:
        snapshot = _compute(catalog)
        with _lock:
            _snapshots[catalog] = snapshot
        logger.info(
            "catalog %s computed in %.1fs: %d models",
            catalog,
            time.monotonic() - started,
            sum(1 for n in snapshot.graph["nodes"] if n.get("type") == "model"),
        )
    except Exception:
        logger.exception("catalog %s: computing its library failed", catalog)
    finally:
        with _lock:
            _refreshing.discard(catalog)


def _refresher() -> None:
    """Every catalog at startup and then every REFRESH_SECONDS; in between,
    whatever POST /refresh asked for, as soon as it is asked."""
    due = 0.0
    while True:
        with _lock:
            wanted = set(_wanted)
            _wanted.clear()
        if time.monotonic() >= due:
            wanted |= set(CATALOGS)
            due = time.monotonic() + REFRESH_SECONDS
        for catalog in sorted(wanted):
            _refresh(catalog)
        _wake.wait(timeout=max(0.0, due - time.monotonic()))
        _wake.clear()


@asynccontextmanager
async def lifespan(app: FastAPI):
    if REFRESH_SECONDS > 0:
        threading.Thread(
            target=_refresher, name="lineage-refresher", daemon=True
        ).start()
    yield


app = FastAPI(title="LINEAGE", lifespan=lifespan)


# The one write path (POST /state/{name}/reset) can be switched off for a
# deployment with LINEAGE_READ_ONLY=1; /config tells the GUI so it hides the
# controls.
def _writable() -> bool:
    return os.environ.get("LINEAGE_READ_ONLY", "").lower() not in ("1", "true", "yes")


@app.get("/config")
def config():
    """Runtime UI config from env vars. The frontend narrows the lineage graph
    to one random model on load so slow clients never lay out the whole DAG;
    `default_tags` lets a deployment pin a tag filter instead. `writable` says
    whether state resets are allowed; `default_catalog` is the catalog read
    when the URL names none; `refresh_seconds` how often the cache is rebuilt
    (0 = no cache)."""
    return {
        "title": os.environ.get("LINEAGE_TITLE") or "model explorer",
        "default_tags": os.environ.get("LINEAGE_DEFAULT_TAGS") or None,
        "writable": _writable(),
        "default_catalog": DEFAULT_CATALOG,
        "refresh_seconds": REFRESH_SECONDS,
    }


@app.get("/catalogs")
def catalogs():
    """The catalogs (state databases) this deployment reads, the default
    first. The GUI's catalog switcher lists them."""
    names = [DEFAULT_CATALOG] + [c for c in CATALOGS if c != DEFAULT_CATALOG]
    return [{"catalog": c} for c in names]


# Every endpoint takes an optional `?catalog=<name>` — which state database to
# read (the default catalog when absent) — and most take `?env=<schema>`, the
# library schema inside it (prod `z_bollhav` by default, or a suffixed dev/PR
# env). The GUI's two header switchers pass them; `/catalogs` and
# `/environments` list the choices.
def _catalog(catalog: str | None) -> str:
    name = catalog or DEFAULT_CATALOG
    if name not in CATALOGS:
        raise HTTPException(
            status_code=404,
            detail=f"catalog {name!r} is not configured (have: {', '.join(CATALOGS)})",
        )
    return name


def _conn(catalog: str | None = None, *, autocommit: bool = True):
    """A connection to the catalog's state database. Reads run in autocommit
    mode: the read helpers ask every model's state table a question or two,
    and inside one transaction the locks on those tables (and their indexes)
    pile up until Postgres runs out of lock slots — "out of shared memory",
    max_locks_per_transaction — on a library of a few thousand models. With
    autocommit each statement lets go of its locks as soon as it completes.
    The one write path passes autocommit=False so its statements stay one
    transaction."""
    return psycopg.connect(CATALOGS[_catalog(catalog)], autocommit=autocommit)


def _schema(env: str | None) -> str:
    return env or LIBRARY_SCHEMA


def _cached(catalog: str | None, env: str | None) -> Snapshot | None:
    """The precomputed snapshot for this catalog + env, or None when the request
    is served live (dev env, or the cache is off). 503 while a catalog is
    still being computed for the first time after a start."""
    name = _catalog(catalog)
    if REFRESH_SECONDS <= 0 or _schema(env) != LIBRARY_SCHEMA:
        return None
    snapshot = _snapshots.get(name)
    if snapshot is None:
        raise HTTPException(
            status_code=503,
            detail=f"catalog {name!r} is still being computed",
            headers={"Retry-After": "5"},
        )
    return snapshot


@app.get("/freshness")
def freshness(catalog: str | None = None, env: str | None = None):
    """Whether this catalog + env is served from the cache, when that cache was
    computed, and whether a recompute is under way. The header shows it."""
    name = _catalog(catalog)
    cached = REFRESH_SECONDS > 0 and _schema(env) == LIBRARY_SCHEMA
    snapshot = _snapshots.get(name) if cached else None
    with _lock:
        refreshing = name in _refreshing or name in _wanted
    return {
        "cached": cached,
        "computed_at": snapshot.computed_at if snapshot else None,
        "refreshing": cached and refreshing,
        "refresh_seconds": REFRESH_SECONDS,
    }


@app.post("/refresh")
def refresh(catalog: str | None = None):
    """Recompute this catalog's cache now, in the background; poll /freshness
    for the new `computed_at`."""
    name = _catalog(catalog)
    if REFRESH_SECONDS <= 0:
        return {"queued": False}
    with _lock:
        _wanted.add(name)
    _wake.set()
    return {"queued": True}


def _one(rows: list[dict]) -> dict | None:
    return rows[0] if rows else None


def _refresh_model(catalog: str, full_name: str) -> str:
    """Recompute one model's share of the cache — its status lights, gaps
    entry, grid row, and its rows in the runs and errors lists — and swap in
    a patched snapshot. Returns the time of the recompute."""
    with _conn(catalog) as c:
        node = read.get_model(c, full_name)
        if node is None:
            raise HTTPException(
                status_code=404, detail=f"{full_name!r} is not registered"
            )
        meta = read.get_model_metadata(c, full_name) or {}
        st_schema, st_table = node["state_schema"], node["state_table"]
        has_blocked, has_stale = read._blocked_kinds(c, st_schema, st_table)
        lights = {
            "tags": meta.get("tags", []) or [],
            "last_seen": node["last_seen"],
            "has_error": read._has_status(c, st_schema, st_table, "error"),
            "has_running": read._has_status(c, st_schema, st_table, "running"),
            "has_blocked": has_blocked,
            "has_stale": has_stale,
        }
        gaps_row = _one(
            read.get_gaps_grouped(c, schema=LIBRARY_SCHEMA, full_name=full_name)
        )
        stateful = st_schema and st_table and read._table_exists(c, st_schema, st_table)
        grid_rows = (
            [
                _row_json(r)
                for r in _state_rows(c, st_schema, st_table, GRID_ORDER, CACHE_RUNS)
            ]
            if stateful
            else []
        )
        run_rows = (
            [
                {"full_name": full_name, **r}
                for r in _state_rows(c, st_schema, st_table, RUNS_ORDER, CACHE_RECENT)
            ]
            if stateful
            else []
        )
        error_rows = read.get_errors(c, full_name=full_name, limit=CACHE_RECENT)

    computed_at = _now()
    with _lock:
        old = _snapshots.get(catalog)
        if old is None:
            return computed_at
        nodes = [
            {**n, **lights} if n.get("name") == full_name else n
            for n in old.graph["nodes"]
        ]
        if any(g["full_name"] == full_name for g in old.gaps):
            gaps = [gaps_row if g["full_name"] == full_name else g for g in old.gaps]
        else:
            gaps = old.gaps + ([gaps_row] if gaps_row else [])
        if any(fn == full_name for fn, _ in old.grid):
            grid = [
                (fn, grid_rows if fn == full_name else rows) for fn, rows in old.grid
            ]
        else:
            grid = old.grid + ([(full_name, grid_rows)] if stateful else [])
        runs = heapq.nlargest(
            CACHE_RECENT,
            [r for r in old.runs if r["full_name"] != full_name] + run_rows,
            key=_run_key,
        )
        errors = [e for e in old.errors if e["full_name"] != full_name] + error_rows
        errors.sort(key=lambda e: e["created_at"] or "", reverse=True)
        _snapshots[catalog] = Snapshot(
            computed_at=old.computed_at,
            graph={**old.graph, "nodes": nodes},
            gaps=gaps if gaps_row else old.gaps,
            grid=grid,
            runs=runs,
            errors=errors[:CACHE_RECENT],
        )
    return computed_at


@app.post("/refresh/{full_name}")
def refresh_model(full_name: str, catalog: str | None = None, env: str | None = None):
    """Recompute one model in the cache (its lights, gaps, grid row, recent
    runs and errors) right now. A no-op for a live (dev env) read."""
    name = _catalog(catalog)
    if REFRESH_SECONDS <= 0 or _schema(env) != LIBRARY_SCHEMA:
        return {"full_name": full_name, "cached": False}
    return {
        "full_name": full_name,
        "cached": True,
        "computed_at": _refresh_model(name, full_name),
    }


@app.get("/environments")
def environments(catalog: str | None = None):
    """The bollhav library schemas in the catalog's database (prod + suffixed envs)."""
    with _conn(catalog) as c:
        return read.list_environments(c)


@app.get("/models")
def models(env: str | None = None, catalog: str | None = None):
    with _conn(catalog) as c:
        return read.list_models(c)


@app.get("/lineage/{full_name}")
def lineage(full_name: str, catalog: str | None = None):
    with _conn(catalog) as c:
        result = read.get_lineage(c, full_name)
    if result is None:
        raise HTTPException(status_code=404, detail=f"{full_name!r} is not registered")
    return result


@app.get("/tree/{full_name}")
def tree(full_name: str, catalog: str | None = None):
    with _conn(catalog) as c:
        result = read.get_upstream_tree(c, full_name)
    if result is None:
        raise HTTPException(status_code=404, detail=f"{full_name!r} is not registered")
    return result


@app.get("/state/{full_name}")
def state(
    full_name: str, limit: int = 50, env: str | None = None, catalog: str | None = None
):
    with _conn(catalog) as c:
        return read.get_recent_state(c, full_name, limit=limit, schema=_schema(env))


class Window(BaseModel):
    since: datetime | None = None
    until: datetime | None = None


class ResetRequest(BaseModel):
    """Exactly one of: `all` (the whole model), `intervals` (one or more
    windows matched exactly — a null/null window is the whole-table row), or
    `range` (every interval inside [since, until))."""

    all: bool = False
    intervals: list[Window] | None = None
    range: Window | None = None


@app.post("/state/{full_name}/reset")
def reset_state(
    full_name: str,
    body: ResetRequest,
    env: str | None = None,
    catalog: str | None = None,
):
    """Make the next run redo part of a model's state. The chosen rows flip
    `applied` → `pending` (a flexible model's coverage is uncovered instead);
    rows and history are kept and a `running` row is never touched. See
    `bollhav.postgres.state.write`. Off when LINEAGE_READ_ONLY is set. A
    cached model is recomputed afterwards so the reset shows at once."""
    if not _writable():
        raise HTTPException(
            status_code=403, detail="state resets are off (LINEAGE_READ_ONLY)"
        )
    chosen = [
        k
        for k, v in (
            ("all", body.all),
            ("intervals", body.intervals),
            ("range", body.range),
        )
        if v
    ]
    if len(chosen) != 1:
        raise HTTPException(
            status_code=400, detail="give exactly one of: all, intervals, range"
        )
    schema = _schema(env)
    with _conn(catalog, autocommit=False) as c:
        if read.get_model(c, full_name, schema=schema) is None:
            raise HTTPException(
                status_code=404, detail=f"{full_name!r} is not registered"
            )
        if body.all:
            n = write.reset_model(c, full_name, library_schema=schema)
        elif body.range is not None:
            if body.range.since is None or body.range.until is None:
                raise HTTPException(
                    status_code=400, detail="a range needs both since and until"
                )
            n = write.reset_range(
                c, full_name, body.range.since, body.range.until, library_schema=schema
            )
        elif body.intervals:
            n = write.reset_intervals(
                c,
                full_name,
                [(w.since, w.until) for w in body.intervals],
                library_schema=schema,
            )
        else:  # unreachable: `chosen` guaranteed one of the three
            raise HTTPException(
                status_code=400, detail="give exactly one of: all, intervals, range"
            )
    if REFRESH_SECONDS > 0 and schema == LIBRARY_SCHEMA:
        try:
            _refresh_model(_catalog(catalog), full_name)
        except Exception:
            logger.exception("%s: recomputing after a reset failed", full_name)
    return {"full_name": full_name, "reset": n}


@app.get("/downstreams/{full_name}")
def downstreams(full_name: str, catalog: str | None = None):
    with _conn(catalog) as c:
        return {
            "full_name": full_name,
            "downstreams": read.get_downstreams(c, full_name),
        }


@app.get("/graph")
def graph(env: str | None = None, catalog: str | None = None):
    """The whole graph; from the cache for a prod library (then carrying
    `computed_at`), live otherwise."""
    snapshot = _cached(catalog, env)
    if snapshot is not None:
        return {**snapshot.graph, "computed_at": snapshot.computed_at}
    with _conn(catalog) as c:
        return read.get_graph(c, schema=_schema(env))


@app.get("/match")
def match(expr: str, env: str | None = None, catalog: str | None = None):
    """Full names of models matching a bollhav tag expression (the `[group]`
    syntax), within the selected env. 400 on a malformed expression."""
    with _conn(catalog) as c:
        try:
            return {
                "expr": expr,
                "models": read.match_tags(c, expr, schema=_schema(env)),
            }
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))


@app.get("/model/{full_name}")
def model(full_name: str, env: str | None = None, catalog: str | None = None):
    with _conn(catalog) as c:
        result = read.get_model_metadata(c, full_name, schema=_schema(env))
    if result is None:
        raise HTTPException(status_code=404, detail=f"{full_name!r} is not registered")
    return result


@app.get("/errors")
def errors(
    full_name: str | None = None,
    limit: int = 100,
    env: str | None = None,
    catalog: str | None = None,
):
    """Recent errors, newest first: across every model from the cache for a
    prod library, live for one model or a dev env."""
    if full_name is None:
        snapshot = _cached(catalog, env)
        if snapshot is not None:
            return snapshot.errors[:limit]
    with _conn(catalog) as c:
        return read.get_errors(c, full_name=full_name, limit=limit, schema=_schema(env))


@app.get("/runs")
def runs(limit: int = 50, env: str | None = None, catalog: str | None = None):
    """Recent run/interval state rows across every model (newest first)."""
    snapshot = _cached(catalog, env)
    if snapshot is not None:
        return snapshot.runs[:limit]
    with _conn(catalog) as c:
        return read.get_recent_runs(c, limit=limit, schema=_schema(env))


@app.get("/grid")
def grid(limit: int = 40, env: str | None = None, catalog: str | None = None):
    """Per-model run history for the grid view — each model with its most
    recent `limit` run rows (from the cache, at most CACHE_RUNS of them)."""
    snapshot = _cached(catalog, env)
    if snapshot is not None:
        # the rows are already JSON: splice the slices together, no re-encoding
        body = "[%s]" % ",".join(
            '{"full_name":%s,"runs":[%s]}'
            % (json.dumps(full_name), ",".join(rows[:limit]))
            for full_name, rows in snapshot.grid
        )
        return Response(content=body, media_type="application/json")
    with _conn(catalog) as c:
        return read.get_runs_grouped(c, limit=limit, schema=_schema(env))


@app.get("/gaps")
def gaps(env: str | None = None, catalog: str | None = None):
    """Per-model backfill gaps — the spans of each model's contract window that
    aren't yet `applied` in its state table (what still needs backfilling)."""
    snapshot = _cached(catalog, env)
    if snapshot is not None:
        return snapshot.gaps
    with _conn(catalog) as c:
        return read.get_gaps_grouped(c, schema=_schema(env))
