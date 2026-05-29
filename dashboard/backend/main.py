"""Bollhav dashboard — read-only FastAPI backend.

Serves JSON over `/api/*` for the Svelte frontend AND the built static
frontend out of `dashboard/frontend/dist/`. One process; no auth.

Env:
    TARGET_DSN     postgresql://...   (read-only access is sufficient)
    DASHBOARD_HOST default 127.0.0.1
    DASHBOARD_PORT default 5173

Endpoints:
    GET /api/library                      → all library rows (the DAG)
    GET /api/state/{full_name}            → all state rows for a model,
                                            ordered by since ASC
    GET /api/summary                      → per-model count rollups
                                            (pending/running/applied/blocked/error)
                                            for everything in the library
    GET /api/errors?limit=50              → recent errors UNION across all
                                            *_errors tables; `full_name`
                                            attribution column included
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Generator

import psycopg
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from psycopg import sql
from psycopg.rows import dict_row

# Same library schema as bollhav.postgres.library — keep them in sync.
LIBRARY_SCHEMA = "z_bollhav"
LIBRARY_TABLE = "model_library"

app = FastAPI(title="bollhav dashboard", version="0")

# Permissive CORS for local dev (Svelte dev server on a different port).
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


@contextmanager
def _conn() -> Generator[psycopg.Connection, None, None]:
    dsn = os.environ.get("TARGET_DSN")
    if not dsn:
        raise HTTPException(500, "TARGET_DSN env var is not set on the backend")
    try:
        conn = psycopg.connect(dsn, row_factory=dict_row)
    except psycopg.Error as exc:
        raise HTTPException(503, f"DB unreachable: {exc}") from exc
    try:
        yield conn
    finally:
        conn.close()


def _split_full_name(full_name: str) -> tuple[str, str]:
    """`warehouse.orders` → `('warehouse', 'orders')`."""
    if "." not in full_name:
        raise HTTPException(
            400, f"full_name must be `<schema>.<name>`, got {full_name!r}"
        )
    schema, name = full_name.rsplit(".", 1)
    return schema, name


# ── /api/library — the DAG source-of-truth ──────────────────────────


@app.get("/api/library")
def library() -> list[dict]:
    """Every model bollhav has ever seen, with its upstream
    declarations and pointers to its state/errors tables. Ordered
    alphabetically so the UI renders rows consistently."""
    with _conn() as conn:
        rows = conn.execute(
            sql.SQL(
                "SELECT full_name, upstream, state_schema, state_table, last_seen "
                "FROM {schema}.{table} ORDER BY full_name"
            ).format(
                schema=sql.Identifier(LIBRARY_SCHEMA),
                table=sql.Identifier(LIBRARY_TABLE),
            )
        ).fetchall()
    return [
        {
            "full_name": r["full_name"],
            "upstream": list(r["upstream"]) if r["upstream"] is not None else [],
            "state_schema": r["state_schema"],
            "state_table": r["state_table"],
            "last_seen": r["last_seen"].isoformat() if r["last_seen"] else None,
        }
        for r in rows
    ]


# ── /api/state/{full_name} — per-model intervals ────────────────────


@app.get("/api/state/{full_name}")
def state(full_name: str) -> list[dict]:
    """All state rows for one model, ordered by `since` ASC so the
    frontend can render them left-to-right as a timeline grid."""
    with _conn() as conn:
        # Resolve via library so renames/moves are picked up.
        lib_row = conn.execute(
            sql.SQL(
                "SELECT state_schema, state_table FROM {schema}.{table} "
                "WHERE full_name = %s"
            ).format(
                schema=sql.Identifier(LIBRARY_SCHEMA),
                table=sql.Identifier(LIBRARY_TABLE),
            ),
            [full_name],
        ).fetchone()
        if not lib_row:
            raise HTTPException(404, f"{full_name!r} not in library")

        state_q = sql.SQL(
            "SELECT id, since, until, status, blocked_reason, applied_at, "
            "run_id::text AS run_id "
            "FROM {schema}.{table} ORDER BY since"
        ).format(
            schema=sql.Identifier(lib_row["state_schema"]),
            table=sql.Identifier(lib_row["state_table"]),
        )
        try:
            rows = conn.execute(state_q).fetchall()
        except psycopg.errors.UndefinedTable:
            # Registered in library but the state table doesn't exist
            # yet (model declared but never bootstrapped). Return [].
            return []

    return [
        {
            "id": r["id"],
            "since": r["since"].isoformat(),
            "until": r["until"].isoformat(),
            "status": r["status"],
            "blocked_reason": r["blocked_reason"],
            "applied_at": r["applied_at"].isoformat() if r["applied_at"] else None,
            "run_id": r["run_id"],
        }
        for r in rows
    ]


# ── /api/summary — per-model count rollup ───────────────────────────


@app.get("/api/summary")
def summary() -> list[dict]:
    """For each model in the library, return counts per status.
    Used by the main grid header to draw the per-row totals."""
    rollup: list[dict] = []
    with _conn() as conn:
        lib_rows = conn.execute(
            sql.SQL(
                "SELECT full_name, upstream, state_schema, state_table "
                "FROM {schema}.{table} ORDER BY full_name"
            ).format(
                schema=sql.Identifier(LIBRARY_SCHEMA),
                table=sql.Identifier(LIBRARY_TABLE),
            )
        ).fetchall()
        for lib in lib_rows:
            counts = {
                "pending": 0,
                "running": 0,
                "applied": 0,
                "blocked": 0,
                "error": 0,
            }
            try:
                state_rows = conn.execute(
                    sql.SQL(
                        "SELECT status, count(*) AS n FROM {schema}.{table} "
                        "GROUP BY status"
                    ).format(
                        schema=sql.Identifier(lib["state_schema"]),
                        table=sql.Identifier(lib["state_table"]),
                    )
                ).fetchall()
                for s in state_rows:
                    counts[s["status"]] = s["n"]
            except psycopg.errors.UndefinedTable:
                conn.rollback()
            rollup.append(
                {
                    "full_name": lib["full_name"],
                    "upstream": list(lib["upstream"])
                    if lib["upstream"] is not None
                    else [],
                    "counts": counts,
                }
            )
    return rollup


# ── /api/errors — global error stream ───────────────────────────────


@app.get("/api/errors")
def errors(limit: int = 50) -> list[dict]:
    """Recent errors across every model. Builds a UNION ALL over
    each `*_errors` table referenced by the library, ordered by
    `created_at DESC`. Capped at `limit` (default 50)."""
    if limit < 1 or limit > 1000:
        raise HTTPException(400, "limit must be between 1 and 1000")
    with _conn() as conn:
        lib_rows = conn.execute(
            sql.SQL("SELECT state_schema, state_table FROM {schema}.{table}").format(
                schema=sql.Identifier(LIBRARY_SCHEMA),
                table=sql.Identifier(LIBRARY_TABLE),
            )
        ).fetchall()
        if not lib_rows:
            return []

        union_parts = []
        for lib in lib_rows:
            errors_table = lib["state_table"].replace("_state", "_errors")
            union_parts.append(
                sql.SQL(
                    "SELECT full_name, since, until, error_type, error_message, "
                    "       traceback, created_at, run_id::text AS run_id "
                    "FROM {schema}.{table}"
                ).format(
                    schema=sql.Identifier(lib["state_schema"]),
                    table=sql.Identifier(errors_table),
                )
            )
        union_sql = sql.SQL(" UNION ALL ").join(union_parts)
        full_q = union_sql + sql.SQL(f" ORDER BY created_at DESC LIMIT {int(limit)}")
        try:
            rows = conn.execute(full_q).fetchall()
        except psycopg.errors.UndefinedTable:
            conn.rollback()
            return []
    return [
        {
            "full_name": r["full_name"],
            "since": r["since"].isoformat(),
            "until": r["until"].isoformat(),
            "error_type": r["error_type"],
            "error_message": r["error_message"],
            "traceback": r["traceback"],
            "created_at": r["created_at"].isoformat(),
            "run_id": r["run_id"],
        }
        for r in rows
    ]


# ── static frontend (served at /) ───────────────────────────────────


_FRONTEND_DIST = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "frontend", "dist"
)
_STATIC_FALLBACK = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "static"
)

# Prefer the built Svelte SPA when present; otherwise serve the
# no-Node-required vanilla-JS fallback in `dashboard/static/`.
if os.path.isdir(_FRONTEND_DIST):
    app.mount("/", StaticFiles(directory=_FRONTEND_DIST, html=True), name="frontend")
elif os.path.isdir(_STATIC_FALLBACK):
    app.mount("/", StaticFiles(directory=_STATIC_FALLBACK, html=True), name="static")
else:

    @app.get("/")
    def _no_frontend() -> dict:
        return {
            "message": "No frontend assets found.",
            "api": [
                "/api/library",
                "/api/state/{full_name}",
                "/api/summary",
                "/api/errors?limit=50",
            ],
        }


if __name__ == "__main__":
    import uvicorn

    host = os.environ.get("DASHBOARD_HOST", "127.0.0.1")
    port = int(os.environ.get("DASHBOARD_PORT", "5173"))
    uvicorn.run(app, host=host, port=port)
