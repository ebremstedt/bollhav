from __future__ import annotations

from typing import TYPE_CHECKING

from psycopg import sql

from bollhav.postgres.state import ERRORS_TABLE, LIBRARY_SCHEMA, LIBRARY_TABLE

if TYPE_CHECKING:
    import psycopg


def _table_exists(conn: "psycopg.Connection", schema: str, table: str) -> bool:
    return (
        conn.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = %s AND table_name = %s",
            [schema, table],
        ).fetchone()
        is not None
    )


def _iso(value) -> str | None:
    return value.isoformat() if value is not None else None


def _has_status(conn, state_schema, state_table, status: str) -> bool:
    """True if the model currently has any state row in `status`.

    Drives the live graph badges: 'error' (an unresolved failure — clears
    once that interval reruns to 'applied') and 'running' (a run in flight)."""
    if (
        not state_schema
        or not state_table
        or not _table_exists(conn, state_schema, state_table)
    ):
        return False
    return (
        conn.execute(
            sql.SQL("SELECT 1 FROM {schema}.{table} WHERE status = %s LIMIT 1").format(
                schema=sql.Identifier(state_schema),
                table=sql.Identifier(state_table),
            ),
            [status],
        ).fetchone()
        is not None
    )


def _node(
    full_name, kind, model_type, upstream, sources, state_schema, state_table, last_seen
) -> dict:
    """One library row as a JSON-serializable dict. `upstream` is a list of
    names (the edge's type is the upstream model's own kind — see
    `get_lineage`); `sources` is already typed `[{"name", "kind"}]`."""
    return {
        "full_name": full_name,
        "kind": kind,
        "model_type": model_type,
        "upstream": list(upstream),
        "sources": list(sources) if sources else [],
        "state_schema": state_schema,
        "state_table": state_table,
        "last_seen": _iso(last_seen),
    }


_SELECT_NODE = (
    "SELECT full_name, kind, model_type, upstream, sources, "
    "state_schema, state_table, last_seen FROM {schema}.{table}"
)


def list_models(conn: "psycopg.Connection") -> list[dict]:
    """Every registered model in the library, as nodes (ordered by name).
    Empty list if the library hasn't been created yet."""
    if not _table_exists(conn, LIBRARY_SCHEMA, LIBRARY_TABLE):
        return []
    rows = conn.execute(
        sql.SQL(_SELECT_NODE + " ORDER BY full_name").format(
            schema=sql.Identifier(LIBRARY_SCHEMA),
            table=sql.Identifier(LIBRARY_TABLE),
        )
    ).fetchall()
    return [_node(*r) for r in rows]


def get_model(conn: "psycopg.Connection", full_name: str) -> dict | None:
    """One model's library row as a node, or None if unregistered."""
    if not _table_exists(conn, LIBRARY_SCHEMA, LIBRARY_TABLE):
        return None
    row = conn.execute(
        sql.SQL(_SELECT_NODE + " WHERE full_name = %s").format(
            schema=sql.Identifier(LIBRARY_SCHEMA),
            table=sql.Identifier(LIBRARY_TABLE),
        ),
        [full_name],
    ).fetchone()
    return _node(*row) if row is not None else None


def get_lineage(conn: "psycopg.Connection", full_name: str) -> dict | None:
    """One model's typed inputs, in the SAME shape as `Model.lineage()`:

        {"model", "kind", "upstream": [{"name","kind"}],
         "sources": [{"name","kind"}], "inputs_known"}

    Upstream edges are typed by resolving each upstream name to its own
    registered `kind` (the upstream model's row); an unregistered upstream
    gets `kind=None`. Returns None if `full_name` itself is unregistered."""
    node = get_model(conn, full_name)
    if node is None:
        return None

    upstream_names = node["upstream"]
    kind_by_name: dict[str, str] = {}
    if upstream_names:
        rows = conn.execute(
            sql.SQL(
                "SELECT full_name, kind FROM {schema}.{table} WHERE full_name = ANY(%s)"
            ).format(
                schema=sql.Identifier(LIBRARY_SCHEMA),
                table=sql.Identifier(LIBRARY_TABLE),
            ),
            [upstream_names],
        ).fetchall()
        kind_by_name = {name: kind for name, kind in rows}

    upstream = [{"name": n, "kind": kind_by_name.get(n)} for n in upstream_names]
    sources = node["sources"]
    return {
        "model": node["full_name"],
        "kind": node["kind"],
        "upstream": upstream,
        "sources": sources,
        "inputs_known": bool(upstream or sources),
    }


def get_upstream_tree(
    conn: "psycopg.Connection", full_name: str, *, max_depth: int = 25
) -> dict | None:
    """Recursively expand a model's managed upstreams into a **nested** tree —
    the whole ancestry, not just the direct inputs. Each node carries its
    `kind` and external `sources` (leaf boundary inputs); `upstream` holds the
    expanded parent nodes.

    Diamonds re-expand per path (it's a tree, not a deduped graph). Cycles are
    cut: a node already on the current path, or beyond `max_depth`, is included
    with `truncated: true` and not expanded further. Returns None if
    `full_name` isn't registered."""
    if get_model(conn, full_name) is None:
        return None

    def build(name: str, depth: int, on_path: frozenset[str]) -> dict:
        node = get_model(conn, name)
        if node is None:
            return {"model": name, "registered": False, "upstream": [], "sources": []}
        out = {"model": name, "kind": node["kind"], "sources": node["sources"]}
        if name in on_path or depth >= max_depth:
            out["upstream"] = []
            out["truncated"] = True
            return out
        out["upstream"] = [
            build(up, depth + 1, on_path | {name}) for up in node["upstream"]
        ]
        return out

    return build(full_name, 0, frozenset())


def get_recent_state(
    conn: "psycopg.Connection", full_name: str, limit: int = 50
) -> list[dict]:
    """Recent rows from a model's state table — its run/interval ledger
    (status, window, when it was applied, run id). Newest first. The state
    table is located from the model's library row; returns [] if the model or
    its state table isn't there (e.g. registered but never bootstrapped)."""
    node = get_model(conn, full_name)
    if node is None or not node["state_schema"] or not node["state_table"]:
        return []
    schema, table = node["state_schema"], node["state_table"]
    if not _table_exists(conn, schema, table):
        return []
    rows = conn.execute(
        sql.SQL(
            "SELECT status, since, until, applied_at, run_id, kind, blocked_reason "
            "FROM {schema}.{table} "
            "ORDER BY applied_at DESC NULLS LAST, since DESC NULLS LAST LIMIT %s"
        ).format(
            schema=sql.Identifier(schema),
            table=sql.Identifier(table),
        ),
        [limit],
    ).fetchall()
    return [
        {
            "status": status,
            "since": _iso(since),
            "until": _iso(until),
            "applied_at": _iso(applied_at),
            "run_id": str(run_id) if run_id is not None else None,
            "kind": kind,
            "blocked_reason": blocked_reason,
        }
        for status, since, until, applied_at, run_id, kind, blocked_reason in rows
    ]


def get_downstreams(conn: "psycopg.Connection", full_name: str) -> list[str]:
    """Reverse edges — the names of models that declare `full_name` as a
    managed upstream ('who depends on me'). Ordered by name."""
    if not _table_exists(conn, LIBRARY_SCHEMA, LIBRARY_TABLE):
        return []
    rows = conn.execute(
        sql.SQL(
            "SELECT full_name FROM {schema}.{table} "
            "WHERE %s = ANY(upstream) ORDER BY full_name"
        ).format(
            schema=sql.Identifier(LIBRARY_SCHEMA),
            table=sql.Identifier(LIBRARY_TABLE),
        ),
        [full_name],
    ).fetchall()
    return [r[0] for r in rows]


def get_graph(conn: "psycopg.Connection") -> dict:
    """The whole cross-pipeline graph: `{"nodes": [...], "edges": [...]}`.

    Nodes are models (`type="model"`, typed by `kind`) plus external source
    boundary nodes (`type="external"`, typed by source kind). Edges point
    from input to consumer: `relation` is `upstream` (managed) or `source`
    (external)."""
    if not _table_exists(conn, LIBRARY_SCHEMA, LIBRARY_TABLE):
        return {"nodes": [], "edges": []}
    rows = conn.execute(
        sql.SQL(
            "SELECT full_name, kind, model_type, upstream, sources, "
            "state_schema, state_table, last_seen FROM {schema}.{table} "
            "ORDER BY full_name"
        ).format(
            schema=sql.Identifier(LIBRARY_SCHEMA),
            table=sql.Identifier(LIBRARY_TABLE),
        )
    ).fetchall()

    nodes: dict[str, dict] = {}
    edges: list[dict] = []
    for (
        full_name,
        kind,
        model_type,
        upstream,
        sources,
        st_schema,
        st_table,
        last_seen,
    ) in rows:
        nodes[full_name] = {
            "name": full_name,
            "type": "model",
            "kind": kind,
            "model_type": model_type,
            # lineage inputs (also encoded as edges) — exposed per-node so the
            # GUI can show them in a metadata tooltip without re-deriving from
            # the edge list.
            "upstream": list(upstream),
            "sources": sources or [],
            "last_seen": last_seen.isoformat() if last_seen is not None else None,
            # current (unresolved) failure: a state row still in 'error'.
            # A successful rerun flips that row to 'applied', clearing this.
            "has_error": _has_status(conn, st_schema, st_table, "error"),
            # a run is in flight right now: a state row still in 'running'.
            "has_running": _has_status(conn, st_schema, st_table, "running"),
        }
        for up in upstream:
            edges.append({"from": up, "to": full_name, "relation": "upstream"})
        for src in sources or []:
            name, src_kind = src["name"], src.get("kind")
            nodes.setdefault(name, {"name": name, "type": "external", "kind": src_kind})
            edges.append(
                {"from": name, "to": full_name, "relation": "source", "kind": src_kind}
            )
    return {"nodes": list(nodes.values()), "edges": edges}


def get_model_metadata(conn: "psycopg.Connection", full_name: str) -> dict | None:
    """The model's stored property bag (`library.metadata`) — write_mode,
    tags, description, contract, batching, columns, … — or `None` when the model
    isn't registered. The bag is `{}` for rows written by a bollhav old enough
    to predate the `metadata` column (re-running that pipeline backfills it)."""
    if not _table_exists(conn, LIBRARY_SCHEMA, LIBRARY_TABLE):
        return None
    row = conn.execute(
        sql.SQL("SELECT metadata FROM {schema}.{table} WHERE full_name = %s").format(
            schema=sql.Identifier(LIBRARY_SCHEMA),
            table=sql.Identifier(LIBRARY_TABLE),
        ),
        [full_name],
    ).fetchone()
    return row[0] if row is not None else None


def get_errors(
    conn: "psycopg.Connection", full_name: str | None = None, limit: int = 100
) -> list[dict]:
    """Recent rows from the shared `z_bollhav.errors`, newest first.
    Optionally filtered to one model by `full_name`. Empty if the errors
    table doesn't exist yet."""
    if not _table_exists(conn, LIBRARY_SCHEMA, ERRORS_TABLE):
        return []
    where = sql.SQL("WHERE full_name = %s ") if full_name is not None else sql.SQL("")
    params: list = [full_name] if full_name is not None else []
    params.append(limit)
    rows = conn.execute(
        sql.SQL(
            "SELECT full_name, run_id, since, until, error_type, error_message, "
            "traceback, created_at FROM {schema}.{table} {where}"
            "ORDER BY created_at DESC LIMIT %s"
        ).format(
            schema=sql.Identifier(LIBRARY_SCHEMA),
            table=sql.Identifier(ERRORS_TABLE),
            where=where,
        ),
        params,
    ).fetchall()
    return [
        {
            "full_name": fn,
            "run_id": str(run_id),
            "since": _iso(since),
            "until": _iso(until),
            "error_type": error_type,
            "error_message": error_message,
            "traceback": traceback,
            "created_at": _iso(created_at),
        }
        for fn, run_id, since, until, error_type, error_message, traceback, created_at in rows
    ]


__all__ = [
    "list_models",
    "get_model",
    "get_lineage",
    "get_upstream_tree",
    "get_recent_state",
    "get_downstreams",
    "get_graph",
    "get_errors",
]
