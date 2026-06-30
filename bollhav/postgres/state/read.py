from __future__ import annotations

from typing import TYPE_CHECKING

from psycopg import sql

from ._ddl import ERRORS_TABLE, LIBRARY_SCHEMA, LIBRARY_TABLE

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


def _blocked_kinds(conn, state_schema, state_table) -> tuple[bool, bool]:
    """Inspect the model's `blocked` state rows and split them into the two
    badge classes: `(has_blocked, has_stale)`.

      * `has_stale`   — blocked because an upstream is present but too old (its
                        reason carries the `stale` tag from a freshness gate).
      * `has_blocked` — blocked on completeness (an upstream simply hasn't
                        produced the data yet) — i.e. a non-stale block.

    A row tagged `stale` counts only as stale, so the yellow and orange badges
    never both light for the same blocker."""
    if (
        not state_schema
        or not state_table
        or not _table_exists(conn, state_schema, state_table)
    ):
        return (False, False)
    has_stale = (
        conn.execute(
            sql.SQL(
                "SELECT 1 FROM {schema}.{table} "
                "WHERE status = 'blocked' AND blocked_reason LIKE '%%stale%%' LIMIT 1"
            ).format(
                schema=sql.Identifier(state_schema),
                table=sql.Identifier(state_table),
            )
        ).fetchone()
        is not None
    )
    has_blocked = (
        conn.execute(
            sql.SQL(
                "SELECT 1 FROM {schema}.{table} WHERE status = 'blocked' "
                "AND (blocked_reason IS NULL OR blocked_reason NOT LIKE '%%stale%%') "
                "LIMIT 1"
            ).format(
                schema=sql.Identifier(state_schema),
                table=sql.Identifier(state_table),
            )
        ).fetchone()
        is not None
    )
    return (has_blocked, has_stale)


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
    "SELECT full_name, temporality, model_type, upstream, sources, "
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


def list_environments(conn: "psycopg.Connection") -> list[dict]:
    """Every bollhav library schema in the connected database — prod
    (`z_bollhav`) plus any suffixed dev / PR environments (`z_bollhav_<suffix>…`).
    Returns `[{"schema", "label"}]` (prod first); `label` is `"prod"` for the
    base schema, else the suffix part. Only schemas that actually hold a
    `library` table are listed, so the GUI can offer a real env switcher."""
    rows = conn.execute(
        "SELECT s.schema_name FROM information_schema.schemata s "
        "JOIN information_schema.tables t "
        "  ON t.table_schema = s.schema_name AND t.table_name = %s "
        "ORDER BY s.schema_name",
        [LIBRARY_TABLE],
    ).fetchall()
    envs: list[dict] = []
    for (name,) in rows:
        if name == LIBRARY_SCHEMA or name.startswith(LIBRARY_SCHEMA + "_"):
            label = (
                "prod"
                if name == LIBRARY_SCHEMA
                else name[len(LIBRARY_SCHEMA) + 1 :].rstrip("_")
            )
            envs.append({"schema": name, "label": label or name})
    envs.sort(key=lambda e: (e["schema"] != LIBRARY_SCHEMA, e["schema"]))
    return envs


def get_model(
    conn: "psycopg.Connection", full_name: str, schema: str = LIBRARY_SCHEMA
) -> dict | None:
    """One model's library row as a node, or None if unregistered. `schema` is
    the library schema to read — prod by default, or a suffixed env."""
    if not _table_exists(conn, schema, LIBRARY_TABLE):
        return None
    row = conn.execute(
        sql.SQL(_SELECT_NODE + " WHERE full_name = %s").format(
            schema=sql.Identifier(schema),
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
                "SELECT full_name, temporality FROM {schema}.{table} WHERE full_name = ANY(%s)"
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
    conn: "psycopg.Connection",
    full_name: str,
    limit: int = 50,
    schema: str = LIBRARY_SCHEMA,
) -> list[dict]:
    """Recent rows from a model's state table — its run/interval ledger
    (status, window, when it was applied, run id). Newest first. The state
    table is located from the model's library row (in `schema`); returns [] if
    the model or its state table isn't there (e.g. registered but never
    bootstrapped)."""
    node = get_model(conn, full_name, schema=schema)
    if node is None or not node["state_schema"] or not node["state_table"]:
        return []
    st_schema, st_table = node["state_schema"], node["state_table"]
    if not _table_exists(conn, st_schema, st_table):
        return []
    rows = conn.execute(
        sql.SQL(
            "SELECT status, since, until, applied_at, run_id, temporality, blocked_reason "
            "FROM {schema}.{table} "
            "ORDER BY applied_at DESC NULLS LAST, since DESC NULLS LAST LIMIT %s"
        ).format(
            schema=sql.Identifier(st_schema),
            table=sql.Identifier(st_table),
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


def get_recent_runs(
    conn: "psycopg.Connection",
    limit: int = 50,
    schema: str = LIBRARY_SCHEMA,
) -> list[dict]:
    """Recent run/interval rows across ALL models in `schema` — every model's
    state ledger unioned and sorted newest-first, each row carrying its
    `full_name`. The cross-model companion to `get_recent_state`. Empty if the
    library isn't there."""
    if not _table_exists(conn, schema, LIBRARY_TABLE):
        return []
    models = conn.execute(
        sql.SQL(
            "SELECT full_name, state_schema, state_table FROM {schema}.{table} "
            "WHERE state_table IS NOT NULL ORDER BY full_name"
        ).format(
            schema=sql.Identifier(schema),
            table=sql.Identifier(LIBRARY_TABLE),
        )
    ).fetchall()
    runs: list[dict] = []
    for full_name, st_schema, st_table in models:
        if (
            not st_schema
            or not st_table
            or not _table_exists(conn, st_schema, st_table)
        ):
            continue
        rows = conn.execute(
            sql.SQL(
                "SELECT status, since, until, applied_at, run_id, temporality, "
                "blocked_reason FROM {schema}.{table} "
                "ORDER BY applied_at DESC NULLS LAST, since DESC NULLS LAST LIMIT %s"
            ).format(
                schema=sql.Identifier(st_schema),
                table=sql.Identifier(st_table),
            ),
            [limit],
        ).fetchall()
        for status, since, until, applied_at, run_id, kind, blocked_reason in rows:
            runs.append(
                {
                    "full_name": full_name,
                    "status": status,
                    "since": _iso(since),
                    "until": _iso(until),
                    "applied_at": _iso(applied_at),
                    "run_id": str(run_id) if run_id is not None else None,
                    "kind": kind,
                    "blocked_reason": blocked_reason,
                }
            )
    # newest first across all models; rows with no applied_at sink to the end
    runs.sort(key=lambda r: (r["applied_at"] or "", r["since"] or ""), reverse=True)
    return runs[:limit]


def get_gaps_grouped(
    conn: "psycopg.Connection",
    schema: str = LIBRARY_SCHEMA,
) -> list[dict]:
    """Per stateful model, the **backfill gaps** between its contract and its
    state table — the `[since, until)` spans of the contract window that aren't
    yet `applied`. The set-math mirrors `StateTable.uncovered_gaps`
    (`multirange([begin, horizon)) − range_agg(applied)`), but read straight from
    the library so the GUI needs no model objects.

    The horizon is the contract `begin` → contract `end`, falling back to the
    latest `until` materialized in state (an open contract's forward edge), then
    `now()` when nothing's been materialized yet. Each model returns:

        {full_name, has_contract, begin, end,
         gaps: [{since, until, seconds}],
         contract_seconds, gap_seconds, covered_seconds, pct_covered,
         status_counts: {applied, pending, blocked, running, error, …}}

    `has_contract` is False — with empty `gaps` — for a model with no contract
    `begin` (an open-start contract, so gaps-vs-contract is undefined), no state
    table, or a timeless temporality. Such models are still listed so the GUI can
    show them as "no declared bounds". Ordered by name."""
    if not _table_exists(conn, schema, LIBRARY_TABLE):
        return []
    models = conn.execute(
        sql.SQL(
            "SELECT full_name, temporality, state_schema, state_table, metadata "
            "FROM {schema}.{table} ORDER BY full_name"
        ).format(
            schema=sql.Identifier(schema),
            table=sql.Identifier(LIBRARY_TABLE),
        )
    ).fetchall()

    out: list[dict] = []
    for full_name, temporality, st_schema, st_table, metadata in models:
        contract = (metadata or {}).get("contract") or {}
        begin, end = contract.get("begin"), contract.get("end")
        tags = (metadata or {}).get("tags", []) or []
        has_table = bool(
            st_schema and st_table and _table_exists(conn, st_schema, st_table)
        )
        tbl = (
            sql.SQL("{schema}.{table}").format(
                schema=sql.Identifier(st_schema), table=sql.Identifier(st_table)
            )
            if has_table
            else None
        )
        status_counts = (
            {
                s: n
                for s, n in conn.execute(
                    sql.SQL(
                        "SELECT status, count(*) FROM {tbl} GROUP BY status"
                    ).format(tbl=tbl)
                ).fetchall()
            }
            if has_table
            else {}
        )

        # Timeless (oneshot / whole-table) model: no time axis, so backfill is
        # all-or-nothing — its single whole-table row is either `applied`
        # (`applied=True`, the GUI paints it fully "loaded") or not yet
        # (`applied=False`, fully "not loaded"). Never a partial gap. Flagged
        # `timeless` so the GUI can colour it distinctly from a temporal bar.
        if temporality != "temporal":
            applied = status_counts.get("applied", 0) > 0 if has_table else None
            out.append(
                {
                    "full_name": full_name,
                    "has_contract": False,
                    "timeless": True,
                    "applied": applied,
                    "begin": None,
                    "end": None,
                    "gaps": [],
                    "contract_seconds": 0,
                    "gap_seconds": 0,
                    "covered_seconds": 0,
                    "pct_covered": None
                    if applied is None
                    else (100.0 if applied else 0.0),
                    "tags": tags,
                    "status_counts": status_counts,
                }
            )
            continue

        # Temporal model with no declared lower bound (open start) or no state
        # table — gaps-vs-contract is undefined; list it as "no declared bounds".
        if not begin or not has_table:
            out.append(
                {
                    "full_name": full_name,
                    "has_contract": False,
                    "timeless": False,
                    "applied": None,
                    "begin": begin,
                    "end": end,
                    "gaps": [],
                    "contract_seconds": 0,
                    "gap_seconds": 0,
                    "covered_seconds": 0,
                    "pct_covered": None,
                    "tags": tags,
                    "status_counts": status_counts,
                }
            )
            continue
        # The horizon: contract begin → contract end, else the state's forward
        # edge (max until), else now() when nothing's materialized.
        b, e = conn.execute(
            sql.SQL(
                "SELECT %(b)s::timestamptz, COALESCE("
                "%(e)s::timestamptz, "
                "(SELECT max(until) FROM {tbl} WHERE until IS NOT NULL), now())"
            ).format(tbl=tbl),
            {"b": begin, "e": end},
        ).fetchone()
        # multirange([b, e)) − range_agg(applied) = the maximal uncovered spans.
        gap_rows = conn.execute(
            sql.SQL(
                "SELECT lower(g), upper(g) FROM unnest("
                "tstzmultirange(tstzrange(%(b)s::timestamptz, %(e)s::timestamptz, '[)')) - ("
                "SELECT COALESCE("
                "range_agg(tstzrange(since, until, '[)')), '{{}}'::tstzmultirange"
                ") FROM {tbl} WHERE status = 'applied' AND since IS NOT NULL"
                ")) AS g WHERE upper(g) > lower(g) ORDER BY lower(g)"
            ).format(tbl=tbl),
            {"b": b, "e": e},
        ).fetchall()
        gaps = [
            {
                "since": _iso(gs),
                "until": _iso(gu),
                "seconds": (gu - gs).total_seconds(),
            }
            for gs, gu in gap_rows
        ]
        contract_seconds = (e - b).total_seconds() if e > b else 0
        gap_seconds = sum(g["seconds"] for g in gaps)
        covered_seconds = max(contract_seconds - gap_seconds, 0)
        out.append(
            {
                "full_name": full_name,
                "has_contract": True,
                "timeless": False,
                "applied": None,
                "begin": _iso(b),
                "end": _iso(e),
                "gaps": gaps,
                "contract_seconds": contract_seconds,
                "gap_seconds": gap_seconds,
                "covered_seconds": covered_seconds,
                "pct_covered": (
                    round(covered_seconds / contract_seconds * 100, 1)
                    if contract_seconds
                    else None
                ),
                "tags": tags,
                "status_counts": status_counts,
            }
        )
    return out


def get_runs_grouped(
    conn: "psycopg.Connection",
    limit: int = 40,
    schema: str = LIBRARY_SCHEMA,
) -> list[dict]:
    """Per-model run history for the grid view — every stateful model (ordered
    by name) with its most recent `limit` run rows (newest first). Unlike
    `get_recent_runs` it groups by model and is NOT globally capped, so each
    model keeps its own row of cells."""
    if not _table_exists(conn, schema, LIBRARY_TABLE):
        return []
    models = conn.execute(
        sql.SQL(
            "SELECT full_name, state_schema, state_table FROM {schema}.{table} "
            "WHERE state_table IS NOT NULL ORDER BY full_name"
        ).format(
            schema=sql.Identifier(schema),
            table=sql.Identifier(LIBRARY_TABLE),
        )
    ).fetchall()
    out = []
    for full_name, st_schema, st_table in models:
        runs: list[dict] = []
        if st_schema and st_table and _table_exists(conn, st_schema, st_table):
            rows = conn.execute(
                sql.SQL(
                    "SELECT status, since, until, applied_at, run_id, temporality, "
                    "blocked_reason FROM {schema}.{table} "
                    "ORDER BY applied_at DESC NULLS LAST, since DESC NULLS LAST LIMIT %s"
                ).format(
                    schema=sql.Identifier(st_schema),
                    table=sql.Identifier(st_table),
                ),
                [limit],
            ).fetchall()
            runs = [
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
        out.append({"full_name": full_name, "runs": runs})
    return out


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


def get_graph(conn: "psycopg.Connection", schema: str = LIBRARY_SCHEMA) -> dict:
    """The whole cross-pipeline graph: `{"nodes": [...], "edges": [...]}`.

    Reads the library in `schema` (prod by default, or a suffixed env). Nodes
    are models (`type="model"`, typed by `kind`) plus external source boundary
    nodes (`type="external"`, typed by source kind). Edges point from input to
    consumer: `relation` is `upstream` (managed) or `source` (external)."""
    if not _table_exists(conn, schema, LIBRARY_TABLE):
        return {"nodes": [], "edges": []}
    rows = conn.execute(
        sql.SQL(
            "SELECT full_name, temporality, model_type, upstream, sources, "
            "state_schema, state_table, last_seen, metadata FROM {schema}.{table} "
            "ORDER BY full_name"
        ).format(
            schema=sql.Identifier(schema),
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
        metadata,
    ) in rows:
        # Per-upstream contract details (level + freshness), keyed by name, so
        # each upstream edge can carry the gating policy it represents.
        contracts = {
            u["name"]: u for u in (metadata or {}).get("upstreams", []) if u.get("name")
        }
        has_blocked, has_stale = _blocked_kinds(conn, st_schema, st_table)
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
            "tags": (metadata or {}).get("tags", []) or [],
            "last_seen": last_seen.isoformat() if last_seen is not None else None,
            # current (unresolved) failure: a state row still in 'error'.
            # A successful rerun flips that row to 'applied', clearing this.
            "has_error": _has_status(conn, st_schema, st_table, "error"),
            # a run is in flight right now: a state row still in 'running'.
            "has_running": _has_status(conn, st_schema, st_table, "running"),
            # blocked on completeness (an upstream hasn't produced data yet).
            "has_blocked": has_blocked,
            # blocked because an upstream is present but too old (freshness gate).
            "has_stale": has_stale,
        }
        for up in upstream:
            c = contracts.get(up, {})
            edges.append(
                {
                    "from": up,
                    "to": full_name,
                    "relation": "upstream",
                    # the gating policy this edge enforces (None on older rows
                    # written before contracts were persisted in metadata).
                    "contract": c.get("contract"),
                    "freshness": c.get("freshness"),
                }
            )
        for src in sources or []:
            name, src_kind = src["name"], src.get("kind")
            nodes.setdefault(name, {"name": name, "type": "external", "kind": src_kind})
            edges.append(
                {"from": name, "to": full_name, "relation": "source", "kind": src_kind}
            )
    return {"nodes": list(nodes.values()), "edges": edges}


def match_tags(
    conn: "psycopg.Connection", expression: str, schema: str = LIBRARY_SCHEMA
) -> list[dict]:
    """Registered models whose tags satisfy a bollhav tag `expression` — the same
    `[group] & | not:` syntax used to select models for a run (see
    `bollhav.model.tagexpr`), reusing that parser so the GUI filter and the run
    selector behave identically. Raises `ValueError` on a malformed expression.

    Returns `[{"name", "tags"}]` where `tags` is the model's OWN tags that the
    expression positively referenced (the intersection of its real, server-
    derived tag set with the positive candidates in the expression) — so the GUI
    can highlight exactly the matching tags without re-deriving anything client-
    side. Negated (`not:`) tags are excluded (they assert absence)."""
    from bollhav.model.tagexpr import parse_expression, tags_match

    # Accept a bare tag ("clean") or a simple un-bracketed expression
    # ("(raw|clean)&orbit") as well as the full `[group]` syntax — wrap it in
    # one group when the caller didn't bracket anything.
    raw = expression.strip()
    if "[" not in raw:
        raw = f"[{raw}]"
    # Case-INSENSITIVE for the GUI: lowercase the expression and every model's
    # tags before matching, so `[Lakehouse]` matches the `lakehouse` tag (auto-
    # derived tags are lowercased, but the names users read are PascalCase).
    parsed = parse_expression(raw.lower())  # ValueError on bad syntax
    # the positive tag candidates the expression references (for highlighting)
    positive: set[str] = set()
    for group in parsed:
        if group.negate:
            continue
        for tm in group.tags:
            if not tm.negate:
                positive.update(c.strip() for c in tm.candidates)

    if not _table_exists(conn, schema, LIBRARY_TABLE):
        return []
    rows = conn.execute(
        sql.SQL("SELECT full_name, metadata FROM {schema}.{table}").format(
            schema=sql.Identifier(schema),
            table=sql.Identifier(LIBRARY_TABLE),
        )
    ).fetchall()
    out: list[dict] = []
    for full_name, metadata in rows:
        tags = {str(t).lower() for t in ((metadata or {}).get("tags", []) or [])}
        if tags_match(tags, parsed):
            out.append({"name": full_name, "tags": sorted(tags & positive)})
    return out


def get_model_metadata(
    conn: "psycopg.Connection", full_name: str, schema: str = LIBRARY_SCHEMA
) -> dict | None:
    """The model's stored property bag (`library.metadata`) — write_mode,
    tags, description, contract, batching, columns, … — or `None` when the model
    isn't registered. Read from `schema` (prod by default, or a suffixed env).
    The bag is `{}` for rows written by a bollhav old enough to predate the
    `metadata` column (re-running that pipeline backfills it)."""
    if not _table_exists(conn, schema, LIBRARY_TABLE):
        return None
    row = conn.execute(
        sql.SQL("SELECT metadata FROM {schema}.{table} WHERE full_name = %s").format(
            schema=sql.Identifier(schema),
            table=sql.Identifier(LIBRARY_TABLE),
        ),
        [full_name],
    ).fetchone()
    return row[0] if row is not None else None


def get_errors(
    conn: "psycopg.Connection",
    full_name: str | None = None,
    limit: int = 100,
    schema: str = LIBRARY_SCHEMA,
) -> list[dict]:
    """Recent rows from the errors table in `schema` (prod `z_bollhav` by
    default, or a suffixed env), newest first. Optionally filtered to one model
    by `full_name`. Empty if the errors table doesn't exist yet."""
    if not _table_exists(conn, schema, ERRORS_TABLE):
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
            schema=sql.Identifier(schema),
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
    "get_recent_runs",
    "get_runs_grouped",
    "get_downstreams",
    "get_graph",
    "get_errors",
]
