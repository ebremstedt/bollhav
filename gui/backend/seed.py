"""Seed a realistic raw -> clean -> consume analytics DAG into the bollhav
library so the lineage GUI has something true-to-life to show.

Three schema layers (raw -> clean -> consume), a mix of contract kinds
(interval / view / monolithic) and source kinds (api / file / model),
plus per-model run history and a few historic errors sprinkled in.
Wipes z_bollhav first for a clean graph.
"""

import os
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg
from psycopg import sql

from bollhav.model import (
    Batch,
    IntervalContract,
    Kind,
    MonolithicContract,
    Model,
    Source,
    SourceApi,
    SourceFile,
    SourceModel,
    State,
    Target,
    TimeChunking,
    ViewContract,
)
from bollhav.model.intervals import TZInterval  # noqa: E402
from bollhav.postgres.state import (  # noqa: E402
    ERRORS_TABLE,
    LIBRARY_SCHEMA,
    PostgresState,
)

DSN = os.environ.get(
    "BOLLHAV_STATE_DSN", "postgresql://postgres:postgres@localhost:5432/postgres"
)
NOW = datetime.now(tz=timezone.utc)

# bollhav 3.0 source kinds are api / file / model. The old DATABASE kind is
# now a SourceModel (an external relational table read as a source), and the
# STREAM kind was dropped — we render the kafka source as an api below.
API, DB, FILE, STREAM = "api", "db", "file", "stream"


def _source(name, tag):
    """Build an ungated Source (external input) from a demo tag."""
    if tag in (API, STREAM):
        return Source(name, type=SourceApi())
    if tag == DB:
        return Source(name, type=SourceModel(schema=name.split(".", 1)[0]))
    if tag == FILE:
        return Source(name, type=SourceFile(path=Path(name)))
    raise ValueError(f"unknown source tag {tag!r}")


# (full_name, kind, [upstream full_names], [(source_name, source_kind)])
SPEC = [
    # RAW — ingest from external sources
    ("raw.orders", Kind.INTERVAL, [], [("shopify.orders", API)]),
    ("raw.payments", Kind.INTERVAL, [], [("stripe.charges", API)]),
    ("raw.customers", Kind.INTERVAL, [], [("app_db.customers", DB)]),
    ("raw.events", Kind.INTERVAL, [], [("kafka.clickstream", STREAM)]),
    ("raw.marketing", Kind.INTERVAL, [], [("files/marketing_spend.csv", FILE)]),
    # CLEAN — typed / deduped
    ("clean.orders", Kind.INTERVAL, ["raw.orders"], []),
    ("clean.payments", Kind.INTERVAL, ["raw.payments"], []),
    ("clean.customers", Kind.INTERVAL, ["raw.customers"], []),
    ("clean.events", Kind.INTERVAL, ["raw.events"], []),
    ("clean.marketing", Kind.INTERVAL, ["raw.marketing"], []),
    # CONSUME — facts, dimensions, reporting, metrics
    (
        "consume.fct_orders",
        Kind.INTERVAL,
        ["clean.orders", "clean.payments", "clean.events"],
        [],
    ),
    ("consume.dim_customers", Kind.MONOLITHIC, ["clean.customers"], []),
    (
        "consume.exec_dashboard",
        Kind.VIEW,
        ["consume.fct_orders", "consume.dim_customers", "clean.marketing"],
        [],
    ),
    (
        "consume.kpi_summary",
        Kind.INTERVAL,
        ["consume.exec_dashboard", "consume.fct_orders"],
        [("files/sales_targets.csv", FILE)],
    ),
]

# Historic (already-resolved) errors to sprinkle in — these landed in the
# errors log but the interval later succeeded, so they DON'T light the red
# dot (state is applied). consume.kpi_summary gets a separate *unresolved*
# error in _seed_runs to demo the dot.
# (full_name, error_type, message, days_ago)
ERRORS = [
    ("raw.events", "ConnectionResetError", "kafka consumer dropped mid-batch", 11),
    (
        "raw.marketing",
        "FileNotFoundError",
        "marketing_spend.csv missing for that day",
        8,
    ),
    ("clean.customers", "ValueError", "null personal number after pseudonymisation", 5),
    (
        "consume.fct_orders",
        "OperationalError",
        "deadlock detected; interval retried",
        2,
    ),
]

_KINDS = {name: kind for name, kind, _, _ in SPEC}
_CONTRACT = {
    Kind.INTERVAL: IntervalContract,
    Kind.VIEW: ViewContract,
    Kind.MONOLITHIC: MonolithicContract,
}


def _build(name, kind, upstream_names, sources):
    schema, table = name.split(".", 1)
    # Lineage demo: a bare target (no database/columns) keeps full_name a
    # consistent `schema.table` for every model. A database-backed target in
    # bollhav 3.0 requires a catalog and so gets a 3-part `catalog.schema.table`
    # name — which would desync the table vs. view nodes in the graph.
    target = Target(name=table, schema=schema, dsn_env_var="TARGET_DSN")
    # bollhav 3.0: upstreams and sources share one `upstream` list of Sources.
    # A managed upstream model is a gated SourceModel carrying a contract (its
    # kind picks how satisfaction is resolved); an external input is an ungated
    # Source typed by its provenance (api / file / model).
    upstream = [
        Source(
            u,
            type=SourceModel(schema=u.split(".", 1)[0]),
            contract=_CONTRACT[_KINDS[u]](),
        )
        for u in upstream_names
    ] + [_source(n, tag) for n, tag in sources]
    return Model(
        target=target,
        kind=kind,
        batching=(
            Batch(time=TimeChunking(chunk="@daily")) if kind is Kind.INTERVAL else None
        ),
        state=State(),
        upstream=upstream,
    )


def _seed_runs(model, conn, *, error_latest=False, running_latest=False):
    """Give the model a little run history via the real state API: 6 daily
    intervals, the rest applied. By default the newest is left pending; with
    `error_latest` the newest interval is recorded as a failure (state row
    -> 'error'), so it shows as an unresolved error (the red dot). A later
    successful rerun would flip it back to applied and clear that. With
    `running_latest` the newest interval is left in 'running' (a run in
    flight), which lights the blue dot.

    Monolithic/view models get one applied whole-table row."""
    st = PostgresState(model=model, conn=conn)
    st.ensure_tables()
    rid = uuid.uuid4()  # bollhav 3.0: run_id lives on ModelRun, mint one per run
    if model.kind is Kind.INTERVAL:
        base = (NOW - timedelta(days=6)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        ivs = [
            TZInterval(base + timedelta(days=i), base + timedelta(days=i + 1))
            for i in range(6)
        ]
        st.insert_intervals(run_id=rid, intervals=tuple(ivs))
        for iv in ivs[:-1]:
            st.mark_applied(run_id=rid, interval=iv)
        if error_latest:
            st.record_failure(
                run_id=rid,
                interval=ivs[-1],
                error_type="AssertionError",
                error_message="revenue total below sanity threshold",
                traceback_text="Traceback ...\nAssertionError: below threshold",
                update_state=True,
            )
        elif running_latest:
            st.mark_running(run_id=rid, interval=ivs[-1])
        # else: newest stays pending
    else:
        st.insert_singleton(run_id=rid)
        st.mark_applied(run_id=rid, interval=None)


def _seed_errors(conn):
    insert = sql.SQL(
        "INSERT INTO {schema}.{table} "
        "(full_name, run_id, since, until, error_type, error_message, "
        "traceback, created_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    ).format(schema=sql.Identifier(LIBRARY_SCHEMA), table=sql.Identifier(ERRORS_TABLE))
    with conn.transaction():
        for full_name, etype, msg, days_ago in ERRORS:
            day = (NOW - timedelta(days=days_ago)).replace(
                hour=2, minute=0, second=0, microsecond=0
            )
            conn.execute(
                insert,
                [
                    full_name,
                    str(uuid.uuid4()),
                    day,
                    day + timedelta(days=1),
                    etype,
                    msg,
                    f"Traceback (most recent call last):\n  ...\n{etype}: {msg}",
                    day + timedelta(minutes=3),
                ],
            )


def main() -> None:
    with psycopg.connect(DSN, autocommit=True) as conn:
        # one central bollhav schema holds state + library + errors (no
        # separate z_bollhav_state since 3.0)
        conn.execute("DROP SCHEMA IF EXISTS z_bollhav CASCADE")

    with psycopg.connect(DSN) as conn:
        for name, kind, ups, srcs in SPEC:
            model = _build(name, kind, ups, srcs)
            st = PostgresState(model=model, conn=conn)
            st.ensure_library()
            st.register_model()
            _seed_runs(
                model,
                conn,
                error_latest=(name == "consume.kpi_summary"),
                running_latest=(name == "clean.orders"),
            )
        _seed_errors(conn)
        conn.commit()
    print(
        f"seeded {len(SPEC)} models (raw -> clean -> consume) "
        f"with run history and {len(ERRORS)} historic errors"
    )


if __name__ == "__main__":
    main()
