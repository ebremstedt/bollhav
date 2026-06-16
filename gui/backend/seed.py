"""Seed a generic clean -> consume DAG into the bollhav library so the lineage
GUI shows true-to-life, deliberately long 3-part `catalog.schema.table` names
(good for exercising the name presentation toggle).

Managed models live in `AnalyticsLakehouse.curated_clean_entities` /
`AnalyticsLakehouse.consumption_reporting_marts` and read from unmanaged
`UpstreamSourceWarehouse.raw_operational_datamart.*` source tables. A mix of
contract kinds (interval / monolithic / view), per-model run history, and a few
historic errors. All names are invented placeholders. Wipes z_bollhav first.
"""

import os
import uuid
from datetime import datetime, timedelta, timezone

import psycopg
from psycopg import sql

from pathlib import Path

from bollhav.model import (
    Batch,
    Contract,
    Database,
    Freshness,
    FreshnessScope,
    Temporality,
    Model,
    Source,
    SourceApi,
    SourceFile,
    SourceModel,
    State,
    Tags,
    Target,
    TimeChunking,
    UpstreamContract,
    WriteMode,
)
from bollhav.model.intervals import TZInterval  # noqa: E402
from bollhav.postgres import PostgresColumn, PostgresType  # noqa: E402
from bollhav.postgres.state import (  # noqa: E402
    ERRORS_TABLE,
    LIBRARY_SCHEMA,
    PostgresState,
)

DSN = os.environ.get(
    "BOLLHAV_STATE_DSN", "postgresql://postgres:postgres@localhost:5432/postgres"
)
NOW = datetime.now(tz=timezone.utc)

# Demo namespaces — deliberately long, generic placeholders. Managed models
# live in the analytics lakehouse; their unmanaged source tables live in an
# upstream source warehouse.
SRC_CATALOG, SRC_SCHEMA = "UpstreamSourceWarehouse", "raw_operational_datamart"
DW_CATALOG = "AnalyticsLakehouse"
CLEAN, CONSUME = "curated_clean_entities", "consumption_reporting_marts"


def clean(t: str) -> str:
    return f"{DW_CATALOG}.{CLEAN}.{t}"


def consume(t: str) -> str:
    return f"{DW_CATALOG}.{CONSUME}.{t}"


def cha(t: str) -> str:
    return f"{SRC_CATALOG}.{SRC_SCHEMA}.{t}"


def api(t: str) -> str:
    return f"ExternalVendorGateway.rest_ingest.{t}"


def filesrc(t: str) -> str:
    return f"ManualDropZone.csv_inbox.{t}"


# Freshness shorthands for the demo upstream contracts.
def fresh_latest(days: int) -> Freshness:
    return Freshness(within=timedelta(days=days), scope=FreshnessScope.LATEST)


def fresh_all(days: int) -> Freshness:
    return Freshness(within=timedelta(days=days), scope=FreshnessScope.ALL)


def _split3(full: str) -> tuple[str | None, str | None, str]:
    """catalog, schema, table from a dotted name (catalog optional)."""
    parts = full.split(".")
    if len(parts) >= 3:
        return parts[0], parts[1], ".".join(parts[2:])
    if len(parts) == 2:
        return None, parts[0], parts[1]
    return None, None, parts[0]


# (full_name, temporality, [upstream_spec], [source_spec])
#   upstream_spec = (full_name, UpstreamContract, Freshness | None)   — managed, gated
#   source_spec   = (full_name, "model" | "api" | "file")            — unmanaged
#
# The demo deliberately exercises every corner: unmanaged sources of all three
# kinds (model / api / file) feed CLEAN models of every temporality; CONSUME
# models then gate on the clean layer with every contract level (exists / window
# / through / whole) and a mix of freshness bounds (latest / all). Views appear
# in both temporalities. All names are invented placeholders.
SPEC = [
    # ── CLEAN — managed models reading UNMANAGED sources of every kind ──
    (
        clean("CustomerInteractionEngagementEventFact"),
        Temporality.TEMPORAL,
        [],
        [(cha("CustomerInteractionEngagementEventFact"), "model")],
    ),
    (
        clean("SubscriptionBillingLifecycleCycleFact"),
        Temporality.TEMPORAL,
        [],
        [(cha("SubscriptionBillingLifecycleCycleFact"), "model")],
    ),
    (
        clean("OrderFulfilmentShipmentMovementFact"),
        Temporality.TEMPORAL,
        [],
        [(cha("OrderFulfilmentShipmentMovementFact"), "model")],
    ),
    (
        clean("ProductCatalogReferenceHierarchyDimension"),
        Temporality.TIMELESS,
        [],
        [(cha("ProductCatalogReferenceHierarchyDimension"), "model")],
    ),
    (
        clean("GeographicRegionTerritoryHierarchyDimension"),
        Temporality.TIMELESS,
        [],
        [(cha("GeographicRegionTerritoryHierarchyDimension"), "model")],
    ),
    # an API source -> a timeless reference table
    (
        clean("MarketingCampaignChannelBridgeMapping"),
        Temporality.TIMELESS,
        [],
        [(api("MarketingCampaignChannelSpendFeed"), "api")],
    ),
    # a FILE source -> a timeless reference table
    (
        clean("QuarterlySalesTargetReferenceTable"),
        Temporality.TIMELESS,
        [],
        [(filesrc("QuarterlySalesTargetWorkbook"), "file")],
    ),
    # ── CONSUME — managed models gating the clean layer (every contract) ──
    # temporal table: WINDOW (per-window) + WHOLE on a timeless dim, with freshness
    (
        consume("DailyConsolidatedRevenueAggregateFact"),
        Temporality.TEMPORAL,
        [
            (
                clean("CustomerInteractionEngagementEventFact"),
                UpstreamContract.WINDOW,
                fresh_latest(1),
            ),
            (
                clean("SubscriptionBillingLifecycleCycleFact"),
                UpstreamContract.WINDOW,
                None,
            ),
            (
                clean("MarketingCampaignChannelBridgeMapping"),
                UpstreamContract.WHOLE,
                fresh_all(7),
            ),
        ],
        [],
    ),
    # timeless table: WHOLE on dims (one with freshness), EXISTS on a temporal
    # upstream (a windowless consumer depending on it without waiting per-window)
    (
        consume("CustomerThreeSixtyEnrichedProfileDimension"),
        Temporality.TIMELESS,
        [
            (
                clean("ProductCatalogReferenceHierarchyDimension"),
                UpstreamContract.WHOLE,
                None,
            ),
            (
                clean("GeographicRegionTerritoryHierarchyDimension"),
                UpstreamContract.WHOLE,
                fresh_latest(30),
            ),
            (
                clean("CustomerInteractionEngagementEventFact"),
                UpstreamContract.EXISTS,
                None,
            ),
        ],
        [],
    ),
    # temporal VIEW: THROUGH (cumulative prefix) with freshness
    (
        consume("RollingThirtyDayEngagementTrendView"),
        Temporality.TEMPORAL,
        [
            (
                clean("CustomerInteractionEngagementEventFact"),
                UpstreamContract.THROUGH,
                fresh_latest(2),
            ),
        ],
        [],
    ),
    # temporal table: WINDOW on two clean facts — used to demo the orange
    # 'blocked' light (an upstream window hasn't landed yet).
    (
        consume("WeeklyChannelAttributionRollupFact"),
        Temporality.TEMPORAL,
        [
            (
                clean("CustomerInteractionEngagementEventFact"),
                UpstreamContract.WINDOW,
                None,
            ),
            (
                clean("OrderFulfilmentShipmentMovementFact"),
                UpstreamContract.WINDOW,
                fresh_latest(3),
            ),
        ],
        [],
    ),
    # timeless VIEW: WHOLE on a temporal aggregate + EXISTS on a dim
    (
        consume("ExecutivePerformanceOverviewSummaryView"),
        Temporality.TIMELESS,
        [
            (
                consume("DailyConsolidatedRevenueAggregateFact"),
                UpstreamContract.WHOLE,
                fresh_latest(1),
            ),
            (
                consume("CustomerThreeSixtyEnrichedProfileDimension"),
                UpstreamContract.EXISTS,
                None,
            ),
        ],
        [],
    ),
]

# Historic (already-resolved) errors — logged, but the interval later
# succeeded, so they DON'T light the red dot. DailyConsolidatedRevenueAggregateFact
# gets a separate *unresolved* error in _seed_runs to demo the dot.
# (full_name, error_type, message, days_ago)
ERRORS = [
    (
        clean("ProductCatalogReferenceHierarchyDimension"),
        "ConnectionResetError",
        "source connection dropped mid-read",
        11,
    ),
    (
        clean("OrderFulfilmentShipmentMovementFact"),
        "ValueError",
        "null fulfilment key after join",
        8,
    ),
    (
        clean("SubscriptionBillingLifecycleCycleFact"),
        "OperationalError",
        "deadlock detected; interval retried",
        5,
    ),
]


# A generic, deliberately wordy column set so the ⓘ tooltip has real columns
# (with PK / UQ markers and the "+N more" cap) to show.
def _demo_columns():
    return [
        PostgresColumn(
            name="surrogate_warehouse_key_id",
            data_type=PostgresType.BIGINT,
            nullable=False,
            primary_key=True,
        ),
        PostgresColumn(
            name="business_natural_identifier_code",
            data_type=PostgresType.TEXT,
            nullable=False,
            unique=True,
        ),
        PostgresColumn(
            name="effective_valid_from_timestamp",
            data_type=PostgresType.TIMESTAMPTZ,
            nullable=False,
        ),
        PostgresColumn(
            name="effective_valid_until_timestamp", data_type=PostgresType.TIMESTAMPTZ
        ),
        PostgresColumn(
            name="recorded_measure_amount_value", data_type=PostgresType.NUMERIC
        ),
        PostgresColumn(name="source_system_origin_label", data_type=PostgresType.TEXT),
        PostgresColumn(
            name="ingestion_batch_sequence_number", data_type=PostgresType.BIGINT
        ),
        PostgresColumn(
            name="is_current_active_record_flag", data_type=PostgresType.BOOLEAN
        ),
        PostgresColumn(
            name="data_quality_assessment_score", data_type=PostgresType.NUMERIC
        ),
        PostgresColumn(
            name="last_modified_audit_timestamp", data_type=PostgresType.TIMESTAMPTZ
        ),
    ]


def _unmanaged_source(name: str, src_kind: str) -> Source:
    """An ungated (unmanaged) input of the given kind — model / api / file."""
    if src_kind == "api":
        return Source(name, type=SourceApi(base_url="https://vendor.example/api"))
    if src_kind == "file":
        return Source(name, type=SourceFile(path=Path(f"/dropzone/{name}.csv")))
    cat, schema, _ = _split3(name)
    return Source(name, type=SourceModel(catalog=cat, schema=schema))


def _build(full_name, kind, upstream_specs, source_specs):
    cat, schema, table = _split3(full_name)
    layer = "consumption reporting" if schema == CONSUME else "curated clean"
    # A view is any model whose name ends in "View" (demo convention) — it can
    # be temporal or timeless.
    view = table.endswith("View")
    description = f"{kind.value.title()} model in the {layer} layer (demo)."
    # A view has no columns/write_mode; tables carry the demo columns so the
    # tooltip can show them. A bare/database target both keep a catalog, so
    # full_name stays a 3-part catalog.schema.table for every node.
    if view:
        target = Target(
            name=table, schema=schema, catalog=cat, dsn_env_var="TARGET_DSN"
        )
    else:
        target = Target(
            name=table,
            schema=schema,
            catalog=cat,
            database=Database.POSTGRES,
            dsn_env_var="TARGET_DSN",
            write_mode=WriteMode.UPSERT_NO_DELETE,
            columns=_demo_columns(),
        )
    # Managed upstreams are gated SourceModels carrying an explicit contract
    # (and optional freshness); unmanaged inputs are ungated sources of any kind.
    upstream = [
        Source(
            name,
            type=SourceModel(catalog=_split3(name)[0], schema=_split3(name)[1]),
            contract=contract,
            freshness=freshness,
        )
        for name, contract, freshness in upstream_specs
    ] + [_unmanaged_source(name, src_kind) for name, src_kind in source_specs]
    # A view is never batched (it's one CREATE VIEW, not materialised per
    # window); only a temporal TABLE gets batching.
    batched = kind is Temporality.TEMPORAL and not view
    # Tags are AUTO-DERIVED from the model's name / schema / catalog via bollhav's
    # Tags.assemble — including splitting PascalCase names into words
    # (CustomerInteractionEngagementEventFact -> customer, interaction, …, fact).
    # That's the real tagging mechanism; no hand-assigned tags here.
    tagging = Tags(
        unpascal_name_for_tags=True,
        unpascal_schema_for_tags=True,
        unpascal_catalog_for_tags=True,
    )
    return Model(
        target=target,
        temporality=kind,
        view=view,
        tagging=tagging,
        batching=Batch(time=TimeChunking(chunk="@daily")) if batched else None,
        state=State(),
        upstream=upstream,
        description=description,
        contract=(
            Contract(begin=NOW - timedelta(days=60))
            if kind is Temporality.TEMPORAL
            else None
        ),
    )


def _seed_runs(
    model,
    conn,
    *,
    error_latest=False,
    running_latest=False,
    stale_latest=False,
    blocked_latest=False,
):
    """Give the model a little run history via the real state API: 6 daily
    intervals, the rest applied. By default the newest is left pending; with
    `error_latest` the newest interval is recorded as a failure (state row
    -> 'error'), so it shows as an unresolved error (the red dot). A later
    successful rerun would flip it back to applied and clear that. With
    `running_latest` the newest interval is left in 'running' (a run in
    flight), which lights the blue dot. With `stale_latest` the newest interval
    is left `blocked` with a freshness-stale reason, which lights the yellow dot.

    Monolithic/view models get one applied whole-table row."""
    st = PostgresState(model=model, conn=conn)
    st.ensure_tables()
    rid = uuid.uuid4()  # bollhav 3.0: run_id lives on ModelRun, mint one per run
    if model.temporality is Temporality.TEMPORAL:
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
                error_message="case count below sanity threshold",
                traceback_text="Traceback ...\nAssertionError: below threshold",
                update_state=True,
            )
        elif running_latest:
            st.mark_running(run_id=rid, interval=ivs[-1])
        elif stale_latest:
            # newest interval blocked because an upstream is present but too old
            # (freshness gate) — the descriptor carries the `stale` tag the
            # blue 'stale' badge keys on.
            reason = (
                "STATE_002: upstream "
                f"'{clean('CustomerInteractionEngagementEventFact')}' (through, stale)"
            )
            st.insert_intervals(run_id=rid, intervals=((ivs[-1], "blocked", reason),))
        elif blocked_latest:
            # newest interval blocked on completeness — an upstream simply hasn't
            # produced the data yet (no `stale` tag → orange 'blocked' badge).
            reason = (
                "STATE_002: upstream "
                f"'{clean('CustomerInteractionEngagementEventFact')}' (window)"
            )
            st.insert_intervals(run_id=rid, intervals=((ivs[-1], "blocked", reason),))
        # else: newest stays pending
    else:
        st.insert_oneshot(run_id=rid)
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
                error_latest=(name == consume("DailyConsolidatedRevenueAggregateFact")),
                running_latest=(
                    name == clean("CustomerInteractionEngagementEventFact")
                ),
                stale_latest=(name == consume("RollingThirtyDayEngagementTrendView")),
                blocked_latest=(name == consume("WeeklyChannelAttributionRollupFact")),
            )
        _seed_errors(conn)
        conn.commit()
    print(
        f"seeded {len(SPEC)} models (clean -> consume) "
        f"with run history and {len(ERRORS)} historic errors"
    )


if __name__ == "__main__":
    main()
