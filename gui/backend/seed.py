"""Seed a generic clean -> consume DAG into the bollhav library so the lineage
GUI shows true-to-life, deliberately long 3-part `catalog.schema.table` names
(good for exercising the name presentation toggle).

Managed models live in `AnalyticsLakehouse.curated_clean_entities` /
`AnalyticsLakehouse.consumption_reporting_marts` and read from unmanaged
`UpstreamSourceWarehouse.raw_operational_datamart.*` source tables. A mix of
contract kinds (interval / oneshot / view), per-model run history, and a few
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
                UpstreamContract.ENCAPSULATE,
                fresh_latest(1),
            ),
            (
                clean("SubscriptionBillingLifecycleCycleFact"),
                UpstreamContract.ENCAPSULATE,
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
                UpstreamContract.ENCAPSULATE,
                None,
            ),
            (
                clean("OrderFulfilmentShipmentMovementFact"),
                UpstreamContract.ENCAPSULATE,
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

# More temporal CLEAN facts — each reads one unmanaged `model` source with no
# managed upstreams. They exist mainly to give the Gaps tab a rich spread of
# DIFFERENT backfill shapes (see PROFILES below).
_EXTRA_TEMPORAL = [
    "InventoryStockLevelSnapshotFact",
    "PaymentTransactionSettlementFact",
    "WebSessionClickstreamEventFact",
    "SupportTicketResolutionEventFact",
    "AdImpressionDeliveryEventFact",
    "LogisticsRouteTelemetryFact",
    "PricingExperimentAssignmentFact",
    "DeviceTelemetryHeartbeatFact",
]
SPEC += [
    (clean(t), Temporality.TEMPORAL, [], [(cha(t), "model")]) for t in _EXTRA_TEMPORAL
]


# Per-temporal-model backfill shape for the demo: how far back the contract
# begins (`days`) and which day-indices are already `applied` (a list of
# half-open [start, end) index ranges; index 0 = the contract's first day,
# index days-1 = the most recent day). Everything not applied is a gap, so this
# table is really "draw me these gaps". The Gaps tab then shows a spread of
# shapes: tiny recent tails, huge leading backlogs, swiss-cheese holes,
# fully-covered, and never-run.
DEFAULT_PROFILE = {"days": 60, "applied": [(0, 54)]}
PROFILES = {
    # existing models — the special `*_latest` newest cell is layered on top
    clean("CustomerInteractionEngagementEventFact"): {"days": 45, "applied": [(0, 44)]},
    clean("SubscriptionBillingLifecycleCycleFact"): {"days": 90, "applied": [(80, 90)]},
    clean("OrderFulfilmentShipmentMovementFact"): {
        "days": 40,
        "applied": [(0, 10), (16, 24), (30, 40)],
    },
    consume("DailyConsolidatedRevenueAggregateFact"): {
        "days": 60,
        "applied": [(0, 59)],
    },
    consume("RollingThirtyDayEngagementTrendView"): {"days": 30, "applied": [(0, 29)]},
    consume("WeeklyChannelAttributionRollupFact"): {"days": 70, "applied": [(20, 55)]},
    # extra models — pure gap-shape variety
    clean("InventoryStockLevelSnapshotFact"): {"days": 120, "applied": [(0, 120)]},
    clean("PaymentTransactionSettlementFact"): {"days": 50, "applied": []},
    clean("WebSessionClickstreamEventFact"): {"days": 75, "applied": [(50, 75)]},
    clean("SupportTicketResolutionEventFact"): {
        "days": 35,
        "applied": [(0, 12), (20, 35)],
    },
    clean("AdImpressionDeliveryEventFact"): {
        "days": 100,
        "applied": [(0, 30), (70, 100)],
    },
    clean("LogisticsRouteTelemetryFact"): {
        "days": 28,
        "applied": [(0, 7), (10, 14), (18, 21), (24, 28)],
    },
    clean("PricingExperimentAssignmentFact"): {"days": 14, "applied": [(0, 7)]},
    clean("DeviceTelemetryHeartbeatFact"): {"days": 120, "applied": [(0, 90)]},
}


def _temporal_window(full_name):
    """The `(begin, days, profile)` for a temporal model — `begin` is aligned to
    midnight so daily intervals tile the contract exactly. Shared by `_build`
    (the `Contract` bound) and `_seed_runs` (the rows) so they always agree."""
    prof = PROFILES.get(full_name, DEFAULT_PROFILE)
    today0 = NOW.replace(hour=0, minute=0, second=0, microsecond=0)
    return today0 - timedelta(days=prof["days"]), prof["days"], prof


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
            Contract(begin=_temporal_window(full_name)[0])
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
    loaded=True,
):
    """Materialize a temporal model's whole contract window as daily state rows
    (one per day, like prefill does), `mark_applied` the days named in its
    PROFILE, and leave the rest `pending` — so the Gaps tab shows exactly the
    profile's holes as backfill gaps. The newest day can instead be flipped to a
    special status to light the lineage badges: `error_latest` records a failure
    (red dot), `running_latest` leaves it 'running' (blue dot), `stale_latest` /
    `blocked_latest` leave it 'blocked' (a freshness-stale reason → yellow dot,
    a completeness reason → orange dot).

    Oneshot/view (timeless) models get one applied whole-table row."""
    st = PostgresState(model=model, conn=conn)
    st.ensure_tables()
    rid = uuid.uuid4()  # bollhav 3.0: run_id lives on ModelRun, mint one per run
    if model.temporality is not Temporality.TEMPORAL:
        # a timeless model is one whole-table row — either loaded (applied) or
        # not yet (left pending), never partial
        st.insert_oneshot(run_id=rid)
        if loaded:
            st.mark_applied(run_id=rid, interval=None)
        return

    begin, days, prof = _temporal_window(model.target.full_name)
    ivs = [
        TZInterval(begin + timedelta(days=i), begin + timedelta(days=i + 1))
        for i in range(days)
    ]
    applied = set()
    for a, b in prof["applied"]:
        applied.update(range(a, b))

    # the newest day carries any special status, so it's never auto-applied
    special = error_latest or running_latest or stale_latest or blocked_latest
    last = days - 1
    if special:
        applied.discard(last)

    # prefill the whole contract as pending, then apply the profile's days
    if ivs:
        st.insert_intervals(run_id=rid, intervals=tuple(ivs))
    for i in sorted(applied):
        st.mark_applied(run_id=rid, interval=ivs[i])

    if special:
        iv = ivs[last]
        if error_latest:
            st.record_failure(
                run_id=rid,
                interval=iv,
                error_type="AssertionError",
                error_message="case count below sanity threshold",
                traceback_text="Traceback ...\nAssertionError: below threshold",
                update_state=True,
            )
        elif running_latest:
            st.mark_running(run_id=rid, interval=iv)
        elif stale_latest:
            # blocked because an upstream is present but too old (freshness gate)
            # — the descriptor carries the `stale` tag the yellow badge keys on.
            reason = (
                "STATE_002: upstream "
                f"'{clean('CustomerInteractionEngagementEventFact')}' (through, stale)"
            )
            st.insert_intervals(run_id=rid, intervals=((iv, "blocked", reason),))
        elif blocked_latest:
            # blocked on completeness — an upstream simply hasn't produced the
            # data yet (no `stale` tag → orange 'blocked' badge).
            reason = (
                "STATE_002: upstream "
                f"'{clean('CustomerInteractionEngagementEventFact')}' (window)"
            )
            st.insert_intervals(run_id=rid, intervals=((iv, "blocked", reason),))


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
                # leave one timeless model un-loaded so the Gaps tab shows the
                # gray "not loaded" whole-table state (the rest load green)
                loaded=(name != clean("QuarterlySalesTargetReferenceTable")),
            )
        _seed_errors(conn)
        conn.commit()
    print(
        f"seeded {len(SPEC)} models (clean -> consume) "
        f"with run history and {len(ERRORS)} historic errors"
    )


if __name__ == "__main__":
    main()
