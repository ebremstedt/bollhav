"""Seed a SECOND, distinct environment into `z_bollhav_demoenv` so the GUI's
environment switcher has prod (`z_bollhav`, see seed.py) vs a dev env to toggle
between. Deliberately uses a COMPLETELY different namespace + model names than
prod, so it's obvious at a glance whether an env is leaking.

Local demo only — registers into the suffixed env (`schema_suffix="demoenv"`,
no week appendix → stable `z_bollhav_demoenv`) and does NOT touch prod's
`z_bollhav`. Run:  python gui/backend/seed_dev.py
"""

from datetime import timedelta

import psycopg

from bollhav.model import Freshness, FreshnessScope, Temporality, UpstreamContract
from bollhav.postgres.state import PostgresState

from seed import DSN, _build, _seed_runs  # reuse the prod seed's builders

SUFFIX = "demoenv"

# A totally separate namespace from prod (AnalyticsLakehouse / UpstreamSourceWarehouse).
DW, CLEAN, CONSUME = "DevSandbox", "raw_experiments", "lab_marts"
SRC_CAT, SRC_SCHEMA = "ExternalFieldStation", "sensor_dropzone"


def clean(t):
    return f"{DW}.{CLEAN}.{t}"


def consume(t):
    return f"{DW}.{CONSUME}.{t}"


def src(t):
    return f"{SRC_CAT}.{SRC_SCHEMA}.{t}"


def _fresh(days):
    return Freshness(within=timedelta(days=days), scope=FreshnessScope.LATEST)


# (full_name, temporality, [upstream (name, contract, freshness)], [source (name, kind)])
DEV_SPEC = [
    # CLEAN — distinct sources of every kind → clean models
    (
        clean("PenguinTelemetryEventFact"),
        Temporality.TEMPORAL,
        [],
        [(src("PenguinTelemetryEventFact"), "model")],
    ),
    (
        clean("GlacierBoundaryReferenceDimension"),
        Temporality.TIMELESS,
        [],
        [(src("GlacierBoundaryFeed"), "api")],
    ),
    (
        clean("SnowfallReadingReferenceTable"),
        Temporality.TIMELESS,
        [],
        [(src("SnowfallReadingWorkbook"), "file")],
    ),
    # CONSUME — gated on the clean layer with varied contracts
    (
        consume("AuroraForecastRollupFact"),
        Temporality.TEMPORAL,
        [
            (clean("PenguinTelemetryEventFact"), UpstreamContract.ENCAPSULATE, _fresh(1)),
            (clean("GlacierBoundaryReferenceDimension"), UpstreamContract.WHOLE, None),
        ],
        [],
    ),
    (
        consume("ExpeditionReadinessOverviewView"),
        Temporality.TIMELESS,
        [
            (consume("AuroraForecastRollupFact"), UpstreamContract.WHOLE, _fresh(1)),
            (clean("SnowfallReadingReferenceTable"), UpstreamContract.EXISTS, None),
        ],
        [],
    ),
]


def main():
    # fresh start for the dev env (never touches prod z_bollhav)
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute(f"DROP SCHEMA IF EXISTS z_bollhav_{SUFFIX} CASCADE")

    with psycopg.connect(DSN) as conn:
        for name, kind, ups, srcs in DEV_SPEC:
            model = _build(name, kind, ups, srcs)
            model.target.schema_suffix = SUFFIX
            model.target.schema_suffix_appendix = None  # stable z_bollhav_demoenv
            st = PostgresState(model=model, conn=conn)
            st.ensure_library()
            st.register_model()
            _seed_runs(model, conn)
        conn.commit()
    print(f"seeded {len(DEV_SPEC)} models into z_bollhav_{SUFFIX} (distinct from prod)")


if __name__ == "__main__":
    main()
