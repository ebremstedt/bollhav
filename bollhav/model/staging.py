"""Per-target staging config.

`Staging` is the opt-in object for the staged write path. Set
`Target(staging=Staging(...))` and the `write()` dispatcher routes
through the staging mechanism: sub-batches COPY into a staging
table; one transaction at the end moves staging → target. When
`state=State(...)` is also set, the same transaction flips the
model's state row to `applied` — staged write and state flip are
atomic-or-neither.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class StagingMode(Enum):
    """How the staging table's lifecycle relates to intervals.

    REUSED (default)
        One staging table per pipeline run. Created once on the
        first interval, `TRUNCATE`d at the start of every subsequent
        interval, never dropped by `flush` — survives across the
        whole pipeline. Cheapest on long backfills: 1 `CREATE` +
        N `TRUNCATE`s vs `PER_INTERVAL`'s 2N catalog statements.
        Fits the `Mutations` one-shot pattern via
        `mutations.staging_table_created`.

    PER_INTERVAL
        Fresh staging table per interval — `CREATE` on entry to
        `stage()`, `DROP` inside `flush`'s tx (unless
        `keep_after_flush=True`). Use when you want each interval's
        staging artifact to be inspectable on crash, or when your
        Postgres flavour treats `TRUNCATE` poorly. Pays catalog churn
        in exchange for the per-interval lifecycle.

    Both modes share the same table-name shape (`<prefix><run_id>`),
    so parallel workers on the same model don't collide regardless
    of mode.
    """

    REUSED = "reused"
    PER_INTERVAL = "per_interval"


@dataclass
class Staging:
    """Opt-in staging config for a Target.

    `schema` — override the default staging schema. When unset (the
        normal case), staging tables live in `z_<target.schema.resolved>`
        so bollhav-owned tables stay out of the user's schemas. Override
        if your team has a different convention (e.g. everything in `ops`).
    `table_prefix` — override the default `<target.name>_staging_`
        prefix. Each run still appends its short `run_id` to disambiguate
        concurrent or successive runs.
    `mode` — how the staging table relates to intervals. See
        `StagingMode` for the trade-offs. Default `REUSED` minimises
        catalog churn and is the right choice for ~all use cases.
    `logged` — when False (default), staging tables are created
        UNLOGGED: writes skip the WAL, giving ~2-3x faster COPY
        throughput. A crash will truncate the staging table — fine,
        since the interval reruns from the top anyway. Set True for
        environments that mandate WAL on every write (compliance,
        replication policy).
    `keep_after_flush` — applies to `PER_INTERVAL` mode only. When
        False (default), the staging table is dropped inside the
        flush transaction. Set True to keep it after a successful
        flush — useful for audit (compare what was staged vs what
        landed). Auto orphan-GC is disabled for the model when this
        is True; manual cleanup is the operator's responsibility. In
        `REUSED` mode this flag has no effect — the staging table
        always stays until the next pipeline run's bootstrap GC
        drops it.
    """

    schema: str | None = None
    table_prefix: str | None = None
    mode: StagingMode = StagingMode.REUSED
    logged: bool = False
    keep_after_flush: bool = False


__all__ = ["Staging", "StagingMode"]
