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

from dataclasses import dataclass, field
from enum import Enum

from bollhav.model.write_modes import WriteMode


class StagingMode(Enum):
    """How the staging table's lifecycle relates to intervals.

    REUSED (default)
        One staging table per pipeline run. Created once on the
        first interval, `TRUNCATE`d at the start of every subsequent
        interval, never dropped by `flush` — survives across the
        whole pipeline. Cheapest on long backfills: 1 `CREATE` +
        N `TRUNCATE`s vs `INTERVAL`'s 2N catalog statements.
        Fits the `Mutations` one-shot pattern via
        `mutations.staging_table_created`.

    INTERVAL
        Fresh staging table per interval — `CREATE` on entry to
        `stage()`, `DROP` inside `flush`'s tx (unless
        `keep_after_apply=True`). Use when you want each interval's
        staging artifact to be inspectable on crash, or when your
        Postgres flavour treats `TRUNCATE` poorly. Pays catalog churn
        in exchange for the per-interval lifecycle.

    Both modes share the same table-name shape (`<prefix><run_id>`),
    so parallel workers on the same model don't collide regardless
    of mode.
    """

    REUSED = "reused"
    INTERVAL = "interval"


@dataclass
class Staging:
    """Neutral, database-agnostic staging config for a Target.

    Backend-specific options live on subclasses next to each backend's
    staging module — see `bollhav.postgres.staging.PostgresStaging`
    (`logged` for UNLOGGED tables) and `bollhav.mssql.staging.MssqlStaging`.
    `Target.staging` accepts any subclass; each backend's staging module
    isinstance-checks for its own fields.

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
    `keep_after_apply` — applies to `INTERVAL` mode only. When
        False (default), the staging table is dropped inside the
        flush transaction. Set True to keep it after a successful
        flush — useful for audit (compare what was staged vs what
        landed). Auto orphan-GC is disabled for the model when this
        is True; manual cleanup is the operator's responsibility. In
        `REUSED` mode this flag has no effect — the staging table
        always stays until the next pipeline run's bootstrap GC
        drops it.
    `write_mode` — how each chunk lands IN the staging table. Default
        `APPEND` bulk-inserts every chunk; pick `UPSERT_NO_DELETE` to
        MERGE chunks into staging by `target.unique_columns`, which
        keeps staging deduped as data arrives. Independent of how
        staging then moves into the target: that's driven by
        `target.write_mode` at flush time.

        The four supported pairings:
          (staging APPEND,  target APPEND)  — raw stream-and-load
          (staging APPEND,  target UPSERT)  — collect raw, MERGE once at end
          (staging UPSERT,  target UPSERT)  — dedup early, MERGE at end
          (staging UPSERT,  target APPEND)  — pre-dedup before append
        `target.write_mode = RECREATE_PARTITION` is also supported on
        the flush side regardless of staging mode.
    """

    schema: str | None = None
    table_prefix: str | None = None
    mode: StagingMode = StagingMode.REUSED
    keep_after_apply: bool = False
    write_mode: WriteMode = field(default=WriteMode.APPEND)

    def __post_init__(self) -> None:
        if self.write_mode not in (WriteMode.APPEND, WriteMode.UPSERT_NO_DELETE):
            raise ValueError(
                f"Staging.write_mode must be WriteMode.APPEND or "
                f"WriteMode.UPSERT_NO_DELETE — got "
                f"{self.write_mode!r}. RECREATE_PARTITION and VIEW are "
                f"target-side concepts that don't apply to chunks landing "
                f"in a staging table."
            )


__all__ = ["Staging", "StagingMode"]
