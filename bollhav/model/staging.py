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

from bollhav.model.write_modes import WriteMode


@dataclass
class Staging:
    """Neutral, database-agnostic staging config for a Target.

    Backend-specific options live on subclasses next to each backend's
    staging module — see `bollhav.postgres.staging.PostgresStaging`
    (`logged` for UNLOGGED tables) and `bollhav.mssql.staging.MssqlStaging`.
    `Target.staging` accepts any subclass; each backend's staging module
    isinstance-checks for its own fields.

    `schema` — override the default staging schema. When unset (the
        normal case), staging tables live in `z_<target.schema_resolved>`
        so bollhav-owned tables stay out of the user's schemas. Override
        if your team has a different convention (e.g. everything in `ops`).
    `table_prefix` — override the default `<target.name>_staging_`
        prefix. Each run still appends its short `run_id` to disambiguate
        concurrent or successive runs.
    `keep_after_apply` — when False (default), each interval's staging
        table is dropped inside the flush transaction, so staging always
        self-cleans on the write connection. Set True to keep tables
        after a successful flush — useful for audit (compare what was
        staged vs what landed). Auto orphan-GC is disabled for the model
        when this is True; manual cleanup is the operator's
        responsibility.
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
        the flush side.
    """

    schema: str | None = None
    table_prefix: str | None = None
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


__all__ = ["Staging"]
