from __future__ import annotations

from dataclasses import dataclass, field

from bollhav.model.messages.error import StagingWriteModeError
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
        normal case), staging tables live in the central bollhav schema
        (`z_bollhav`, or `z_bollhav_<suffix>` under a SCHEMA_SUFFIX run —
        same as state/library/errors), so bollhav-owned tables stay out of
        the user's schemas. Override if your team has a different
        convention (e.g. everything in `ops`).
    `table_prefix` — override the default `<per-model stem>_stg_` prefix
        (a devowelled catalog/schema/name stem + full-name digest, so GC
        scopes to one model in the shared schema). Each run still appends
        its short `run_id` to disambiguate concurrent or successive runs.
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
            raise StagingWriteModeError(self.write_mode)


__all__ = ["Staging"]
