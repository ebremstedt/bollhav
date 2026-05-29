"""Per-target staging config.

`Staging` is the opt-in object for the staged write path. Set
`Target(staging=Staging(...))` and the `write()` dispatcher routes
through the staging mechanism: sub-batches COPY into a per-interval
staging table; one transaction at the end moves staging → target and
flips the model's state row to `applied`.

Staging requires `state=State(...)` on the model — atomicity depends
on the state flip being in the same transaction as the data move.
This is enforced in `Model.__init__`.
"""

from __future__ import annotations

from dataclasses import dataclass


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
    `logged` — when False (default), staging tables are created
        UNLOGGED: writes skip the WAL, giving ~2-3x faster COPY
        throughput. A crash will truncate the staging table — fine,
        since the interval reruns from the top anyway. Set True for
        environments that mandate WAL on every write (compliance,
        replication policy).
    `keep_after_flush` — when False (default), the staging table is
        dropped inside the flush transaction. Set True to keep it after
        a successful flush — useful for audit (compare what was staged
        vs what landed) or downstream inspection. Auto orphan-GC is
        disabled for the model when this is True; manual cleanup is
        the operator's responsibility.
    """

    schema: str | None = None
    table_prefix: str | None = None
    logged: bool = False
    keep_after_flush: bool = False


__all__ = ["Staging"]
