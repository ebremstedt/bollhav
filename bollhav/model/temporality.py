from enum import Enum


class Temporality(Enum):
    """A model's kind — whether its unit of work has a time axis. Defaults to
    `TEMPORAL` (the common case) on a `Model`; it's the single source of truth
    that the state layer and upstream contracts key on.

    The values are the strings the state backend / library use (`kind`
    column), so `model.temporality.value` is the on-the-wire form. A downstream's
    `UpstreamContract` level (WINDOW / THROUGH / …) is resolved against this
    kind at check time.

    * `TEMPORAL` — has a time axis (`contract` begin/end). Batching is
                   optional: batched → one state row per `(since, until)`
                   window; unbatched → the whole `[begin, end]` range loaded in
                   one run. A downstream may gate it per-window (WINDOW /
                   THROUGH) or wholesale (WHOLE / EXISTS).
    * `TIMELESS` — no time axis; the unit of work is the whole thing, one
                   existence / whole-table state row. Must not have `batching`,
                   and its `contract` carries no `begin`/`end`. A downstream can
                   only gate it WHOLE (loaded) or EXISTS (registered) — there is
                   no window to match, so WINDOW / THROUGH are rejected.

    Materialization (a SQL view vs a materialized table) is orthogonal to the
    time axis — see `Model(view=...)` / `Model.is_view`.
    """

    TEMPORAL = "temporal"
    TIMELESS = "timeless"
