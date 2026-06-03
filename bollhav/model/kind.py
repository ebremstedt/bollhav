from enum import Enum


class Kind(Enum):
    """A model's kind — its unit of work, how many state rows it has, and
    how a downstream contract checks it. Set explicitly on every `Model`
    (there is no default): the kind is the single source of truth that the
    state layer and upstream contracts key on, so it must be unambiguous.

    The values are the strings the state backend / library / contracts use
    (`kind` column, `IntervalContract`/`ViewContract`/`MonolithicContract`),
    so `model.kind.value` is the on-the-wire form.

    * `INTERVAL`   — batched table; unit of work is one `(since, until)`
                     window. One state row per window. Requires `batching`.
    * `MONOLITHIC` — whole-table load; unit of work is the entire table.
                     One whole-table state row. Must not have `batching`.
    * `VIEW`       — a view; unit of work is its existence. One existence
                     state row. No `batching` / no `staging`.
    """

    INTERVAL = "interval"
    MONOLITHIC = "monolithic"
    VIEW = "view"
