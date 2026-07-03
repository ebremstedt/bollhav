from enum import Enum


class Materialization(Enum):
    """How a model is realized as a database object — the materialization
    choice, kept separate from the model's *definition* (`Model.query_builder`). A
    `query` is what the model computes; `materialization` is how that result is
    persisted. Defaults to `TABLE` on a `Model`.

    The values are stable strings (used by the state library's `kind` column),
    so `model.materialization.value` is the on-the-wire form.

    * `TABLE` — a materialized table. Its rows are written by the `execute`
                function (today the only way data is produced). The common case.
    * `VIEW`  — a database VIEW created from the model's `query` (its SELECT
                body). Not materialized per-window: one `CREATE OR REPLACE VIEW`,
                a single existence state row, no staging / write mode.

    Kept an enum (not a bool) so the set can grow — e.g. a future
    `MATERIALIZED` would persist a `query` as a table — without an API break.
    """

    TABLE = "table"
    VIEW = "view"
