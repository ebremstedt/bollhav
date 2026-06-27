from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import TYPE_CHECKING

import psycopg
from psycopg import sql


from ._base import _PostgresStateBase
from ._ddl import LIBRARY_SCHEMA
from .library import Library
from .locks import Locks
from .state_table import StateTable
from .satisfaction import Satisfaction

if TYPE_CHECKING:
    from bollhav.model.model import Model

logger = logging.getLogger(__name__)


# ── errors ──
class DropEnvironmentRefusedError(ValueError):
    """`drop_environment` refuses to run when no model carries a schema suffix —
    it would target prod schemas. Set `SCHEMA_SUFFIX` for an ephemeral
    environment, or drop prod schemas by hand."""

    def __init__(self, library_schema: str) -> None:
        super().__init__(
            "drop_environment refuses to run: no model carries a schema suffix, "
            f"so it would target prod schemas ({library_schema} + unsuffixed "
            "targets). Set SCHEMA_SUFFIX for an ephemeral environment, or drop "
            "prod schemas by hand if you must."
        )


class PostgresState(Library, StateTable, Satisfaction, Locks, _PostgresStateBase):
    """Postgres-backed state store for a single model.

    Construct with the model and the caller-owned state connection
    (opened in `main()`, threaded through the lifecycle hooks). The
    naming helpers (`_state_schema`, `_state_table`) work with the
    connection unset; every DB method requires it.

    Implementation is split by concern across this package: `_base`
    (connection / naming), `state_table` (the per-model state table + interval
    status lifecycle), `satisfaction` (upstream gating), `locks` (advisory
    locks), and `library` (the cross-pipeline model library)."""


def drop_environment(conn: psycopg.Connection, models: Sequence["Model"]) -> None:
    """Tear down an ephemeral suffixed environment with no trace: each model's
    suffixed target schema + its staging schema (`z_<target>`), plus the shared
    per-suffix state/library/errors schema (`z_bollhav_<suffix>`). Drops DATA
    too (the target schemas), so it's a full teardown, not a state reset.

    Refuses unless the models carry a schema suffix — it must be structurally
    impossible to drop prod's `z_bollhav` or an unsuffixed target schema. Models
    without a suffix are skipped; if none have one, it raises rather than no-op
    silently.

    Note: schema names are recomputed from each model's suffix. With a *date*
    `schema_suffix_appendix` the name embeds `now()`, so a later teardown can
    miss the originally-created schema — for that case discover by
    `LIKE 'z_bollhav_<suffix>%'` instead. Plain suffixes (the local-test case)
    are exact."""
    from bollhav.model.target import resolve_schema_name

    suffixed = [m for m in models if m.target.schema_suffix]
    if not suffixed:
        raise DropEnvironmentRefusedError(LIBRARY_SCHEMA)
    target_schemas: set[str] = set()
    state_schemas: set[str] = set()
    for m in suffixed:
        target_schemas.add(m.target.schema_resolved)
        state_schemas.add(
            resolve_schema_name(
                LIBRARY_SCHEMA,
                m.target.schema_suffix,
                m.target.schema_suffix_appendix,
            )
        )
    with conn.transaction():
        for s in sorted(target_schemas):
            for schema in (s, f"z_{s}"):  # target + its staging schema
                conn.execute(
                    sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                        sql.Identifier(schema)
                    )
                )
        for s in sorted(state_schemas):
            conn.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(s))
            )
    logger.info(
        "dropped environment: target schemas %s, state schemas %s",
        sorted(target_schemas),
        sorted(state_schemas),
    )
