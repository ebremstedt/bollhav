from __future__ import annotations

import logging

from typing import TYPE_CHECKING

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb

from ._base import _PostgresStateBase
from ._ddl import (
    _ERRORS_DDL,
    _ERRORS_MODEL_INDEX_DDL,
    _ERRORS_TIME_INDEX_DDL,
    _LIBRARY_DDL,
    LIBRARY_SCHEMA,
    LIBRARY_TABLE,
    ERRORS_TABLE,
)
from ._base import LibraryEntry

if TYPE_CHECKING:
    from bollhav.model.model import Model

logger = logging.getLogger(__name__)


class Library(_PostgresStateBase):
    def ensure_library(self) -> None:
        """Create the shared library schema + table if absent. Idempotent.

        The library is shared across many pipelines run by different
        bollhav images — possibly different versions of bollhav at the
        same time. Schema changes therefore have to be **additive**:
        new columns must have a safe default so old images can keep
        inserting without filling them in, and `NOT NULL` may only be
        relaxed, never tightened. Drops are forbidden — they would
        brick concurrent writers. See `_migrate_library_additively`."""
        conn = self._require_conn()
        with conn.transaction():
            conn.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {schema}").format(
                    schema=sql.Identifier(self._library_schema()),
                )
            )
            conn.execute(
                sql.SQL(_LIBRARY_DDL).format(
                    schema=sql.Identifier(self._library_schema()),
                    table=sql.Identifier(LIBRARY_TABLE),
                )
            )
            self._migrate_library_additively()
            # The one shared, central errors table lives in the same schema.
            conn.execute(
                sql.SQL(_ERRORS_DDL).format(
                    schema=sql.Identifier(self._library_schema()),
                    table=sql.Identifier(ERRORS_TABLE),
                )
            )
            conn.execute(
                sql.SQL(_ERRORS_MODEL_INDEX_DDL).format(
                    index=sql.Identifier(f"{ERRORS_TABLE}_full_name_idx"),
                    schema=sql.Identifier(self._library_schema()),
                    table=sql.Identifier(ERRORS_TABLE),
                )
            )
            conn.execute(
                sql.SQL(_ERRORS_TIME_INDEX_DDL).format(
                    index=sql.Identifier(f"{ERRORS_TABLE}_created_at_idx"),
                    schema=sql.Identifier(self._library_schema()),
                    table=sql.Identifier(ERRORS_TABLE),
                )
            )
        logger.debug(
            "library: ensured %s.%s + %s.%s",
            self._library_schema(),
            LIBRARY_TABLE,
            self._library_schema(),
            ERRORS_TABLE,
        )

    def _migrate_library_additively(self) -> None:
        """Apply additive ALTERs to bring an older `library` table up to
        the current shape without breaking concurrent old-image writers.
        Runs inside the caller's transaction. Each step checks the
        information_schema before issuing the ALTER, so the function is
        idempotent and cheap to call on every bootstrap."""
        conn = self._require_conn()
        # `sources` (typed external-input lineage) — additive, with a safe
        # default so older images keep registering without filling it in.
        conn.execute(
            sql.SQL(
                "ALTER TABLE {schema}.{table} "
                "ADD COLUMN IF NOT EXISTS sources JSONB NOT NULL DEFAULT '[]'::jsonb"
            ).format(
                schema=sql.Identifier(self._library_schema()),
                table=sql.Identifier(LIBRARY_TABLE),
            )
        )
        # `metadata` — a flexible JSON bag of model properties (write_mode,
        # tags, description, contract, batching, columns, …). JSON so new
        # properties can be added later without another schema migration.
        # Additive, safe default so older images keep registering.
        conn.execute(
            sql.SQL(
                "ALTER TABLE {schema}.{table} "
                "ADD COLUMN IF NOT EXISTS metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb"
            ).format(
                schema=sql.Identifier(self._library_schema()),
                table=sql.Identifier(LIBRARY_TABLE),
            )
        )
        has_model_type = conn.execute(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s "
            "  AND column_name = 'model_type' LIMIT 1",
            [self._library_schema(), LIBRARY_TABLE],
        ).fetchone()
        if not has_model_type:
            conn.execute(
                sql.SQL(
                    "ALTER TABLE {schema}.{table} "
                    "ADD COLUMN model_type TEXT NOT NULL DEFAULT 'TABLE'"
                ).format(
                    schema=sql.Identifier(self._library_schema()),
                    table=sql.Identifier(LIBRARY_TABLE),
                )
            )
            logger.info(
                "library: migrated %s.%s — added model_type column "
                "(default 'TABLE' so older images can keep writing)",
                self._library_schema(),
                LIBRARY_TABLE,
            )

        has_kind = conn.execute(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s "
            "  AND column_name = 'kind' LIMIT 1",
            [self._library_schema(), LIBRARY_TABLE],
        ).fetchone()
        if not has_kind:
            conn.execute(
                sql.SQL(
                    "ALTER TABLE {schema}.{table} "
                    "ADD COLUMN kind TEXT NOT NULL DEFAULT 'interval'"
                ).format(
                    schema=sql.Identifier(self._library_schema()),
                    table=sql.Identifier(LIBRARY_TABLE),
                )
            )
            logger.info(
                "library: migrated %s.%s — added kind column (default "
                "'interval' so older images keep registering interval models)",
                self._library_schema(),
                LIBRARY_TABLE,
            )

        for col in ("state_schema", "state_table"):
            is_nullable = conn.execute(
                "SELECT is_nullable FROM information_schema.columns "
                "WHERE table_schema = %s AND table_name = %s "
                "  AND column_name = %s LIMIT 1",
                [self._library_schema(), LIBRARY_TABLE, col],
            ).fetchone()
            if is_nullable and is_nullable[0] == "NO":
                conn.execute(
                    sql.SQL(
                        "ALTER TABLE {schema}.{table} ALTER COLUMN {col} DROP NOT NULL"
                    ).format(
                        schema=sql.Identifier(self._library_schema()),
                        table=sql.Identifier(LIBRARY_TABLE),
                        col=sql.Identifier(col),
                    )
                )
                logger.info(
                    "library: migrated %s.%s — relaxed NOT NULL on %s "
                    "(view / library-only rows store NULL here)",
                    self._library_schema(),
                    LIBRARY_TABLE,
                    col,
                )

    @staticmethod
    def _build_metadata(model: "Model") -> dict:
        """A JSON-serialisable bag of the model's properties for the library
        `metadata` column. Stored as JSON so new keys can be added later
        without another schema migration. Everything here is derived from the
        immutable `Model` definition (not run state)."""
        t = model.target

        def _col(c) -> dict:
            dt = getattr(c, "data_type", None)
            return {
                "name": c.name,
                "type": getattr(dt, "value", None) or getattr(dt, "name", None),
                "nullable": getattr(c, "nullable", None),
                "primary_key": getattr(c, "primary_key", False),
                "unique": getattr(c, "unique", False),
                "length": getattr(c, "length", None),
                "precision": getattr(c, "precision", None),
                "scale": getattr(c, "scale", None),
            }

        contract = model.contract
        batching = model.batching
        return {
            "write_mode": getattr(t.write_mode, "value", None),
            "enabled": model.enabled,
            "description": model.description,
            "dsn_env_var": t.dsn_env_var,
            "catalog": t.catalog,
            "schema": t.schema,
            "table": t.name,
            "tags": sorted(model.tags),
            "partitioned_by": t.partitioned_by,
            "staging": t.stage,
            "primary_key": [c.name for c in t.primary_key_columns],
            "unique_columns": [c.name for c in t.unique_columns],
            "columns": [_col(c) for c in t.columns],
            "contract": {
                "begin": contract.begin.isoformat()
                if contract and contract.begin
                else None,
                "end": contract.end.isoformat() if contract and contract.end else None,
            },
            "batching": (
                {
                    "chunk": batching.time.chunk,
                    "window": batching.time.window,
                    "lookback": batching.time.lookback,
                    "size": batching.size,
                }
                if batching is not None
                else None
            ),
        }

    def register_model(self) -> None:
        """Upsert this model's library row. Overwrites every field
        except `full_name` (the PK) and `last_seen` (always `now()`).
        So renaming an upstream, moving the state table, or flipping a
        TABLE to a VIEW all propagate the next time `register_model` runs.

        Every state-tracked model — interval, monolithic, or view — now has
        a state table, so all of them write `state_schema` / `state_table`
        and a `kind`. `kind` drives how an upstream's satisfaction is
        resolved (`is_satisfied`): interval covers a window, monolithic /
        view check the single existence row."""
        model = self.model
        conn = self._require_conn()
        has_state_table = model.state is not None
        state_schema = self._state_schema() if has_state_table else None
        state_table = self._state_table() if has_state_table else None
        model_type = "VIEW" if model.is_view else "TABLE"
        upsert = sql.SQL(
            "INSERT INTO {schema}.{table} "
            "(full_name, upstream, model_type, state_schema, state_table, kind, "
            "sources, metadata, last_seen) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now()) "
            "ON CONFLICT (full_name) DO UPDATE SET "
            "upstream = EXCLUDED.upstream, "
            "model_type = EXCLUDED.model_type, "
            "state_schema = EXCLUDED.state_schema, "
            "state_table = EXCLUDED.state_table, "
            "kind = EXCLUDED.kind, "
            "sources = EXCLUDED.sources, "
            "metadata = EXCLUDED.metadata, "
            "last_seen = EXCLUDED.last_seen"
        ).format(
            schema=sql.Identifier(self._library_schema()),
            table=sql.Identifier(LIBRARY_TABLE),
        )
        with conn.transaction():
            conn.execute(
                upsert,
                [
                    model.target.full_name,
                    model.upstream_names,
                    model_type,
                    state_schema,
                    state_table,
                    model.kind.value,
                    Jsonb(model.source_specs),
                    Jsonb(self._build_metadata(model)),
                ],
            )
        logger.debug(
            "library: registered %s (model_type=%s, kind=%s, upstream=%s, sources=%s)",
            model.target.full_name,
            model_type,
            model.kind.value,
            model.upstream_names,
            model.source_names,
        )

    @staticmethod
    def lookup_model(
        conn: psycopg.Connection,
        full_name: str,
        library_schema: str = LIBRARY_SCHEMA,
    ) -> "LibraryEntry | None":
        """Return the `LibraryEntry` for a model, or None if unregistered.

        Registry-level (not scoped to one model), so it's a static method
        taking the connection — the caller looks up arbitrary upstream
        names, not `self.model`. `library_schema` selects the environment's
        library (the gating caller passes `self._library_schema()`); defaults
        to the prod `z_bollhav`."""
        row = conn.execute(
            sql.SQL(
                "SELECT upstream, model_type, state_schema, state_table, kind, sources "
                "FROM {schema}.{table} WHERE full_name = %s"
            ).format(
                schema=sql.Identifier(library_schema),
                table=sql.Identifier(LIBRARY_TABLE),
            ),
            [full_name],
        ).fetchone()
        if row is None:
            return None
        upstream, model_type, state_schema, state_table, kind, sources = row
        return LibraryEntry(
            upstream=list(upstream),
            model_type=model_type,
            state_schema=state_schema,
            state_table=state_table,
            kind=kind,
            sources=list(sources) if sources else [],
        )
