"""query_builder — building a view model's SELECT, at run time.

A VIEW model is defined by its `query_builder` (its SELECT body). It can be a
plain `str` (a fixed query) or a **callable** `(model_run, since, until) -> str |
sql.Composable` that builds the body at run time — so the query can vary with the
run's window, per-run filters, and (via `model.ref(...)`) the environment's schema
suffix. `ModelRun.resolve_query()` is what turns either form into runnable SQL,
and the backends' `create_replace_view` turn that into `CREATE [OR REPLACE|ALTER]
VIEW`. These tests cover both return types end to end, on both backends.
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

sys.modules.setdefault("pyodbc", MagicMock())  # bollhav.mssql imports it natively

from psycopg import sql  # noqa: E402

from bollhav.model.database import Database  # noqa: E402
from bollhav.model.materialization import Materialization  # noqa: E402
from bollhav.model.model import Model, ViewWithoutQueryError  # noqa: E402
from bollhav.model.modelrun import ModelRun  # noqa: E402
from bollhav.model.source import Source, SourceModel  # noqa: E402
from bollhav.model.state import State  # noqa: E402
from bollhav.model.target import Target  # noqa: E402
from bollhav.model.temporality import Temporality  # noqa: E402
from bollhav.model.upstream import UpstreamContract  # noqa: E402

SINCE = datetime(2024, 1, 1, tzinfo=timezone.utc)
UNTIL = datetime(2024, 1, 2, tzinfo=timezone.utc)


def _view(
    *,
    query_builder,
    database=None,
    suffix="",
    upstream=None,
    columns=None,
    catalog=None,
):
    """A minimal TIMELESS view model carrying `query_builder`.

    `database` defaults to None (Postgres-style `"x"."y"` quoting, no columns
    required) — pass `Database.MSSQL` with `columns`/`catalog` for the
    bracket-quoting path."""
    return Model(
        target=Target(
            name="v_customers",
            schema="warehouse",
            schema_suffix=suffix,
            schema_suffix_appendix=None,  # no date appendix — keep the suffix exact
            database=database,
            catalog=catalog,
            columns=columns or [],
        ),
        temporality=Temporality.TIMELESS,
        materialization=Materialization.VIEW,
        # a gated upstream needs state; ref() suffixing keys off it being gated.
        state=State() if upstream else None,
        upstream=upstream,
        query_builder=query_builder,
    )


# ── resolve_query: a str passes through ──────────────────────────────────────


class TestStrQueryBuilder:
    def test_view_constructs_with_a_str_body(self):
        m = _view(query_builder="SELECT 1")
        assert m.is_view is True
        assert m.query_builder == "SELECT 1"

    def test_resolve_returns_the_string(self):
        m = _view(query_builder="SELECT id, name FROM src")
        assert ModelRun(model=m).resolve_query() == "SELECT id, name FROM src"

    def test_a_str_ignores_since_until(self):
        # A fixed string body doesn't vary with the window — the args are inert.
        m = _view(query_builder="SELECT 1")
        assert ModelRun(model=m).resolve_query(SINCE, UNTIL) == "SELECT 1"


# ── resolve_query: a callable builds the body at run time ─────────────────────


class TestCallableQueryBuilder:
    def test_callable_body_is_invoked(self):
        m = _view(query_builder=lambda run, since, until: "SELECT 42")
        assert ModelRun(model=m).resolve_query() == "SELECT 42"

    def test_callable_receives_the_run_and_window(self):
        seen = {}

        def qb(run, since, until):
            seen["run"], seen["since"], seen["until"] = run, since, until
            return "SELECT 1"

        run = ModelRun(model=_view(query_builder=qb))
        run.resolve_query(SINCE, UNTIL)
        assert seen["run"] is run  # the ModelRun itself, so ref()/window are reachable
        assert seen["since"] is SINCE
        assert seen["until"] is UNTIL

    def test_body_varies_with_the_window(self):
        # The point of a callable: fold the run's window into the SELECT.
        def qb(run, since, until):
            if since is None:
                return "SELECT * FROM events"
            return f"SELECT * FROM events WHERE ts >= '{since.isoformat()}' AND ts < '{until.isoformat()}'"

        run = ModelRun(model=_view(query_builder=qb))
        assert run.resolve_query() == "SELECT * FROM events"  # windowless (view)
        windowed = run.resolve_query(SINCE, UNTIL)
        assert "ts >= '2024-01-01T00:00:00+00:00'" in windowed
        assert "ts < '2024-01-02T00:00:00+00:00'" in windowed

    def test_body_varies_with_the_schema_suffix(self):
        # ref() of a gated upstream picks up the model's own schema suffix, so
        # the same builder reads warehouse_pr12.orders in a PR env and
        # warehouse.orders in prod — a literal string couldn't move with it.
        upstream = [
            Source(
                "warehouse.orders",
                type=SourceModel(),
                contract=UpstreamContract.ENCAPSULATE,
            )
        ]

        def qb(run, since, until):
            return f"SELECT * FROM {run.model.ref('warehouse.orders')}"

        prod = _view(query_builder=qb, suffix="", upstream=upstream)
        assert (
            ModelRun(model=prod).resolve_query() == 'SELECT * FROM "warehouse"."orders"'
        )

        pr = _view(query_builder=qb, suffix="pr12", upstream=upstream)
        assert (
            ModelRun(model=pr).resolve_query()
            == 'SELECT * FROM "warehouse_pr12"."orders"'
        )


# ── resolve_query: a psycopg Composable (Postgres injection-safe opt-in) ──────


class TestComposableQueryBuilder:
    def test_callable_may_return_a_composable(self):
        m = _view(
            query_builder=lambda run, since, until: sql.SQL("SELECT * FROM {}").format(
                sql.Identifier("orders")
            )
        )
        body = ModelRun(model=m).resolve_query()
        assert isinstance(body, sql.Composable)
        assert body.as_string(None) == 'SELECT * FROM "orders"'


# ── the missing-body guard ───────────────────────────────────────────────────


class TestViewWithoutQueryBuilder:
    def test_view_without_query_builder_raises(self):
        with pytest.raises(ViewWithoutQueryError, match="query_builder"):
            _view(query_builder=None)

    def test_resolve_returns_none_when_unset(self):
        # A non-view (table) model has no builder — resolve is a no-op None.
        m = Model(target=Target(name="orders"), temporality=Temporality.TIMELESS)
        assert ModelRun(model=m).resolve_query() is None


# ── Postgres backend: create_replace_view accepts str AND Composable ──────────


def _pg_conn() -> MagicMock:
    conn = MagicMock()
    conn.transaction.return_value.__enter__ = MagicMock(return_value=None)
    conn.transaction.return_value.__exit__ = MagicMock(return_value=False)
    return conn


def _executed_sql(conn: MagicMock) -> str:
    """The CREATE VIEW statement passed to conn.execute, rendered to text."""
    stmt = conn.execute.call_args.args[0]
    return stmt.as_string(None)


class TestPostgresCreateReplaceView:
    def test_str_body_is_wrapped_and_composed(self):
        from bollhav.postgres.modes import create_replace_view

        conn = _pg_conn()
        m = _view(query_builder="SELECT 1")
        create_replace_view(conn=conn, model=m, body=ModelRun(model=m).resolve_query())
        rendered = _executed_sql(conn)
        assert "CREATE OR REPLACE VIEW" in rendered
        assert '"warehouse"."v_customers"' in rendered
        assert rendered.endswith("AS SELECT 1")

    def test_composable_body_is_composed_directly(self):
        from bollhav.postgres.modes import create_replace_view

        conn = _pg_conn()
        body = sql.SQL("SELECT * FROM {}").format(sql.Identifier("orders"))
        m = _view(query_builder=lambda run, since, until: body)
        create_replace_view(conn=conn, model=m, body=ModelRun(model=m).resolve_query())
        rendered = _executed_sql(conn)
        assert rendered.endswith('AS SELECT * FROM "orders"')

    def test_none_body_raises(self):
        from bollhav.postgres.modes import (
            CreateReplaceViewRequiresQueryError,
            create_replace_view,
        )

        with pytest.raises(CreateReplaceViewRequiresQueryError):
            create_replace_view(
                conn=_pg_conn(), model=_view(query_builder="x"), body=None
            )


# ── MSSQL backend: str only — a Composable is a Postgres-only mistake ─────────


class TestMssqlCreateReplaceView:
    def _mssql_view(self, query_builder):
        from bollhav.mssql.columns import MssqlColumn, MssqlType

        return _view(
            query_builder=query_builder,
            database=Database.MSSQL,
            catalog="cat",
            columns=[MssqlColumn(name="id", data_type=MssqlType.BIGINT)],
        )

    def test_str_body_creates_or_alters_the_view(self):
        from bollhav.mssql.modes import create_replace_view

        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor
        m = self._mssql_view("SELECT 1")
        create_replace_view(conn=conn, model=m, body=ModelRun(model=m).resolve_query())
        issued = str(cursor.execute.call_args.args[0])
        assert "CREATE OR ALTER VIEW" in issued
        assert "[warehouse].[v_customers]" in issued
        assert issued.endswith("AS SELECT 1")

    def test_composable_body_is_rejected(self):
        # pyodbc can't consume psycopg Composables — return a str for MSSQL.
        from bollhav.mssql.modes import create_replace_view

        body = sql.SQL("SELECT 1")
        m = self._mssql_view(lambda run, since, until: body)
        with pytest.raises(TypeError, match="Composable|Postgres-only"):
            create_replace_view(
                conn=MagicMock(), model=m, body=ModelRun(model=m).resolve_query()
            )

    def test_none_body_raises(self):
        from bollhav.mssql.modes import (
            CreateReplaceViewRequiresQueryError,
            create_replace_view,
        )

        with pytest.raises(CreateReplaceViewRequiresQueryError):
            create_replace_view(
                conn=MagicMock(), model=self._mssql_view("x"), body=None
            )
