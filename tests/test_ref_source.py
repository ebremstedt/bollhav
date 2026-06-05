"""ref() / source() — referencing upstreams and external sources in SQL.

`ref()` resolves a declared **managed upstream** to a quoted, schema-suffix-
aware identifier (it moves with the environment). `source()` resolves a
declared **external source** to a quoted LITERAL identifier (no suffix —
external tables live at a fixed location in every environment). The two
namespaces are separate, and an undeclared name raises in both.
"""

import sys
from unittest.mock import MagicMock

import pytest

sys.modules.setdefault("pyodbc", MagicMock())  # bollhav.mssql imports it natively

from bollhav.model import (  # noqa: E402
    Batch,
    Database,
    IntervalChunks,
    IntervalContract,
    Kind,
    Model,
    Source,
    SourceKind,
    State,
    Target,
    TargetSchema,
)
from bollhav.mssql.columns import MssqlColumn, MssqlType  # noqa: E402
from bollhav.postgres.columns import PostgresColumn, PostgresType  # noqa: E402


def _pg_model(*, schema="warehouse_clean", suffix="", upstream=None, sources=None):
    return Model(
        target=Target(
            name="daily_summary",
            schema=TargetSchema(name=schema, suffix=suffix, suffix_appendix=None),
            database=Database.POSTGRES,
            columns=[
                PostgresColumn(name="id", data_type=PostgresType.BIGINT, nullable=False)
            ],
        ),
        batching=Batch(interval=IntervalChunks(expression="@daily")),
        kind=Kind.INTERVAL,
        # upstream is only valid with state; sources need no state.
        state=State() if upstream else None,
        upstream=upstream,
        sources=sources,
    )


def _mssql_model(*, sources=None):
    # MSSQL models aren't state-tracked (state is Postgres-only), so they use
    # `sources`, not managed `upstream` contracts.
    return Model(
        target=Target(
            name="daily_summary",
            schema=TargetSchema(name="warehouse"),
            database=Database.MSSQL,
            columns=[
                MssqlColumn(name="id", data_type=MssqlType.BIGINT, nullable=False)
            ],
        ),
        batching=Batch(interval=IntervalChunks(expression="@daily")),
        kind=Kind.INTERVAL,
        sources=sources,
    )


class TestRef:
    def test_managed_resolves_quoted(self) -> None:
        m = _pg_model(upstream=[IntervalContract("warehouse.orders")])
        assert m.ref("warehouse.orders") == '"warehouse"."orders"'

    def test_applies_schema_suffix(self) -> None:
        m = _pg_model(suffix="pr123", upstream=[IntervalContract("warehouse.orders")])
        assert m.ref("warehouse.orders") == '"warehouse_pr123"."orders"'

    def test_bare_string_upstream_is_refable(self) -> None:
        m = _pg_model(upstream=["warehouse.orders"])
        assert m.ref("warehouse.orders") == '"warehouse"."orders"'

    def test_undeclared_raises(self) -> None:
        m = _pg_model(upstream=[IntervalContract("warehouse.orders")])
        with pytest.raises(ValueError, match="not a declared upstream"):
            m.ref("warehouse.ordrs")

    def test_mssql_uses_bracket_quoting(self) -> None:
        # source() shares the same quoting path as ref(); MSSQL models use
        # sources (no state), so this also covers 3-part catalog.schema.table.
        m = _mssql_model(sources=["Intelligence.warehouse.orders"])
        assert m.source_ref("Intelligence.warehouse.orders") == (
            "[Intelligence].[warehouse].[orders]"
        )


class TestSource:
    def test_resolves_literal_without_suffix(self) -> None:
        # suffix is set on the model, but an external source ignores it.
        m = _pg_model(suffix="pr123", sources=[Source("raw.landing_orders")])
        assert m.source_ref("raw.landing_orders") == '"raw"."landing_orders"'

    def test_bare_string_source_is_resolvable(self) -> None:
        m = _pg_model(sources=["raw.landing_orders"])
        assert m.source_ref("raw.landing_orders") == '"raw"."landing_orders"'

    def test_undeclared_raises(self) -> None:
        m = _pg_model(sources=[Source("raw.landing_orders")])
        with pytest.raises(ValueError, match="not a declared source"):
            m.source_ref("raw.nope")


class TestSourceKind:
    def test_default_kind_is_database_and_addressable(self) -> None:
        assert Source("raw.x").kind is SourceKind.DATABASE
        assert Source("raw.x").sql_addressable is True
        assert Source("raw.x", kind=SourceKind.VIEW).sql_addressable is True

    def test_non_sql_kinds_are_not_addressable(self) -> None:
        for k in (SourceKind.FILE, SourceKind.API, SourceKind.STREAM):
            assert Source("ext", kind=k).sql_addressable is False

    def test_source_ref_works_for_view_kind(self) -> None:
        m = _pg_model(sources=[Source("raw.v_orders", kind=SourceKind.VIEW)])
        assert m.source_ref("raw.v_orders") == '"raw"."v_orders"'

    def test_source_ref_raises_for_non_sql_kind(self) -> None:
        m = _pg_model(sources=[Source("vendor.orders", kind=SourceKind.API)])
        with pytest.raises(ValueError, match="not SQL-addressable"):
            m.source_ref("vendor.orders")


class TestDeclaredInputs:
    def test_unknown_when_nothing_declared(self) -> None:
        m = _pg_model()
        assert m.declared_inputs == []
        assert m.inputs_known is False

    def test_known_with_upstream(self) -> None:
        m = _pg_model(upstream=[IntervalContract("warehouse.orders")])
        assert m.declared_inputs == ["warehouse.orders"]
        assert m.inputs_known is True

    def test_known_with_sources(self) -> None:
        m = _pg_model(sources=[Source("raw.landing"), "raw.other"])
        assert m.declared_inputs == ["raw.landing", "raw.other"]
        assert m.inputs_known is True

    def test_combines_upstream_and_sources(self) -> None:
        m = _pg_model(
            upstream=[IntervalContract("warehouse.orders")],
            sources=[Source("raw.landing", kind=SourceKind.FILE)],
        )
        assert m.declared_inputs == ["warehouse.orders", "raw.landing"]
        assert m.inputs_known is True


class TestLineage:
    def _model(self):
        from bollhav.model import MonolithicContract, ViewContract

        return _pg_model(
            upstream=[
                IntervalContract("warehouse.orders"),
                ViewContract("warehouse.customers"),
                MonolithicContract("warehouse.app_config"),
            ],
            sources=[
                Source("raw.landing", kind=SourceKind.DATABASE),
                Source("vendor.orders", kind=SourceKind.API),
            ],
        )

    def test_lineage_dict_is_typed(self) -> None:
        lin = self._model().lineage()
        assert lin["kind"] == "interval"
        assert lin["inputs_known"] is True
        assert {"name": "warehouse.customers", "kind": "view"} in lin["upstream"]
        assert {"name": "vendor.orders", "kind": "api"} in lin["sources"]

    def test_upstream_specs_bare_string_kind_is_none(self) -> None:
        m = _pg_model(upstream=["warehouse.orders"])
        assert m.upstream_specs == [{"name": "warehouse.orders", "kind": None}]

    def test_lineage_json_roundtrips(self) -> None:
        import json

        m = self._model()
        assert json.loads(m.lineage_json()) == m.lineage()

    def test_lineage_tree_labels_kinds(self) -> None:
        tree = self._model().lineage_tree()
        assert "├─ upstream" in tree
        assert "└─ sources" in tree
        assert "warehouse.orders (interval)" in tree
        assert "warehouse.customers (view)" in tree
        assert "warehouse.app_config (monolithic)" in tree
        assert "vendor.orders (api)" in tree

    def test_lineage_tree_unknown_when_nothing_declared(self) -> None:
        tree = _pg_model().lineage_tree()
        assert "no declared inputs" in tree


class TestSeparateNamespaces:
    def test_ref_cannot_resolve_a_source(self) -> None:
        m = _pg_model(
            upstream=[IntervalContract("a.orders")],
            sources=[Source("raw.landing")],
        )
        with pytest.raises(ValueError, match="not a declared upstream"):
            m.ref("raw.landing")

    def test_source_cannot_resolve_an_upstream(self) -> None:
        m = _pg_model(
            upstream=[IntervalContract("a.orders")],
            sources=[Source("raw.landing")],
        )
        with pytest.raises(ValueError, match="not a declared source"):
            m.source_ref("a.orders")
