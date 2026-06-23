"""ref() — referencing a model's inputs in SQL.

Every input is a `Source` in `upstream`. `ref(name)` resolves it to a quoted
identifier: **suffix-aware** when the Source is gated (a managed model, carries
a contract — it moves with the environment) and **literal** when it isn't (an
external source at a fixed location). Only `SourceModel` inputs are
SQL-addressable; files / APIs raise. An undeclared name raises.
"""

import sys
from unittest.mock import MagicMock

import pytest

sys.modules.setdefault("pyodbc", MagicMock())  # bollhav.mssql imports it natively

from bollhav.model import (  # noqa: E402
    Batch,
    Database,
    TimeChunking,
    Temporality,
    Model,
    Source,
    SourceApi,
    SourceFile,
    SourceModel,
    State,
    Target,
    UpstreamContract,
)
from bollhav.mssql.columns import MssqlColumn, MssqlType  # noqa: E402
from bollhav.postgres.columns import PostgresColumn, PostgresType  # noqa: E402


def _has_gate(upstream) -> bool:
    return bool(upstream) and any(getattr(s, "gated", False) for s in upstream)


def _pg_model(*, schema="warehouse_clean", suffix="", upstream=None):
    return Model(
        target=Target(
            name="daily_summary",
            schema=schema,
            schema_suffix=suffix,
            schema_suffix_appendix=None,
            catalog="intel",
            database=Database.POSTGRES,
            columns=[
                PostgresColumn(name="id", data_type=PostgresType.BIGINT, nullable=False)
            ],
        ),
        batching=Batch(time=TimeChunking(chunk="@daily")),
        temporality=Temporality.TEMPORAL,
        # gated upstreams need state; ungated sources don't.
        state=State() if _has_gate(upstream) else None,
        upstream=upstream,
    )


def _mssql_model(*, upstream=None):
    # MSSQL models aren't state-tracked (state is Postgres-only), so they use
    # ungated sources, not gated contracts.
    return Model(
        target=Target(
            name="daily_summary",
            schema="warehouse",
            catalog="Intelligence",
            database=Database.MSSQL,
            columns=[
                MssqlColumn(name="id", data_type=MssqlType.BIGINT, nullable=False)
            ],
        ),
        batching=Batch(time=TimeChunking(chunk="@daily")),
        temporality=Temporality.TEMPORAL,
        upstream=upstream,
    )


def _gated(name, contract=None, deactivate_for_dev=False):
    """A gated upstream: a SourceModel carrying a contract."""
    return Source(
        name,
        type=SourceModel(),
        contract=contract or UpstreamContract.ENCAPSULATE,
        deactivate_for_dev=deactivate_for_dev,
    )


def _src(name):
    """An ungated relational source."""
    return Source(name, type=SourceModel())


class TestRefGated:
    def test_managed_resolves_quoted(self) -> None:
        m = _pg_model(upstream=[_gated("warehouse.orders")])
        assert m.ref("warehouse.orders") == '"warehouse"."orders"'

    def test_applies_schema_suffix(self) -> None:
        m = _pg_model(suffix="pr123", upstream=[_gated("warehouse.orders")])
        assert m.ref("warehouse.orders") == '"warehouse_pr123"."orders"'

    def test_deactivate_for_dev_resolves_literal_even_when_gated(self) -> None:
        # A gated upstream normally moves with the env suffix; deactivate_for_dev=True
        # pins it to its canonical (prod) location even in a suffixed run.
        m = _pg_model(
            suffix="pr123",
            upstream=[_gated("warehouse.orders", deactivate_for_dev=True)],
        )
        assert m.ref("warehouse.orders") == '"warehouse"."orders"'

    def test_undeclared_raises(self) -> None:
        m = _pg_model(upstream=[_gated("warehouse.orders")])
        with pytest.raises(ValueError, match="not a declared input"):
            m.ref("warehouse.ordrs")

    def test_mssql_bracket_quoting_drops_catalog(self) -> None:
        # ref() shares the quoting path regardless of gating. A 3-part
        # catalog.schema.table name is accepted, but resolves to schema.table —
        # the catalog is connection-level, not part of the FROM.
        m = _mssql_model(upstream=[_src("Intelligence.warehouse.orders")])
        assert m.ref("Intelligence.warehouse.orders") == "[warehouse].[orders]"


class TestRefUngated:
    def test_resolves_literal_without_suffix(self) -> None:
        # suffix is set on the model, but an ungated (external) source ignores it.
        m = _pg_model(suffix="pr123", upstream=[_src("raw.landing_orders")])
        assert m.ref("raw.landing_orders") == '"raw"."landing_orders"'

    def test_catalog_in_name_is_dropped(self) -> None:
        # A model may be referenced as catalog.schema.table, but ref() resolves
        # to schema.table — the catalog is connection-level, not in the FROM.
        m = _pg_model(upstream=[_src("intel.raw.orders")])
        assert m.ref("intel.raw.orders") == '"raw"."orders"'


class TestRefAddressability:
    def test_addressable_flags(self) -> None:
        assert Source("x", type=SourceModel()).sql_addressable is True
        assert Source("x", type=SourceApi()).sql_addressable is False
        assert Source("x", type=SourceFile(path="x.csv")).sql_addressable is False

    def test_api_source_raises(self) -> None:
        m = _pg_model(upstream=[Source("vendor.orders", type=SourceApi())])
        with pytest.raises(ValueError, match="not SQL-addressable"):
            m.ref("vendor.orders")

    def test_view_kind_is_addressable(self) -> None:
        m = _pg_model(
            upstream=[Source("raw.v_orders", type=SourceModel(query="SELECT 1"))]
        )
        assert m.ref("raw.v_orders") == '"raw"."v_orders"'


class TestContractValidation:
    def test_contract_only_valid_on_source_model(self) -> None:
        with pytest.raises(ValueError, match="only a SourceModel can be gated"):
            Source("x", type=SourceApi(), contract=UpstreamContract.ENCAPSULATE)

    def test_contract_on_model_is_ok(self) -> None:
        s = Source(
            "warehouse.orders", type=SourceModel(), contract=UpstreamContract.ENCAPSULATE
        )
        assert s.gated is True
        assert s.contract.value == "encapsulate"


class TestDeclaredInputs:
    def test_unknown_when_nothing_declared(self) -> None:
        m = _pg_model()
        assert m.declared_inputs == []
        assert m.inputs_known is False

    def test_known_with_gated_upstream(self) -> None:
        m = _pg_model(upstream=[_gated("warehouse.orders")])
        assert m.declared_inputs == ["warehouse.orders"]
        assert m.inputs_known is True
        assert m.upstream_names == ["warehouse.orders"]
        assert m.source_names == []

    def test_known_with_ungated_sources(self) -> None:
        m = _pg_model(upstream=[_src("raw.landing"), _src("raw.other")])
        assert m.declared_inputs == ["raw.landing", "raw.other"]
        assert m.source_names == ["raw.landing", "raw.other"]
        assert m.upstream_names == []

    def test_combines_gated_and_ungated(self) -> None:
        m = _pg_model(
            upstream=[
                _gated("warehouse.orders"),
                Source("raw.landing", type=SourceFile(path="raw.csv")),
            ]
        )
        assert m.declared_inputs == ["warehouse.orders", "raw.landing"]
        assert m.upstream_names == ["warehouse.orders"]
        assert m.source_names == ["raw.landing"]


class TestUnknownProvenance:
    def test_unknown_sentinel_populated_when_no_inputs(self) -> None:
        m = _pg_model()
        assert len(m.upstream) == 1
        sentinel = m.upstream[0]
        assert sentinel.type is None
        assert sentinel.name.startswith("unknown-")
        # …but it isn't counted as a declared input.
        assert m.source_names == []
        assert m.upstream_names == []
        assert m.inputs_known is False

    def test_each_unknown_is_a_distinct_node(self) -> None:
        a = _pg_model().upstream[0].name
        b = _pg_model().upstream[0].name
        assert a != b  # uuid-suffixed for lineage-node uniqueness


class TestLineage:
    def _model(self):
        return _pg_model(
            upstream=[
                Source(
                    "warehouse.orders",
                    type=SourceModel(),
                    contract=UpstreamContract.ENCAPSULATE,
                ),
                Source(
                    "warehouse.customers",
                    type=SourceModel(),
                    contract=UpstreamContract.WHOLE,
                ),
                Source(
                    "warehouse.app_config",
                    type=SourceModel(),
                    contract=UpstreamContract.EXISTS,
                ),
                Source("raw.landing", type=SourceModel()),
                Source("vendor.orders", type=SourceApi()),
            ],
        )

    def test_lineage_dict_is_typed(self) -> None:
        lin = self._model().lineage()
        assert lin["kind"] == "temporal"
        assert lin["inputs_known"] is True
        assert {"name": "warehouse.customers", "kind": "whole"} in lin["upstream"]
        assert {"name": "vendor.orders", "kind": "api"} in lin["sources"]
        assert {"name": "raw.landing", "kind": "model"} in lin["sources"]

    def test_upstream_specs_use_contract_level(self) -> None:
        m = _pg_model(upstream=[_gated("warehouse.orders", UpstreamContract.ENCAPSULATE)])
        assert m.upstream_specs == [{"name": "warehouse.orders", "kind": "encapsulate"}]

    def test_lineage_json_roundtrips(self) -> None:
        import json

        m = self._model()
        assert json.loads(m.lineage_json()) == m.lineage()

    def test_lineage_tree_labels_kinds(self) -> None:
        tree = self._model().lineage_tree()
        assert "├─ upstream" in tree
        assert "└─ sources" in tree
        assert "warehouse.orders (encapsulate)" in tree
        assert "warehouse.customers (whole)" in tree
        assert "warehouse.app_config (exists)" in tree
        assert "vendor.orders (api)" in tree

    def test_lineage_tree_unknown_when_nothing_declared(self) -> None:
        tree = _pg_model().lineage_tree()
        assert "no declared inputs" in tree


class TestSeparation:
    def test_ref_resolves_both_gated_and_ungated(self) -> None:
        m = _pg_model(
            upstream=[_gated("a.orders"), _src("raw.landing")],
        )
        assert m.ref("a.orders") == '"a"."orders"'
        assert m.ref("raw.landing") == '"raw"."landing"'
