"""Unit tests for `SourceHardcoded` — inline (in-code) input data.

Mostly pure logic (no DB): the rows/sql validation, the stable `content_hash`,
the `to_dataframe()` materializer for the `rows` form, and the `Source`
integration (kind="hardcoded", never gated, never SQL-addressable, lineage).
The `sql` form's `to_dataframe(conn)` is exercised against a real Postgres when
one is reachable (same opt-in DSN as the contract-kinds tests).
"""

from __future__ import annotations

import os

import polars as pl
import pytest

from bollhav.model import (
    Database,
    Model,
    Source,
    SourceApi,
    SourceHardcoded,
    State,
    Target,
    Temporality,
    UpstreamContract,
)
from bollhav.postgres.columns import PostgresColumn, PostgresType

ROWS = [{"id": 1, "code": "SE"}, {"id": 2, "code": "NO"}]


# ── validation ───────────────────────────────────────────────────────
class TestValidation:
    def test_rows_only_ok(self) -> None:
        assert SourceHardcoded(rows=ROWS).rows == ROWS

    def test_sql_only_ok(self) -> None:
        assert SourceHardcoded(sql="SELECT 1 AS id").sql == "SELECT 1 AS id"

    def test_neither_raises(self) -> None:
        with pytest.raises(ValueError, match="exactly one of"):
            SourceHardcoded()

    def test_both_raises(self) -> None:
        with pytest.raises(ValueError, match="both were set"):
            SourceHardcoded(rows=ROWS, sql="SELECT 1")


# ── content_hash ─────────────────────────────────────────────────────
class TestContentHash:
    def test_stable_across_instances(self) -> None:
        # Not Python's salted hash() — identical content → identical digest,
        # run to run (so it can drive change-detection).
        assert SourceHardcoded(rows=ROWS).content_hash == (
            SourceHardcoded(rows=list(ROWS)).content_hash
        )

    def test_changes_when_rows_change(self) -> None:
        a = SourceHardcoded(rows=ROWS).content_hash
        b = SourceHardcoded(rows=ROWS + [{"id": 3, "code": "DK"}]).content_hash
        assert a != b

    def test_key_order_does_not_matter(self) -> None:
        # Canonical (sorted-key) serialization → row dict key order is irrelevant.
        a = SourceHardcoded(rows=[{"id": 1, "code": "SE"}]).content_hash
        b = SourceHardcoded(rows=[{"code": "SE", "id": 1}]).content_hash
        assert a == b

    def test_rows_and_sql_differ(self) -> None:
        assert (
            SourceHardcoded(rows=ROWS).content_hash
            != SourceHardcoded(sql="SELECT 1").content_hash
        )


# ── to_dataframe (rows form — no connection) ─────────────────────────
class TestToDataframeRows:
    def test_builds_dataframe(self) -> None:
        df = SourceHardcoded(rows=ROWS).to_dataframe()
        assert df.columns == ["id", "code"]
        assert df.to_dicts() == ROWS

    def test_no_connection_needed(self) -> None:
        # The rows form is pure Python — passing a conn is allowed but ignored.
        df = SourceHardcoded(rows=ROWS).to_dataframe(conn=None)
        assert df.shape == (2, 2)

    def test_sql_form_without_conn_raises(self) -> None:
        with pytest.raises(ValueError, match="needs a `conn`"):
            SourceHardcoded(sql="SELECT 1 AS id").to_dataframe()


# ── Source integration ───────────────────────────────────────────────
class TestSourceIntegration:
    def test_kind_label(self) -> None:
        assert Source("ref.countries", type=SourceHardcoded(rows=ROWS)).kind == (
            "hardcoded"
        )

    def test_not_sql_addressable(self) -> None:
        assert Source("x", type=SourceHardcoded(rows=ROWS)).sql_addressable is False

    def test_never_gated(self) -> None:
        s = Source("x", type=SourceHardcoded(rows=ROWS))
        assert s.gated is False
        assert s.contract is None

    def test_contract_rejected(self) -> None:
        # Hardcoded data is always present — it can't carry a contract.
        with pytest.raises(ValueError, match="only a SourceModel can be gated"):
            Source(
                "x",
                type=SourceHardcoded(rows=ROWS),
                contract=UpstreamContract.WHOLE,
            )


# ── lineage ──────────────────────────────────────────────────────────
def _model(*upstream: Source) -> Model:
    return Model(
        target=Target(
            name="countries",
            schema="ref",
            catalog="intel",
            database=Database.POSTGRES,
            columns=[
                PostgresColumn(name="id", data_type=PostgresType.BIGINT, nullable=False)
            ],
        ),
        temporality=Temporality.TIMELESS,
        state=State(),
        upstream=list(upstream),
    )


class TestLineage:
    def test_counts_as_a_known_ungated_source(self) -> None:
        m = _model(Source("ref.countries", type=SourceHardcoded(rows=ROWS)))
        assert m.inputs_known is True
        assert m.declared_inputs == ["ref.countries"]
        assert {"name": "ref.countries", "kind": "hardcoded"} in m.source_specs

    def test_mixes_with_other_sources(self) -> None:
        m = _model(
            Source("ref.countries", type=SourceHardcoded(rows=ROWS)),
            Source("vendor.orders", type=SourceApi()),
        )
        kinds = {s["kind"] for s in m.source_specs}
        assert kinds == {"hardcoded", "api"}

    def test_ref_raises_not_addressable(self) -> None:
        m = _model(Source("ref.countries", type=SourceHardcoded(rows=ROWS)))
        with pytest.raises(ValueError, match="not SQL-addressable"):
            m.ref("ref.countries")


# ── to_dataframe (sql form — needs a real DB) ────────────────────────
DEFAULT_DSN = "postgresql://postgres:postgres@localhost:5432/postgres"


def _can_connect() -> bool:
    import psycopg

    try:
        with psycopg.connect(os.environ.get("E2E_DSN", DEFAULT_DSN), connect_timeout=2):
            return True
    except Exception:
        return False


@pytest.mark.skipif(not _can_connect(), reason="Postgres unavailable (set E2E_DSN)")
class TestToDataframeSql:
    def test_runs_sql_literal(self) -> None:
        import psycopg

        src = SourceHardcoded(
            sql="SELECT * FROM (VALUES (1, 'SE'), (2, 'NO')) AS t(id, code)"
        )
        with psycopg.connect(os.environ.get("E2E_DSN", DEFAULT_DSN)) as conn:
            df = src.to_dataframe(conn)
        assert df.columns == ["id", "code"]
        assert df.sort("id").to_dicts() == ROWS
        assert isinstance(df, pl.DataFrame)
