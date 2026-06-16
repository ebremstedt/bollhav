"""Contract × model-kind satisfaction matrix.

Exercises every `UpstreamContract` level — EXISTS / WINDOW / THROUGH / WHOLE —
against every upstream SHAPE that produces state, by driving
`PostgresState.is_satisfied` directly against hand-built state rows:

  * temporal-batched   — one row per window (here: 3 daily windows)
  * temporal-oneshot   — a single row spanning the whole [begin, end] range
                         (an unbatched temporal model)
  * timeless           — a single NULL-window row (a whole-table load or a view;
                         view-vs-table is irrelevant to satisfaction)

`is_satisfied` is keyed by the downstream's contract level (or, for a bare
upstream, the upstream's own `kind`). The hard error for WINDOW/THROUGH against
a TIMELESS upstream lives one layer up in `is_upstream_satisfied_live`; here we
assert what `is_satisfied` itself computes (a timeless upstream simply fails to
match a window query).
"""

import os
from datetime import datetime, timezone

import psycopg
import pytest
from psycopg import sql

from bollhav.model import (
    Temporality,
    Model,
    Source,
    SourceModel,
    State,
    Target,
    UpstreamContract,
)
from bollhav.model.database import Database
from bollhav.model.intervals import TZInterval
from bollhav.postgres.columns import PostgresColumn, PostgresType
from bollhav.postgres.state import LIBRARY_SCHEMA, LibraryEntry, PostgresState

DEFAULT_DSN = "postgresql://postgres:postgres@localhost:5432/postgres"


def _dsn() -> str:
    return os.environ.get("E2E_DSN", DEFAULT_DSN)


def _can_connect() -> bool:
    try:
        with psycopg.connect(_dsn(), connect_timeout=2):
            return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _can_connect(),
    reason="Postgres unavailable (set E2E_DSN; default postgres@localhost:5432)",
)

SCHEMA = "z_bollhav_contract_kinds_test"


def _d(day: int) -> datetime:
    return datetime(2024, 1, day, tzinfo=timezone.utc)


D1, D2, D3, D4 = _d(1), _d(2), _d(3), _d(4)

# A daily batched upstream covers [D1, D4] as three windows.
WINDOWS = [(D1, D2), (D2, D3), (D3, D4)]
# Downstream windows: one strictly inside the range, one at the range's end.
INSIDE = TZInterval(D2, D3)
AT_END = TZInterval(D3, D4)


@pytest.fixture
def conn():
    with psycopg.connect(_dsn(), autocommit=True) as c:
        c.execute(
            sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(SCHEMA))
        )
        c.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(SCHEMA)))
        try:
            yield c
        finally:
            c.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                    sql.Identifier(SCHEMA)
                )
            )


def _table(conn, rows, name: str = "upstream_state") -> str:
    """Create an upstream state table from `rows` (each `(since, until, status)`)
    and return its name."""
    table = name
    conn.execute(
        sql.SQL(
            "CREATE TABLE {s}.{t} "
            "(since TIMESTAMPTZ, until TIMESTAMPTZ, status TEXT NOT NULL)"
        ).format(s=sql.Identifier(SCHEMA), t=sql.Identifier(table))
    )
    for since, until, status in rows:
        conn.execute(
            sql.SQL(
                "INSERT INTO {s}.{t} (since, until, status) VALUES (%s, %s, %s)"
            ).format(s=sql.Identifier(SCHEMA), t=sql.Identifier(table)),
            [since, until, status],
        )
    return table


def _entry(table: str, temporality: str) -> LibraryEntry:
    return LibraryEntry(
        upstream=[],
        model_type="TABLE",
        state_schema=SCHEMA,
        state_table=table,
        temporality=temporality,
        sources=[],
        metadata={},
    )


def _ok(conn, table, kind, *, contract, interval) -> bool:
    return PostgresState.is_satisfied(
        conn, entry=_entry(table, kind), interval=interval, level=contract
    )


# ── row-sets per upstream shape ──────────────────────────────────────
def _batched(loaded: bool):
    # all applied when loaded; otherwise the last window is still pending.
    return [
        (s, u, "applied" if (loaded or (s, u) != WINDOWS[-1]) else "pending")
        for s, u in WINDOWS
    ]


def _oneshot(loaded: bool):
    return [(D1, D4, "applied" if loaded else "pending")]


def _timeless(loaded: bool):
    return [(None, None, "applied" if loaded else "pending")]


class TestTemporalBatched:
    KIND = "temporal"

    @pytest.mark.parametrize(
        "contract,expected",
        [("exists", True), ("window", True), ("through", True), ("whole", True)],
    )
    def test_fully_loaded(self, conn, contract, expected):
        t = _table(conn, _batched(loaded=True))
        assert _ok(conn, t, self.KIND, contract=contract, interval=INSIDE) is expected

    @pytest.mark.parametrize(
        "contract,expected",
        [
            ("exists", True),  # registered
            ("window", True),  # my window [D2,D3] is applied
            ("through", True),  # gap-free prefix up to D3 is applied
            ("whole", False),  # the last window is still pending
        ],
    )
    def test_last_window_pending(self, conn, contract, expected):
        t = _table(conn, _batched(loaded=False))
        assert _ok(conn, t, self.KIND, contract=contract, interval=INSIDE) is expected


class TestTemporalOneshot:
    """An unbatched temporal upstream: one row spanning [D1, D4]."""

    KIND = "temporal"

    @pytest.mark.parametrize(
        "contract,interval,expected",
        [
            ("exists", INSIDE, True),
            ("whole", INSIDE, True),  # the single range row is applied
            ("window", INSIDE, True),  # [D2,D3] falls inside [D1,D4]
            ("window", AT_END, True),  # so does [D3,D4]
            # THROUGH is a gap-free prefix up to my window's end. The one row
            # ends at D4, so it only counts as a prefix when my window reaches
            # D4 — satisfied at the end, not for a window strictly inside.
            ("through", AT_END, True),
            ("through", INSIDE, False),
        ],
    )
    def test_loaded(self, conn, contract, interval, expected):
        t = _table(conn, _oneshot(loaded=True))
        assert _ok(conn, t, self.KIND, contract=contract, interval=interval) is expected

    @pytest.mark.parametrize(
        "contract,expected",
        [("exists", True), ("whole", False), ("window", False), ("through", False)],
    )
    def test_pending(self, conn, contract, expected):
        t = _table(conn, _oneshot(loaded=False))
        assert _ok(conn, t, self.KIND, contract=contract, interval=INSIDE) is expected


class TestTimeless:
    """A timeless upstream (whole-table load or view): one NULL-window row.

    WINDOW/THROUGH are rejected for a timeless upstream by the live gate; at the
    `is_satisfied` layer they simply never match the NULL-window row (False).
    Only EXISTS / WHOLE are meaningful, plus the bare-string fallback (kind=None
    → the upstream's own `timeless` kind → existence check).
    """

    KIND = "timeless"

    @pytest.mark.parametrize(
        "contract,expected",
        [
            ("exists", True),  # registered
            ("whole", True),  # the one row is applied
            ("window", False),  # no window to match
            ("through", False),  # no window to match
        ],
    )
    def test_loaded(self, conn, contract, expected):
        t = _table(conn, _timeless(loaded=True))
        assert _ok(conn, t, self.KIND, contract=contract, interval=INSIDE) is expected

    def test_bare_upstream_uses_existence(self, conn):
        # A bare (un-leveled) dependency falls back to the upstream's own kind:
        # a timeless upstream is satisfied iff its existence row is applied.
        loaded = _table(conn, _timeless(loaded=True), name="up_loaded")
        assert (
            PostgresState.is_satisfied(
                conn, entry=_entry(loaded, self.KIND), interval=None, level=None
            )
            is True
        )
        pending = _table(conn, _timeless(loaded=False), name="up_pending")
        assert (
            PostgresState.is_satisfied(
                conn, entry=_entry(pending, self.KIND), interval=None, level=None
            )
            is False
        )

    @pytest.mark.parametrize(
        "contract,expected",
        [("exists", True), ("whole", False)],
    )
    def test_pending(self, conn, contract, expected):
        t = _table(conn, _timeless(loaded=False))
        assert _ok(conn, t, self.KIND, contract=contract, interval=INSIDE) is expected


# ── the live gate: WINDOW/THROUGH against a TIMELESS upstream is a hard error ──
SUFFIX = "ck_raise"  # isolates the library to z_bollhav_<suffix>


def _model(name, *, temporality, upstream=None):
    return Model(
        target=Target(
            name=name,
            schema="ck",
            catalog="ckcat",
            schema_suffix=SUFFIX,
            schema_suffix_appendix=None,
            database=Database.POSTGRES,
            dsn_env_var="TARGET_DSN",
            columns=[
                PostgresColumn(name="id", data_type=PostgresType.BIGINT, nullable=False)
            ],
        ),
        temporality=temporality,
        state=State(),
        upstream=upstream or [],
    )


class TestWindowOnTimelessRaises:
    @pytest.fixture
    def env(self):
        env_lib = f"{LIBRARY_SCHEMA}_{SUFFIX}"
        with psycopg.connect(_dsn(), autocommit=True) as c:
            c.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                    sql.Identifier(env_lib)
                )
            )
            try:
                # register a TIMELESS upstream in the isolated env library
                up = PostgresState(_model("up", temporality=Temporality.TIMELESS), c)
                up.ensure_library()
                up.ensure_tables()
                up.register_model()
                yield c
            finally:
                c.execute(
                    sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                        sql.Identifier(env_lib)
                    )
                )

    @pytest.mark.parametrize(
        "level", [UpstreamContract.WINDOW, UpstreamContract.THROUGH]
    )
    def test_raises(self, env, level):
        down = _model(
            "down",
            temporality=Temporality.TEMPORAL,
            upstream=[Source("ckcat.ck.up", type=SourceModel(), contract=level)],
        )
        ds = PostgresState(down, env)
        ds.ensure_library()
        with pytest.raises(ValueError, match="TIMELESS"):
            ds.is_upstream_satisfied_live(TZInterval(D2, D3))

    @pytest.mark.parametrize("level", [UpstreamContract.EXISTS, UpstreamContract.WHOLE])
    def test_whole_and_exists_do_not_raise(self, env, level):
        # The shape-compatible levels resolve normally (EXISTS → satisfied;
        # WHOLE → blocked until the upstream's one row is applied).
        down = _model(
            "down",
            temporality=Temporality.TEMPORAL,
            upstream=[Source("ckcat.ck.up", type=SourceModel(), contract=level)],
        )
        ds = PostgresState(down, env)
        ds.ensure_library()
        check = ds.is_upstream_satisfied_live(TZInterval(D2, D3))
        assert check.satisfied is (level is UpstreamContract.EXISTS)
