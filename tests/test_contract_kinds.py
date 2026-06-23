"""Contract × model-kind satisfaction matrix.

Exercises every `UpstreamContract` level — EXISTS / EXACT / ENCAPSULATE /
THROUGH / WHOLE — against every upstream SHAPE that produces state, by driving
`PostgresState.is_satisfied` directly against hand-built state rows:

  * temporal-batched   — one row per window (here: 3 daily windows)
  * temporal-oneshot   — a single row spanning the whole [begin, end] range
                         (an unbatched temporal model)
  * timeless           — a single NULL-window row (a whole-table load or a view;
                         view-vs-table is irrelevant to satisfaction)

`is_satisfied` is keyed by the downstream's contract level (or, for a bare
upstream, the upstream's own `kind`). The hard error for the window-scoped
levels (EXACT / ENCAPSULATE / THROUGH) against a TIMELESS upstream lives one
layer up in `is_upstream_satisfied_live`; here we assert what `is_satisfied`
itself computes (a timeless upstream simply fails to match a window query).

`TestEncapsulateUnion` and `TestExact` cover the two new modes specifically: the
finer→coarser **union** coverage that distinguishes ENCAPSULATE from the old
single-row WINDOW, and the exact-grain match that makes EXACT reject both coarser
rows and finer unions.
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
# A downstream window COARSER than any single upstream window — the whole 3-day
# span. No single daily row contains it; only a union of all three does.
SPAN = TZInterval(D1, D4)
# A coarser window starting one day in (used to show a hole *outside* my window
# doesn't block ENCAPSULATE, but does block THROUGH).
TAIL = TZInterval(D2, D4)


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
        [
            ("exists", True),
            ("exact", True),  # an applied row equals [D2,D3] exactly
            ("encapsulate", True),  # [D2,D3] is applied (single-row fast path)
            ("through", True),
            ("whole", True),
        ],
    )
    def test_fully_loaded(self, conn, contract, expected):
        t = _table(conn, _batched(loaded=True))
        assert _ok(conn, t, self.KIND, contract=contract, interval=INSIDE) is expected

    @pytest.mark.parametrize(
        "contract,expected",
        [
            ("exists", True),  # registered
            ("exact", True),  # my exact window [D2,D3] is applied
            ("encapsulate", True),  # my window [D2,D3] is applied
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
            ("encapsulate", INSIDE, True),  # [D2,D3] falls inside [D1,D4]
            ("encapsulate", AT_END, True),  # so does [D3,D4]
            ("encapsulate", SPAN, True),  # the row IS [D1,D4] — contains it
            # EXACT needs a row at exactly my grain. The one row is [D1,D4], so
            # it matches only the whole-span window, never a sub-window.
            ("exact", SPAN, True),
            ("exact", INSIDE, False),  # a coarser row does not satisfy EXACT
            ("exact", AT_END, False),
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
        [
            ("exists", True),
            ("whole", False),
            ("encapsulate", False),
            ("exact", False),
            ("through", False),
        ],
    )
    def test_pending(self, conn, contract, expected):
        t = _table(conn, _oneshot(loaded=False))
        assert _ok(conn, t, self.KIND, contract=contract, interval=INSIDE) is expected


class TestTimeless:
    """A timeless upstream (whole-table load or view): one NULL-window row.

    The window-scoped levels (EXACT / ENCAPSULATE / THROUGH) are rejected for a
    timeless upstream by the live gate; at the `is_satisfied` layer they simply
    never match the NULL-window row (False). Only EXISTS / WHOLE are meaningful,
    plus the bare-string fallback (kind=None → the upstream's own `timeless` kind
    → existence check).
    """

    KIND = "timeless"

    @pytest.mark.parametrize(
        "contract,expected",
        [
            ("exists", True),  # registered
            ("whole", True),  # the one row is applied
            ("exact", False),  # no window to match
            ("encapsulate", False),  # no window to match
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


class TestEncapsulateUnion:
    """ENCAPSULATE's distinguishing power: a downstream window COARSER than any
    single upstream window is covered by a gap-free UNION of finer rows. The old
    single-row WINDOW would block all of these (no one row contains the span)."""

    KIND = "temporal"

    def test_finer_union_covers_coarser_window(self, conn):
        # 3 daily rows, all applied; downstream wants the whole [D1,D4] span.
        # No single row contains it — the union of all three tiles it gap-free.
        t = _table(conn, _batched(loaded=True))
        assert _ok(conn, t, self.KIND, contract="encapsulate", interval=SPAN) is True

    def test_partial_union_covers_sub_span(self, conn):
        # [D1,D2] + [D2,D3] tile [D1,D3] exactly.
        t = _table(conn, [(D1, D2, "applied"), (D2, D3, "applied")])
        assert (
            _ok(conn, t, self.KIND, contract="encapsulate", interval=TZInterval(D1, D3))
            is True
        )

    def test_hole_in_union_blocks(self, conn):
        # Missing middle window → a gap at [D2,D3] → [D1,D4] not covered.
        t = _table(conn, [(D1, D2, "applied"), (D3, D4, "applied")])
        assert _ok(conn, t, self.KIND, contract="encapsulate", interval=SPAN) is False

    def test_pending_middle_window_is_a_hole(self, conn):
        # A pending middle row is not `applied`, so it leaves a gap.
        t = _table(
            conn,
            [(D1, D2, "applied"), (D2, D3, "pending"), (D3, D4, "applied")],
        )
        assert _ok(conn, t, self.KIND, contract="encapsulate", interval=SPAN) is False

    def test_hole_outside_my_window_does_not_block(self, conn):
        # The KEY difference from THROUGH: a pending [D1,D2] is OUTSIDE my window
        # [D2,D4]. ENCAPSULATE only looks at rows covering my window, so it's
        # satisfied; THROUGH (gap-free prefix up to D4) is blocked by the hole.
        t = _table(
            conn,
            [(D1, D2, "pending"), (D2, D3, "applied"), (D3, D4, "applied")],
        )
        assert _ok(conn, t, self.KIND, contract="encapsulate", interval=TAIL) is True
        assert _ok(conn, t, self.KIND, contract="through", interval=TAIL) is False


class TestExact:
    """EXACT requires a row at *exactly* my grain — it rejects both a coarser
    containing row and a gap-free union of finer rows (which ENCAPSULATE accept)."""

    KIND = "temporal"

    def test_exact_grain_row_satisfies(self, conn):
        t = _table(conn, _batched(loaded=True))
        assert _ok(conn, t, self.KIND, contract="exact", interval=INSIDE) is True

    def test_rejects_finer_union(self, conn):
        # 3 daily rows cover [D1,D4] as a union — but no single row equals it,
        # so EXACT is NOT satisfied (ENCAPSULATE would be).
        t = _table(conn, _batched(loaded=True))
        assert _ok(conn, t, self.KIND, contract="exact", interval=SPAN) is False
        assert _ok(conn, t, self.KIND, contract="encapsulate", interval=SPAN) is True

    def test_rejects_coarser_container(self, conn):
        # One [D1,D4] row contains [D2,D3] but isn't equal to it → EXACT False,
        # ENCAPSULATE True.
        t = _table(conn, _oneshot(loaded=True))
        assert _ok(conn, t, self.KIND, contract="exact", interval=INSIDE) is False
        assert _ok(conn, t, self.KIND, contract="encapsulate", interval=INSIDE) is True


# ── the live gate: a window-scoped level on a TIMELESS upstream is a hard error ──
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


class TestWindowScopedOnTimelessRaises:
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
        "level",
        [
            UpstreamContract.EXACT,
            UpstreamContract.ENCAPSULATE,
            UpstreamContract.THROUGH,
        ],
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
