[Home](index.md) › [Model](MODEL.md) › **Upstream**

# Upstream

A model's inputs — **all of them** — live in one list: `upstream: list[Source]`. A `Source` is one input, described by two independent dials:

- **`type`** — *what* it is, and its read config: a [`SourceModel`](#sourcemodel) (relational — a managed model, an external table, or a view), a [`SourceFile`](#sourcefile), a `SourceApi`, or a [`SourceHardcoded`](#sourcehardcoded) (data written inline, in code).
- **`contract`** — *whether it gates*. A `Source` carrying a contract is a **managed upstream** the state machine waits for (it must be `applied` before this model runs). No contract ⇒ **ungated** — an external input assumed always present, never blocking.

That's the whole model: gated-vs-ungated is just "does this `Source` have a `contract`," not which list it's in.

```python
from bollhav.model import UpstreamContract

upstream=[
    # gated managed upstreams (need state — see below):
    Source("warehouse.orders",    type=SourceModel(), contract=UpstreamContract.ENCAPSULATE),
    Source("warehouse.customers", type=SourceModel(), contract=UpstreamContract.WHOLE),
    Source("warehouse.app_config",type=SourceModel(), contract=UpstreamContract.EXISTS),
    # ungated external sources:
    Source("raw.landing",  type=SourceModel(dsn_env_var="RAW_DSN")),
    Source("crm.contacts", type=SourceApi(base_url="https://crm/api")),
]
```

A **contract is only valid on a `SourceModel`** — files and APIs aren't state-tracked, so they can't gate. Declaring one on a `SourceFile`/`SourceApi` is a definition-time error.

## Contracts (gating)

A contract is the gating **level** — *how much* of the upstream must be ready before this model runs — chosen from the `UpstreamContract` ladder. The upstream's *shape* (its [temporality](TEMPORALITY.md)) is read from the library at check time, so you only state the level. Five levels, on three **scopes**:

| `UpstreamContract` | Scope | Satisfied when |
|---|---|---|
| `EXISTS` | registration | the upstream is **registered**. No data wait — run-ordering + a lineage edge only. |
| `EXACT` | my window | one `applied` upstream row **equals** my `(since, until)`. Only an exact-grain match — a coarser row that *contains* my window, or a union of finer rows, does **not** count. |
| `ENCAPSULATE` | my window | the `applied` rows **cover** my `(since, until)` — a single coarser row that contains it, the exact-grain row, or a gap-free **union** of finer rows. A daily upstream covers an hourly downstream; 24 hourly rows cover a daily one. *(the usual choice)* |
| `THROUGH` | prefix | every upstream window **up to and including** my window is `applied` — a gap-free prefix, for cumulative models that sum history `1..N`. A hole anywhere *before* my window blocks it. |
| `WHOLE` | whole upstream | the **entire** upstream is loaded — every window `applied` (a temporal upstream), or its one row `applied` (a timeless one). |

The three window-scoped levels (`EXACT` ⊂ `ENCAPSULATE`, and `EXISTS` below them) consult **only** the upstream rows touching my window — nothing outside `[since, until)` can block me. `THROUGH` and `WHOLE` widen the scope past my window, so a hole elsewhere *does* block. Strength ladder: `WHOLE` ⟹ `THROUGH` ⟹ `ENCAPSULATE` ⟹ `EXISTS`, with `EXACT` ⟹ `ENCAPSULATE` (an exact-grain row also covers the window).

### When to use which

- **`EXISTS`** — you only need *ordering + lineage*, not data: a config/seed table that just has to be registered, or a windowless consumer (view / monolithic) that wants a managed edge to an interval upstream without waiting for it.
- **`EXACT`** — the upstream is **aggregated at exactly my grain** and that alignment is load-bearing: a daily rollup consuming a daily upstream rollup, where you want *that day's own* aggregate row and a coarser backfill that merged several days must **not** count as ready.
- **`ENCAPSULATE`** — the default for a **window-local transform**: my window's output is a pure function of my window's input (filter / segment / reshape this slice). Pipelines per-window and tolerates the upstream being at any grain — coarser *or* finer — as long as my window is covered. Use this whenever `WINDOW` was your instinct.
- **`THROUGH`** — my window's output **depends on history `1..N`**, not just window N: running totals, cumulative balances, snapshots-as-of. Waits for the gap-free prefix; a hole in old history correctly blocks me.
- **`WHOLE`** — I read **all** of the upstream every run: a full aggregate, an export, a snapshot, or any windowless downstream of an interval upstream.

> `ENCAPSULATE` vs `EXACT`: both look only at my window. `ENCAPSULATE` asks *"is my window's data present, by any combination of rows?"*; `EXACT` asks the stricter *"did the upstream produce a row at exactly my grain?"*. `ENCAPSULATE` vs `THROUGH`: both are gap-free, but `ENCAPSULATE` covers *only* my window while `THROUGH` covers the *whole prefix up to* it — so one old hole blocks `THROUGH` and not `ENCAPSULATE`.

> **Renamed:** the former `WINDOW` level (a single-row "a row contains my window" check) has been **removed** and folded into `ENCAPSULATE`, which subsumes it (a single containing row is a one-element cover) and additionally accepts a finer union. Replace any `UpstreamContract.WINDOW` with `UpstreamContract.ENCAPSULATE`.

**Timeless upstreams.** A [`TIMELESS`](TEMPORALITY.md) upstream (a view or whole-table load) has no window to match, so `EXACT` / `ENCAPSULATE` / `THROUGH` against it is a hard error — gate it `WHOLE` (loaded) or `EXISTS` (registered). The window-scoped levels are for temporal upstreams.

**Fixed vs flexible upstreams.** A *temporal* upstream is one of two shapes, set by [`fixed_intervals`](BATCH.md) on its `TimeChunking` (default `True`):

- **Fixed** (`fixed_intervals=True`) — state is a **grid**: one durable `applied` row per chunk, at a stable grain. Its exact-grain rows persist, so every contract level works against it.
- **Flexible** (`fixed_intervals=False`) — state is a **coverage set**: `applied` rows are coalesced into maximal covered ranges, so individual exact-grain rows don't survive. The chunk is just how work is sliced, not part of the upstream's identity.

Because a flexible upstream coalesces away its exact-grain rows, **`EXACT` against a flexible upstream is a hard error** — it could never match, so the downstream would block forever. Use `ENCAPSULATE` (coverage) instead. The coverage-based levels are unaffected:

| Upstream is… | `EXISTS` | `EXACT` | `ENCAPSULATE` | `THROUGH` | `WHOLE` |
|---|:---:|:---:|:---:|:---:|:---:|
| **Fixed** (default) | ✓ | ✓ | ✓ | ✓ | ✓ |
| **Flexible** (`fixed_intervals=False`) | ✓ | ✗ error | ✓ | ✓ | ✓ |

`fixed_intervals` is **per-model and does not propagate**: a flexible upstream feeding a fixed downstream is fine — the downstream reads the upstream's *data* (by coverage), not its partitioning, and declaring flexible *means* that data is partition-invariant. So a flexible upstream is in fact the safest to build coverage contracts on; only `EXACT` (which demands a specific grain) is off the table.

!!! note "Forward-looking"
    The `EXACT`-on-flexible rule above is **enforced today** (it raises at gate-check time). The flexible *execution* engine — coverage gap-filling and range coalescing — is still being built (see `design/flexible-intervals.md`); until it lands, leave `fixed_intervals` at its default `True`.

### Two axes: identity (`fixed_intervals`) and invalidation (`STATE_MODE`)

`fixed_intervals` and [`STATE_MODE`](STATE.md) are **orthogonal** — one says what a model's state *is*, the other says how much of it a run *throws away* before re-evaluating:

- **Identity — `fixed_intervals`** (per-model, declared in code): **fixed** = a grid keyed by `(since, until)`, where the chunk *is* the state's identity; **flexible** = a coverage set, where the chunk is just how work is sliced.
- **Invalidation — `STATE_MODE`** (per-run, an env var): how much existing state the run resets first —
    - **`discover`** (default) — reset nothing: keep every `applied` row, only add / re-check the rest.
    - **`bulldozer`** — reset the run's window back to `pending` (the `(since, until)` boundaries are kept).
    - **`torch`** — drop *all* state and re-prefill from scratch at the current chunk.

What each mode *does* depends on the identity axis:

| `STATE_MODE` | **fixed** (grid) | **flexible** (coverage) |
|---|---|---|
| `discover` | keep `applied` rows; re-check the rest | leave coverage as-is, fill only the gaps |
| `bulldozer` | re-`pending` the window's rows (same boundaries) | uncover the run window, then re-cover it |
| `torch` | delete all rows, re-prefill at the (possibly new) chunk | uncover everything, re-cover from zero |

For a **flexible** model the three modes collapse into one idea — *how much coverage to uncover before computing gaps*: `discover` = nothing, `bulldozer` = the run window, `torch` = all. That's also why **re-chunking** behaves differently per axis: a fixed model's grain is its identity, so [`INTERVAL_OVERRIDE`](BATCH.md) is **ignored** on it (re-chunking would fork the grid into mixed granularity — migrate with `STATE_MODE=torch` instead); a flexible model's chunk isn't identity, so `INTERVAL_OVERRIDE` **applies** to it and re-chunks freely.

(The flexible column is forward-looking — the engine that uncovers / re-covers is still being built, per the note above. `INTERVAL_OVERRIDE`'s fixed-vs-flexible gating and the `EXACT`-on-flexible guard, however, are live today.)

### Combinations that don't fit — and the damage

Not every (upstream shape × contract) or (model attestation × write/query) pairing is safe. Some are **caught** for you (a loud error at definition or gate-check); the dangerous ones are **silent** — they corrupt data with no crash. Severity legend: ⛔ caught (safe — you can't ship it) · ⏳ blocks forever (visible, recoverable) · 🔴 silent divergence (disastrous — wrong data, no error).

| Combination | Caught? | What actually happens |
|---|---|---|
| `EXACT` on a **flexible** upstream | ⛔ hard error (Guard A) | The flexible upstream coalesces its rows into ranges, so no exact-grain row exists to match — the downstream would sit `blocked` **forever**. Raised at gate-check. |
| `EXACT` / `ENCAPSULATE` / `THROUGH` on a **timeless** upstream | ⛔ hard error | A timeless upstream has no window to match a window-scoped level against — never satisfiable. Gate it `WHOLE` or `EXISTS`. |
| `EXACT` on a **fixed** upstream at a **different grain** | ⏳ blocks | EXACT demands a row at *exactly* your `(since,until)`; a daily upstream never produces your hourly grain → permanently `blocked`. Not wrong data, just stalled — use `ENCAPSULATE`. |
| **Flexible** model + `APPEND` write mode | 🔴 **not caught** | Flexibility re-processes ranges; `APPEND` has no key to overwrite, so every re-cover **duplicates rows**. (Deliberately not enforced — duplicates are detectable and yours to dedupe.) |
| **Flexible** model + order-dependent `UPSERT` (last-writer-wins) | 🔴 **not caught** | Re-partitioning changes the order rows land; a last-writer-wins MERGE makes the final table **depend on partition order** → silently divergent results. Safe only if ties break deterministically (`incoming._data_modified > existing`) or you use `RECREATE_PARTITION`. |
| **Flexible** model + non-decomposable query (`GROUP BY`, `ROW_NUMBER … latest-in-window`, cumulative `THROUGH`) | 🔴 **not caught** | Flexibility assumes `[a,c) ≡ [a,b) ∪ [b,c)`. Aggregates and latest-per-key-within-window break that identity, so re-chunking yields **different numbers** — silently wrong. This is the attestation you sign with `fixed_intervals=False`. |
| **Fixed** model, chunk grain changed (e.g. `INTERVAL_OVERRIDE` hourly→monthly) without `STATE_MODE=torch` | 🔴 **not caught** | Old fine-grained `applied` rows stay; the executor drains every non-applied row unscoped, so the old partitioning leaks into the new run ("ran 14,200 hourly windows under a monthly override"). Re-chunking a fixed model **is** a migration → `STATE_MODE=torch`. |

The pattern: the **caught** rows are the ones the framework can prove wrong from the model definition + the library (a contract level against a known shape). The **silent** rows are exactly the half of the flexibility attestation that *can't* be proven statically — they're the dev's signed promise, which is why `fixed_intervals=False` is opt-in and defaults to `True`.

!!! tip "Does your transform aggregate? Stay fixed."
    If the model **aggregates** — `GROUP BY`, `SUM`/`COUNT`/`AVG`, `ROW_NUMBER() … latest-per-key within the window`, or any running total / cumulative (`THROUGH`-style) logic — its output is **not** invariant to how the window is partitioned: `[a, c)` ≠ `[a, b) ∪ [b, c)` once you aggregate. **Keep `fixed_intervals=True`** (the default) — one durable row per chunk at a stable grain. Never set `fixed_intervals=False` on an aggregate: that's the 🔴 silent-divergence row above (re-chunking would change the numbers with no error). Aggregated models stay fixed; downstreams may then use any contract level against them, including `EXACT` if they need that exact grain. Reserve `fixed_intervals=False` for **window-local, row-preserving** transforms — a pure `WHERE _data_modified ∈ window` filter / map / reshape with an idempotent write — where partitioning genuinely doesn't change the result.

A `Source` with **no** contract is **ungated** — never waited on (there's no default level).

**Gating requires [state](STATE.md).** A contract is checked by the state machine (`@execute_lifecycle`), which only runs for state-tracked models. A gated upstream without `state=State(...)` is an error — the contract would otherwise never be enforced. Ungated sources need no state.

### What happens

Before each unit, every **gated** upstream is checked (no short-circuit):

- all satisfied → runs.
- any unsatisfied → unit is `blocked`; `blocked_reason` names each missing upstream.

On the next run, blocked units re-evaluate — they go `pending` once upstreams catch up. A gated upstream naming an unregistered model is a hard error, not a skip. Ungated sources are never checked.

## Freshness (recency)

A contract *level* says **how much** of an upstream must be applied. **Freshness** adds a second, orthogonal gate: **how recently** it was loaded. Attach a `Freshness` to a gated `Source` and — once the level's completeness is satisfied — the upstream is still **blocked as stale** if its relevant rows are too old.

```python
from datetime import timedelta
from bollhav.model import Freshness, FreshnessScope

Source(
    "warehouse.orders", type=SourceModel(),
    contract=UpstreamContract.WHOLE,
    freshness=Freshness(within=timedelta(days=1), scope=FreshnessScope.LATEST),
)
```

The age is measured against the upstream's `applied_at` — when bollhav *loaded* each row (a producer-side, shared timestamp), **not** the source's own event time. The threshold and scope are **per-consumer**: different downstreams can demand different freshness off the same upstream load. The rows checked are exactly the ones the contract level selects (`WHOLE` → all; `EXACT` → the exact-grain row; `ENCAPSULATE` → the rows covering my window; `THROUGH` → the prefix; timeless → the existence row).

| `FreshnessScope` | Fresh when |
|---|---|
| `LATEST` | the **newest** applied row in the selection is within `within` (`max(applied_at)`). "Is it keeping up at the head?" — for append-only / growing tables. |
| `ALL` | **every** applied row in the selection is — i.e. the **oldest** one (`min(applied_at)`). "Was the whole thing rebuilt recently?" — for full-refresh snapshots / reference tables. |

The scope only differs on a **multi-row** selection (`WHOLE` / `THROUGH`, or an `ENCAPSULATE` window satisfied by a union of finer rows). For a single-row selection (`EXACT`, a single-container `ENCAPSULATE`, or a timeless upstream) `LATEST` and `ALL` are identical.

- **`LATEST`** catches a *stalled* pipeline (nothing new landing) but ignores immutable history — right for append-only tables, where `ALL` would be permanently stale (it'd demand re-loading ancient partitions).
- **`ALL`** catches a *frozen partition* (one window silently stopped refreshing) that `LATEST` misses — right when a stale slice means wrong answers.

A stale upstream blocks like an unmet contract; its `blocked_reason` marks it `(<level>, stale)` — present-but-old, not missing.

**Rules.** `freshness` requires a `contract`, and is **not** valid with `EXISTS` (which never inspects state — there's no `applied_at` to age). Both are definition-time errors.

## Referencing an input in SQL

`model.ref("name")` resolves a declared `SourceModel` input to a quoted identifier for a read query — **suffix-aware when it's gated**, literal when it isn't:

```python
# gated upstream — moves with the env's schema suffix (dev / prod / PR)
query = f"SELECT * FROM {model.ref('warehouse.orders')} WHERE day >= %(since)s"
# -> "warehouse"."orders"   (under SCHEMA_SUFFIX=pr123 -> "warehouse_pr123"."orders")

# ungated source — fixed external location, no suffix
f"... FROM {model.ref('raw.landing')}"   # -> "raw"."landing"
```

A gated source is a managed model, so its schema moves with the suffix just like your own target — the query stays portable. An ungated source is external, at a fixed location in every environment, so it resolves literally. Referencing an undeclared name raises; `ref()` on a `SourceFile`/`SourceApi` raises (there's no `FROM` for a file or an API — read those in your read function).

## Shared upstreams — dev against prod (`deactivate_for_dev`)

In a [suffixed](TARGET.md#schema-vs-table-suffix) (dev / PR) run, a gated upstream resolves to *your env's* copy (`warehouse_pr123.orders`) and gates against your env's state. But some upstreams aren't rebuilt per environment — a shared table that lives only in prod. Set `deactivate_for_dev=True` on the `Source` to read it from prod and trust it — **only in a suffixed run**. It's inert without a suffix, so you declare it once and never flip it between dev and prod.

```python
Source("warehouse.orders", type=SourceModel(), contract=UpstreamContract.ENCAPSULATE, deactivate_for_dev=True)
```

The three cases and how to declare each:

| Case | Declaration | `ref()` reads | Gating |
|---|---|---|---|
| **dev → prod table**, assume its state ok | `contract=…, deactivate_for_dev=True` | prod — `"warehouse"."orders"` | assumed okay (skipped) |
| **dev → dev table**, check its state | `contract=…` (default) | dev copy — `"warehouse_pr123"."orders"` | checks the dev registration |
| **prod → prod table** | *either of the above* | prod — `"warehouse"."orders"` | checks prod's registration |

**Case 3 is automatic.** Prod has no suffix, so every gated upstream reads prod and gates prod regardless of `deactivate_for_dev` — the flag only does anything in a suffixed run. Different upstreams on one model can mix freely (`orders` pinned to prod, `customers` using your dev copy).

To assume an upstream is okay in **every** environment, prod included, don't gate it at all — drop the contract (an [ungated source](#ungated-sources)), which is never checked anywhere.

## SourceModel

The `type` of a **relational** input — a database table, a view, or another bollhav-managed model. It carries the config to read it:

```python
upstream=[
    Source("raw.orders", type=SourceModel(schema="raw", dsn_env_var="RAW_DSN")),
]
```

`SourceModel` is the only **SQL-addressable** type — `model.ref("raw.orders")` resolves it into a `FROM`. It's also the only type that can be **gated**: attach a `contract` to make it a managed upstream. A [view](TEMPORALITY.md) model's (`view=True`) definition *is* a `SourceModel` with a `query` set, in its `upstream` list — that's what `CREATE OR REPLACE VIEW` runs.

| Field | Type · Default | Purpose |
|---|---|---|
| `schema` | `str` · `None` | Source schema. |
| `catalog` | `str` · `None` | Source catalog / database (3-part `catalog.schema.table` names). |
| `dsn_env_var` | `str` · `None` | DSN env var for the source connection. |
| `query` | `str` · `None` | Optional query override. On a [view](TEMPORALITY.md) model's (`view=True`) source it *is* the view definition; otherwise the loader may use this SQL instead of `SELECT * FROM <schema>.<name>`. |
| `partitioned_by` | `str` · `None` | Partition column on the source, when relevant to the read. |
| `infer_schema_length` | `int` · `None` | Passed to polars as `infer_schema_length` — max rows scanned to infer column types. `None` scans every row (slow on large sources). |
| `extra` | `dict` · `{}` | Free-form config bag for read functions that need extra knobs. |

## SourceFile

The `type` of a **file** input — loads from a file (CSV, Parquet, JSON, …) rather than a database:

```python
upstream=[Source("orders.csv", type=SourceFile(path=Path("dropzone/orders.csv")))]
```

A `SourceFile` is **not** SQL-addressable (`ref()` on it raises — there's no `FROM` for a file) and can't be gated (no `contract`). Your `read()` function uses its config to load the data.

| Field | Type · Default | Purpose |
|---|---|---|
| `path` | `Path` · *required* | Path to the source file. |
| `encoding` | `str` · `None` | Character encoding. `None` lets polars autodetect. |
| `separator` | `str` · `None` | Column separator (e.g. `","`, `"\t"`, `";"`). `None` lets polars infer. |
| `infer_schema_length` | `int` · `None` | Rows polars scans when inferring column types. `None` scans every row (slow on large files). |
| `remove_top_rows` | `int` · `0` | Strip N rows from the top before parsing — for files with cover sheets or extra header rows above the column row. |
| `archive_folder` | `Path` · `None` | If set, the file is moved here after a successful load — prevents the same file being processed twice. |
| `dateformat` | `str` · `None` | `strftime` format for date columns when polars can't autodetect (e.g. `"%d/%m/%Y"`). |
| `file_ending` | `str` · `None` | File extension hint (e.g. `"csv"`, `"tsv"`). Only needed when `path` doesn't carry one. |

## SourceHardcoded

The `type` of a **hardcoded** input — data written **inline, in code**, not read from a database, file, or API. For small constants kept in the repo: seed / reference / lookup tables, enum mappings, fixtures. The data lives in **exactly one** of two forms:

```python
from bollhav.model import Source, SourceHardcoded

upstream=[
    # Python rows — a list of dicts, one per row:
    Source("ref.countries", type=SourceHardcoded(rows=[
        {"id": 1, "code": "SE", "name": "Sweden"},
        {"id": 2, "code": "NO", "name": "Norway"},
    ])),
    # …or a self-contained SQL literal (no FROM a real table):
    Source("ref.flags", type=SourceHardcoded(
        sql="SELECT * FROM (VALUES (1, true), (2, false)) AS t(id, active)"
    )),
]
```

Like files / APIs it is **never gated** (a `contract` is rejected — hardcoded data is always present, there's nothing to wait on) and **never SQL-addressable** (`ref()` raises — it has no stable table identifier). You read it with `to_dataframe()` and write it like any other DataFrame — there's no read query or external connection to plumb:

```python
# in your execute body — no read() function needed:
df = my_source.to_dataframe(data_conn)   # rows: built directly; sql: run on the conn
write(conn=data_conn, run=run, df_gen=df)
```

| Field / member | Type · Default | Purpose |
|---|---|---|
| `rows` | `Sequence[Mapping] · None` | Inline Python rows (list of dicts). Column names come from the keys. |
| `sql` | `str · None` | A self-contained SQL literal (`VALUES` / `SELECT`-literal, no real `FROM`) that yields the rows, run on the data connection. |
| `extra` | `dict · {}` | Free-form config bag. |
| `content_hash` *(property)* | `str` | A stable, cross-process fingerprint of the inline content — the constant's **version**. Changes iff `rows`/`sql` change, so it can drive re-apply-on-edit (there's no `_data_modified` on a constant). |
| `to_dataframe(conn=None)` *(method)* | `pl.DataFrame` | Materialize: `rows` builds the frame directly (no connection); `sql` runs on `conn` (a psycopg / pyodbc connection) and reads the result. |

Exactly one of `rows` / `sql` must be set (both, or neither, is a definition-time error). In [lineage](LINEAGE.md) it appears as a real input with `kind="hardcoded"` — distinct from the `unknown-<uuid>` sentinel — so "which tables are code-maintained constants?" is an auditable query.

> **Materialization is explicit today.** You call `to_dataframe()` in your execute body and `write()` the result. A fully-automatic, view-style path (bollhav writes the constant with *no* execute body, re-applying when `content_hash` changes) is a planned follow-up.

## Ungated sources

An **ungated source** is a `Source` in `upstream` with **no `contract`** — a raw landing table, a third-party API, a dropped file, a hand-made table. Assumed always present, it can never block a unit of work; declaring it just records where data enters the system and (for relational types) enables `ref()` resolution. You never *have* to declare one — you can always hardcode an external table in your SQL. Declaring is the opt-in that buys you [lineage](LINEAGE.md) and `ref()` resolution.

| | gated upstream | ungated source |
|---|---|---|
| has a `contract` | yes | no |
| refers to | a bollhav-managed model | an external, unmanaged input |
| requires [state](STATE.md) | yes — gated by the state machine | no — never gated |
| `type` allowed | `SourceModel` only | any (`SourceModel` / `SourceFile` / `SourceApi` / `SourceHardcoded`) |
| `ref()` resolution | suffix-aware (moves with env) | literal (fixed location) |
| purpose | wait-for + lineage | lineage / boundary marker |

Only a [`SourceModel`](#sourcemodel) is SQL-addressable — `ref()` on a `SourceFile`/`SourceApi` raises; read those in your read function, the declaration is still recorded for lineage.

## Unknown provenance

A model doesn't have to declare any inputs. Your `read()` function is what actually produces the data (it receives the model + the interval and returns DataFrames), so where the rows come from is the read function's business — hardcoded SQL, or anything else.

```python
Model(
    target=Target(name="orders", ...),
    temporality=Temporality.TEMPORAL,
    batching=Batch(...),
    # no upstream — read() supplies the rows
)
```

When a model declares an empty `upstream`, bollhav doesn't leave it empty — provenance is *total*. It auto-injects a single **typeless** `Source`:

```python
Source(name="unknown-<uuid>", type=None)
```

- `type=None` is the marker for unknown provenance. It's never SQL-addressable and never gated.
- The name is uuid-suffixed so each unknown is a **distinct** node in the [lineage](LINEAGE.md) graph (two unknown-provenance models don't collapse into one).
- It isn't counted as a declared input — `source_names` / `upstream_names` exclude it.

Two computed fields surface this for a lineage audit:

```python
model.declared_inputs   # [] — nothing real declared (gated + ungated, by name, when present)
model.inputs_known      # False — its only input is the unknown sentinel
```

## See also

- [State](STATE.md) — status values, rerun behaviour, errors, locks.
- [Lineage](LINEAGE.md) — the dependency graph these feed.
- [examples/staging_state_contracts/](https://github.com/ebremstedt/bollhav/tree/main/examples/staging_state_contracts) — a view, a whole-table (timeless) model, a temporal model, and a fourth model gated on all three.
