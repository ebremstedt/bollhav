[Home](index.md) › [Model](MODEL.md) › **Upstream**

# Upstream

A model's inputs — **all of them** — live in one list: `upstream: list[Source]`. A `Source` is one input, described by two independent dials:

- **`type`** — *what* it is, and its read config: a [`SourceModel`](#sourcemodel) (relational — a managed model, an external table, or a view), a [`SourceFile`](#sourcefile), or a `SourceApi`.
- **`contract`** — *whether it gates*. A `Source` carrying a contract is a **managed upstream** the state machine waits for (it must be `applied` before this model runs). No contract ⇒ **ungated** — an external input assumed always present, never blocking.

That's the whole model: gated-vs-ungated is just "does this `Source` have a `contract`," not which list it's in.

```python
from bollhav.model import UpstreamContract

upstream=[
    # gated managed upstreams (need state — see below):
    Source("warehouse.orders",    type=SourceModel(), contract=UpstreamContract.WINDOW),
    Source("warehouse.customers", type=SourceModel(), contract=UpstreamContract.WHOLE),
    Source("warehouse.app_config",type=SourceModel(), contract=UpstreamContract.EXISTS),
    # ungated external sources:
    Source("raw.landing",  type=SourceModel(dsn_env_var="RAW_DSN")),
    Source("crm.contacts", type=SourceApi(base_url="https://crm/api")),
]
```

A **contract is only valid on a `SourceModel`** — files and APIs aren't state-tracked, so they can't gate. Declaring one on a `SourceFile`/`SourceApi` is a definition-time error.

## Contracts (gating)

A contract is the gating **level** — *how much* of the upstream must be ready before this model runs — chosen from the `UpstreamContract` ladder (weak → strong). The upstream's *shape* (its [temporality](TEMPORALITY.md)) is read from the library at check time, so you only state the level:

| `UpstreamContract` | Satisfied when |
|---|---|
| `EXISTS` | the upstream is **registered**. No data wait — run-ordering + a lineage edge only. |
| `WINDOW` | the upstream's window covering this unit's `(since, until)` is `applied`. A daily upstream covers an hourly downstream. *(the usual choice)* |
| `THROUGH` | every upstream window **up to and including** this unit is `applied` — a gap-free prefix, for cumulative models that sum history `1..N`. |
| `WHOLE` | the **entire** upstream is loaded — every window `applied` (a temporal upstream), or its one row `applied` (a timeless one). |

**Timeless upstreams.** A [`TIMELESS`](TEMPORALITY.md) upstream (a view or whole-table load) has no window to match, so `WINDOW`/`THROUGH` against it is a hard error — gate it `WHOLE` (loaded) or `EXISTS` (registered). `WINDOW`/`THROUGH` are for temporal upstreams.

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

The age is measured against the upstream's `applied_at` — when bollhav *loaded* each row (a producer-side, shared timestamp), **not** the source's own event time. The threshold and scope are **per-consumer**: different downstreams can demand different freshness off the same upstream load. The rows checked are exactly the ones the contract level selects (`WHOLE` → all; `WINDOW` → the matching window; `THROUGH` → the prefix; timeless → the existence row).

| `FreshnessScope` | Fresh when |
|---|---|
| `LATEST` | the **newest** applied row in the selection is within `within` (`max(applied_at)`). "Is it keeping up at the head?" — for append-only / growing tables. |
| `ALL` | **every** applied row in the selection is — i.e. the **oldest** one (`min(applied_at)`). "Was the whole thing rebuilt recently?" — for full-refresh snapshots / reference tables. |

The scope only differs on a **multi-row** selection (`WHOLE` / `THROUGH` over several windows). For a single-row selection (`WINDOW`, or a timeless upstream) `LATEST` and `ALL` are identical.

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
Source("warehouse.orders", type=SourceModel(), contract=UpstreamContract.WINDOW, deactivate_for_dev=True)
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

## Ungated sources

An **ungated source** is a `Source` in `upstream` with **no `contract`** — a raw landing table, a third-party API, a dropped file, a hand-made table. Assumed always present, it can never block a unit of work; declaring it just records where data enters the system and (for relational types) enables `ref()` resolution. You never *have* to declare one — you can always hardcode an external table in your SQL. Declaring is the opt-in that buys you [lineage](LINEAGE.md) and `ref()` resolution.

| | gated upstream | ungated source |
|---|---|---|
| has a `contract` | yes | no |
| refers to | a bollhav-managed model | an external, unmanaged input |
| requires [state](STATE.md) | yes — gated by the state machine | no — never gated |
| `type` allowed | `SourceModel` only | any (`SourceModel` / `SourceFile` / `SourceApi`) |
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
