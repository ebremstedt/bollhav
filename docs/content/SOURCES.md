[Home](index.md) › [Model](MODEL.md) › **Sources**

# Sources

External inputs a model reads but bollhav does **not** manage — a raw landing table, a third-party API, a dropped file, a hand-made table. A model's `sources` is a list of `Source` (or bare strings) declaring where data enters the system from outside bollhav.

Sources are **optional** and exist mainly for **lineage**. Unlike [upstream](UPSTREAM.md), a source has no [state](STATE.md) and is never gated — it's assumed always present, so it can't block a unit of work. Declaring one just records a boundary in the dependency graph.

You never *have* to declare a source: you can always hardcode an external table directly in your SQL. Declaring is the opt-in that buys you two things — [lineage](LINEAGE.md), and `source_ref()` resolution.

```python
sources=[
    Source("raw.landing_orders"),                      # a database table (default)
    Source("vendor.orders", kind=SourceKind.API),       # an external API
    Source("dropzone/customers.csv", kind=SourceKind.FILE),
]
```

## Sources vs upstream

| | [Upstream](UPSTREAM.md) | Sources |
|---|---|---|
| Refers to | a bollhav-managed model | an external, unmanaged input |
| Requires state | yes — gated by the state machine | no — never gated |
| Optional | enforced when declared | always optional (lineage only) |
| Schema suffix | applied (`ref()`) | not applied (`source_ref()`, literal) |
| Purpose | wait-for + lineage | lineage / boundary marker |

## SourceKind

A source is tagged by `kind`. Only `DATABASE` / `VIEW` are **SQL-addressable** — you can `source_ref()` them into a `FROM`. The rest are read by your read function (Python), so declaring them is lineage-only and `source_ref()` on one raises.

| kind | SQL-addressable? |
|---|---|
| `DATABASE` (default) — relational table | ✅ |
| `VIEW` — database view | ✅ |
| `FILE` — CSV / Parquet / JSON, local or object storage | ❌ |
| `API` — REST / HTTP | ❌ |
| `SFTP` — file over SFTP / FTP | ❌ |
| `STREAM` — queue / stream (Kafka, Kinesis, …) | ❌ |
| `SPREADSHEET` — Excel / Google Sheets | ❌ |
| `SEED` — static / inline / hand-seeded | ❌ |

## Referencing a source in SQL

`model.source_ref("name")` resolves an SQL-addressable source to a quoted, **literal** identifier — **no schema suffix**, because an external table lives at the same fixed location in every environment (dev / prod / PR), whereas managed models move with the suffix.

```python
sources=[Source("raw.landing_orders")]
...
query = f"SELECT * FROM {model.source_ref('raw.landing_orders')} WHERE ..."
# -> SELECT * FROM "raw"."landing_orders" WHERE ...
#    (unchanged even under SCHEMA_SUFFIX=pr123)
```

(It's `source_ref()`, not `source()`, because `model.source` is already the model's own [read-source](SOURCETABLE.md) definition.)

Calling `source_ref()` on a non-SQL kind (`FILE`, `API`, …) raises — there's no `FROM` for an API or a file. Read those in your read function; the declaration is still recorded for lineage.

## Unknown provenance

A model that declares **no** upstreams and **no** sources reads from something untracked (hardcoded SQL, or a Python read with no declarations). Two computed fields surface this for a [lineage](LINEAGE.md) audit:

```python
model.declared_inputs   # ["warehouse.orders", "raw.landing"] — upstreams + sources, by name
model.inputs_known      # False when nothing is declared → provenance unknown
```

## See also

- [Upstream](UPSTREAM.md) — managed, state-gated dependencies.
- [Lineage](LINEAGE.md) — the dependency graph these feed.
- [Library](LIBRARY.md) — the registry where models (and their inputs) are recorded.
