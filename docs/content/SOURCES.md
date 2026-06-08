[Home](index.md) › [Model](MODEL.md) › **Sources**

# Sources

A **source** is an ungated input: a `Source` in the model's [`upstream`](UPSTREAM.md) list with **no `contract`**. It's something the model reads but bollhav doesn't gate on — a raw landing table, a third-party API, a dropped file, a hand-made table. Assumed always present, it can never block a unit of work; declaring it just records where data enters the system and (for relational types) enables `ref()` resolution.

There's no separate `sources=` list anymore — gated upstreams and ungated sources share one `upstream` list. The only difference is the `contract`:

```python
upstream=[
    Source("warehouse.orders", type=SourceModel(), contract=IntervalContract()),  # gated upstream
    Source("raw.landing",      type=SourceModel(dsn_env_var="RAW_DSN")),           # ungated source
    Source("crm.contacts",     type=SourceApi(base_url="https://crm/api")),        # ungated source
]
```

You never *have* to declare a source: you can always hardcode an external table in your SQL. Declaring is the opt-in that buys you [lineage](LINEAGE.md) and `ref()` resolution.

## Gated upstream vs ungated source

| | gated upstream | ungated source |
|---|---|---|
| has a `contract` | yes | no |
| refers to | a bollhav-managed model | an external, unmanaged input |
| requires [state](STATE.md) | yes — gated by the state machine | no — never gated |
| `type` allowed | `SourceModel` only | any (`SourceModel` / `SourceFile` / `SourceApi`) |
| `ref()` resolution | suffix-aware (moves with env) | literal (fixed location) |
| purpose | wait-for + lineage | lineage / boundary marker |

## type — what the source is

A `Source`'s `type` says what it is and carries its read config. Only a `SourceModel` is **SQL-addressable** (you can `ref()` it into a `FROM`); the rest are read by your read function, so `ref()` on them raises.

| type | SQL-addressable? | for |
|---|---|---|
| [`SourceModel`](SOURCETABLE.md) | ✅ | a relational table / view (set `query=` for a view) |
| [`SourceFile`](SOURCEFILE.md) | ❌ | CSV / Parquet / JSON, local or object storage |
| `SourceApi` | ❌ | REST / HTTP endpoint |

## Referencing a source in SQL

`model.ref("name")` resolves a `SourceModel` source to a quoted, **literal** identifier — **no schema suffix**, because an external table lives at the same fixed location in every environment (dev / prod / PR), whereas a gated managed model moves with the suffix.

```python
upstream=[Source("raw.landing_orders", type=SourceModel())]
...
query = f"SELECT * FROM {model.ref('raw.landing_orders')} WHERE ..."
# -> SELECT * FROM "raw"."landing_orders" WHERE ...
#    (unchanged even under SCHEMA_SUFFIX=pr123 — it has no contract)
```

Calling `ref()` on a `SourceFile`/`SourceApi` raises — there's no `FROM` for a file or an API. Read those in your read function; the declaration is still recorded for lineage.

## Unknown provenance

A model that declares **nothing** reads from something untracked (hardcoded SQL, or a Python read with no declarations). bollhav marks this with an auto-injected typeless source — see [None](SOURCE_NONE.md) — and two computed fields surface it for a [lineage](LINEAGE.md) audit:

```python
model.declared_inputs   # ["warehouse.orders", "raw.landing"] — gated + ungated, by name
model.inputs_known      # False when nothing real is declared → provenance unknown
```

## See also

- [Upstream](UPSTREAM.md) — the one inputs list; gating, contracts, and `ref()`.
- [Lineage](LINEAGE.md) — the dependency graph these feed.
- [Library](LIBRARY.md) — the registry where models (and their inputs) are recorded.
