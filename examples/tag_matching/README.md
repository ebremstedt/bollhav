# tag_matching

Shows how the `TAGS` expression selects models. No data is read or written,
this example just prints the matched models so you can see which
expressions resolve to which subset.

## How a model gets its tags

**TL;DR — if a tag is derivable from the model's name or schema, don't
add it manually.** bollhav auto-assembles tags from several sources.
You only need to pass `Tags(tags={...})` for tags that can't be
inferred.

Every model's tag set is **assembled** from several sources. You rarely
need to list every tag by hand — most come for free from the model's
name and schema.

For a model with `target=Target(name="sales_orders", schema=TargetSchema(name="warehouse_sales"))`
and `tagging=Tags(tags={"sales"})`, the assembled tag set is:

| SourceTable | Tag contributed | Controlled by |
|---|---|---|
| You passed it explicitly | `sales` | `Tags(tags={...})` |
| The table name | `sales_orders` | `name_add_to_tags` (default `True`) |
| The schema name | `warehouse_sales` | `schema_add_to_tags` (default `True`) |
| The `"all"` convenience tag | `all` | `model_gets_all_tag` (default `True`) |
| The fully-qualified name | `warehouse_sales.sales_orders` | `full_name_add_to_tags` (default `True`) |
| Snake-split of the **name** | `sales`, `orders` | `unsnake_name_for_tags` (default `True`) |
| Snake-split of the **schema** | `warehouse`, `sales` | `unsnake_schema_for_tags` (default `True`) |
| Pascal-split of the **name** | *(off)* | `unpascal_name_for_tags` (default `False`) |
| Pascal-split of the **schema** | *(off)* | `unpascal_schema_for_tags` (default `False`) |

Result: `{all, orders, sales, sales_orders, warehouse, warehouse_sales, warehouse_sales.sales_orders}`

This means you can write `TAGS="[sales]"` and it matches **anything
named or schema'd `sales_*`** without ever having to add `"sales"` to
`Tags(tags={...})` — the snake-splitter derives it from
`warehouse_sales.sales_orders` on your behalf.

### When do you actually need to add a tag manually?

Only when the tag can **not** be derived from the name or schema. Common
reasons:

- **Cross-cutting domains** — a `forecast` tag that groups `sales_forecasts`
  and `finance_budgets` has nothing to do with either name or either
  schema, so it must be listed explicitly on both.
- **Classification** — tags like `pii`, `critical`, `deprecated` aren't
  baked into the table name, so they're passed in explicitly.
- **Plural/singular mismatch** — `sales_forecasts` unsnake-splits to
  `forecasts` (plural). If you want `[forecast]` (singular) to match,
  add it by hand.

If you catch yourself writing `Tags(tags={"sales"})` on a model already
called `sales_orders` in schema `warehouse_sales` — delete it. It's a
no-op.

### Snake-case vs PascalCase

By default only snake-case splitting is on, since most warehouses use
snake_case. If your environment uses PascalCase (common in some BI tools
and CDC pipelines), opt into the Pascal splitters:

```python
Model(
    target=Target(name="CustomerJourney", schema=TargetSchema(name="DataMart")),
    tagging=Tags(unpascal_name_for_tags=True, unpascal_schema_for_tags=True),
)
# auto-tags include: customer, journey, data, mart
```

Snake-splitting a PascalCase name does nothing (no underscores to split
on) and vice versa — they are independent, so you can turn on whichever
matches your naming convention. See
[pascalcase.py](src/models/pascalcase.py) for a live example in this
folder.

### Turning auto-tags off

If you want full control, every contributor can be disabled:

```python
Tags(
    tags={"sales"},
    name_add_to_tags=False,
    schema_add_to_tags=False,
    model_gets_all_tag=False,
    full_name_add_to_tags=False,
    unsnake_name_for_tags=False,
    unsnake_schema_for_tags=False,
)
# only "sales" is on the model, nothing else.
```

## Models in this example

Note which models pass `Tags(tags=...)` and which don't — most of them
get away with none.

| Model | Naming | Explicit tags | Notable auto-tags |
|---|---|---|---|
| `warehouse_sales.sales_orders` | snake_case | *(none — all auto)* | `sales`, `orders`, `warehouse`, `warehouse_sales` |
| `warehouse_sales.sales_forecasts` | snake_case | `forecast` (cross-cutting) | `sales`, `forecasts`, `warehouse`, `warehouse_sales` |
| `warehouse_finance.finance_ledger` | snake_case | *(none — all auto)* | `finance`, `ledger`, `warehouse`, `warehouse_finance` |
| `warehouse_finance.finance_budgets` | snake_case | `forecast` (cross-cutting) | `finance`, `budgets`, `warehouse`, `warehouse_finance` |
| `warehouse_raw.raw_events` | snake_case | *(none — all auto)* | `raw`, `events`, `warehouse`, `warehouse_raw` |
| `DataMart.CustomerJourney` | PascalCase | *(none — all auto)* | `customer`, `journey`, `data`, `mart` (with both `unpascal_*` flags on) |

Every model also gets `all` and its own fully-qualified name like
`warehouse_sales.sales_orders`.

## Run it

From the repo root:

```bash
TAGS="[all]" USE_SCHEMA_SUFFIX=false python examples/tag_matching/main.py
```

## Expressions to try

| `TAGS` value | Selects | Why |
|---|---|---|
| `[all]` | every model | `all` is auto-added to all models |
| `[sales]` | `sales_orders`, `sales_forecasts` | snake-split of schema AND name contributes `sales` |
| `[finance]` | `finance_ledger`, `finance_budgets` | snake-split contributes `finance` |
| `[sales\|finance]` | all four sales/finance models | OR inside a group |
| `[sales & forecast]` | `sales_forecasts` only | AND — must match both |
| `[forecast & not:sales]` | `finance_budgets` only | AND with exclusion |
| `[raw][sales]` | `raw_events` + both sales models | two groups are OR'd together |
| `[warehouse]` | every snake_case model | `warehouse` comes from snake-splitting every schema |
| `[customer]` | `CustomerJourney` only | pascal-split of the name — only that model opts in |
| `[mart]` | `CustomerJourney` only | pascal-split of the schema `DataMart` |
| `[all & not:warehouse_raw]` | everything except `raw_events` | exclude by schema auto-tag |
| `[r:sales]` | both sales models, marked for reload | `r:` prefix sets `directives.reload=True` |
| `[reload:sales]` | same as `[r:sales]` | `reload` is a full-word alias for `r` |
| `[r_row_100:sales]` | sales models, reload in ROW mode, 100 rows/chunk | forces `ChunkMode.ROW` + `batch_size=100` at runtime (requires `WriteMode.APPEND` or `UPSERT_NO_DELETE`) |
| `[r_interval_@daily:sales]` | sales models, reload in INTERVAL mode, daily chunks | forces `ReloadMode.INTERVAL` + `interval_expression="@daily"` at runtime |
| `r_interval_@hourly:[sales\|finance]` | both domains, reload hourly | group-level — applies to every tag in the group |

## What the output looks like

Running with `TAGS="[forecast & not:sales]"`:

```
TAGS = '[forecast & not:sales]'
Matched 1 model(s):

  - warehouse_finance.finance_budgets
      auto-tags: all, budgets, finance, finance_budgets, forecast, warehouse, warehouse_finance, warehouse_finance.finance_budgets
```

See [bollhav/docs/TAGS.md](../../bollhav/docs/TAGS.md) for the full tag
expression grammar and [bollhav/model/tags.py](../../bollhav/model/tags.py)
for the `Tags` dataclass fields.
