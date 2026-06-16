[Home](index.md) › **Examples** › **Tags**

# Tags from a name

bollhav derives a model's tags from its **identity** — the catalog, schema, and
name — so `TAGS=` run selection and lineage filtering work without you
hand-labelling anything. `snake_case` is split on underscores; turn on the
`unpascal_*` flags and `PascalCase` is split into its words too (lowercased).

Take a model whose target is
`AnalyticsLakehouse.curated_clean_entities.WeeklyChannelAttributionRollupFact`:

```python title="weekly_rollup.py"
from bollhav.model import Database, Model, Tags, Target, Temporality, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType


weekly_rollup = Model(
    temporality=Temporality.TEMPORAL,
    target=Target(
        name="WeeklyChannelAttributionRollupFact",
        schema="curated_clean_entities",
        catalog="AnalyticsLakehouse",
        database=Database.POSTGRES,
        write_mode=WriteMode.APPEND,
        columns=[
            PostgresColumn("week", PostgresType.TIMESTAMPTZ, nullable=False),
            PostgresColumn("channel", PostgresType.TEXT, nullable=False),
            PostgresColumn("attributed_revenue", PostgresType.NUMERIC, nullable=False),
        ],
    ),
    tagging=Tags(
        unpascal_name_for_tags=True,     # default: False
        unpascal_schema_for_tags=True,   # default: False
        unpascal_catalog_for_tags=True,  # default: False
    ),
)
```

That single declaration produces this tag set:

```text
all

AnalyticsLakehouse                  →  analytics, lakehouse          (catalog, un-pascal'd)
curated_clean_entities              →  curated, clean, entities      (schema, un-snake'd)
WeeklyChannelAttributionRollupFact  →  weekly, channel, attribution, rollup, fact   (name, un-pascal'd)

curated_clean_entities.WeeklyChannelAttributionRollupFact
AnalyticsLakehouse.curated_clean_entities.WeeklyChannelAttributionRollupFact
```

## What gets added

| Rule | From the example |
|---|---|
| the bare catalog / schema / name | `AnalyticsLakehouse`, `curated_clean_entities`, `WeeklyChannelAttributionRollupFact` |
| `unsnake_*` — split on `_` | `curated`, `clean`, `entities` |
| `unpascal_*` — split `PascalCase` (lowercased) | `analytics`, `lakehouse`, `weekly`, `channel`, `attribution`, `rollup`, `fact` |
| the schema-qualified name | `curated_clean_entities.WeeklyChannelAttributionRollupFact` |
| the fully-qualified name | `AnalyticsLakehouse.curated_clean_entities.WeeklyChannelAttributionRollupFact` |
| `all` (every model gets it) | `all` |

Any explicit `tags={...}` you pass are merged in on top. Defaults: the bare
names, `unsnake_*`, the qualified names, and `all` are **on**; `unpascal_*` is
**off** (enable it for PascalCase names like above).

Now `TAGS="[clean & fact]"` selects this model — and so does the **tag filter**
in the [GUI](GUI.md), which uses the very same matching. See
[Tags](TAGS.md) for the expression syntax and [Tagging](TAGGING.md) for the
full set of `Tags` options.
