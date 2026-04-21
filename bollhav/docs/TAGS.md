[back to README](../../README.md)

# Tags

A model is included if it matches **any** group. Groups are OR'd together.

| Syntax | Meaning |
|--------|---------|
| `[foo]` | has tag `foo` |
| `[foo & bar]` | has `foo` AND `bar` |
| `[foo\|bar]` | has `foo` OR `bar` |
| `[(foo\|bar) & baz]` | has (`foo` OR `bar`) AND `baz` |
| `[foo][bar]` | has `foo` OR `bar` (separate groups) |

### Negation (`not:`)

Prefix `not:` to exclude models:

| Syntax | Meaning |
|--------|---------|
| `[all & not:foo]` | has `all` but NOT `foo` |
| `[not:foo]` | does NOT have `foo` |
| `[foo & not:(bar\|baz)]` | has `foo` but NOT `bar` and NOT `baz` |
| `not:[foo & bar]` | does NOT have both `foo` AND `bar` |

Tag-level `not:` (inside the brackets) negates that specific condition. Group-level `not:` (outside the brackets) negates the entire group — the group matches when the inner expression does NOT match.

### Reload flag (`r:` or `reload:`)

Prefix `r:` (or the full-word alias `reload:`) to mark matched models for reload:

| Syntax | Meaning |
|--------|---------|
| `[r:foo]` | match `foo`, reload |
| `[reload:foo]` | same as `[r:foo]` — `reload` is a full-word alias for `r` |
| `[r:foo & bar]` | match `foo` AND `bar`, reload |
| `[r:(foo\|bar)]` | match `foo` OR `bar`, reload |
| `r:[foo & bar]` | match `foo` AND `bar`, reload for all |

Group-level `r:` (outside the brackets) applies to all tags inside. Tag-level `r:` applies only to that tag. `reload` works as a drop-in replacement for `r` in every form — pick whichever reads better for you.

### Controlling how reload chunks its work

Plain `r:` reloads using whatever the model is statically configured with (the `reload_mode` / `reload_batch_size` fields on the model's `Batch`, defaulting to `INTERVAL` + the model's own `interval_expression`). Two extended prefixes let you override that *at match time* without touching the model code:

| Prefix | Forces | Notes |
|--------|--------|-------|
| `r_row_<N>:` | ROW-mode reload, `batch_size=<N>` | `<N>` is the row count per chunk. Capped at 10000. Compatible with `WriteMode.APPEND` and `WriteMode.UPSERT_NO_DELETE`. |
| `r_interval_<@alias>:` | INTERVAL-mode reload, `interval_expression=<alias>` | `<alias>` is one of `@minutely`/`@minute`, `@hourly`/`@hour`, `@daily`/`@day`, `@weekly`/`@week`, `@monthly`/`@month` (sourced from roskarl). |

For a cadence that doesn't have a named alias, set it statically on the model (`Batch(interval_expression="*/15 * * * *")`) and reload with plain `r:`, or override globally with the `BATCH_EXPRESSION_OVERRIDE` env var — arbitrary cron expressions are intentionally not accepted inside tags.

Both extended prefixes accept the `reload_` long form (`reload_row_100:`, `reload_interval_@daily:`), and both work at tag-level and group-level:

| Syntax | Meaning |
|--------|---------|
| `[r_row_100:vPAS]` | reload `vPAS` in ROW mode, 100 rows/chunk |
| `reload_row_500:[foo & bar]` | group-level — ROW mode, 500 rows/chunk for both |
| `[r_interval_@daily:sales]` | reload `sales` in INTERVAL mode, one chunk per day |
| `r_interval_@hourly:[facts]` | group-level — hourly chunks for every matched model |
| `[r_row_100:foo][r_interval_@daily:bar]` | mix — `foo` in ROW/100, `bar` in INTERVAL/@daily |

Runtime trumps the model's own `reloading`, so the same model can be run ROW one day and INTERVAL the next without code changes. Validation fires at parse/match time — an unknown cron alias or a row-batch over the cap raises immediately.

### Combining `r:` and `not:`

The prefixes can be combined:

| Syntax | Meaning |
|--------|---------|
| `[r:sales & not:foo]` | match `sales`, exclude `foo`, reload matched |
| `r:not:[foo]` | match everything without `foo`, reload all |
| `r_row_100:not:[views]` | reload everything except `views` in ROW mode |

## Usage

```python
for model, reload in match_models(folder="src/models", tags="[r:sales & finance]"):
    if reload:
        interval = (model.bounds.begin, model.bounds.end)
    else:
        interval = ...  # use your default incremental interval
```

Each result is a `(model, reload)` tuple. `reload` is `True` if the matched expression included `r:`.

## Why not regex?

Tag expressions exist because regex is a poor fit for model selection:

- **Reload is a first-class concept.** The `r:` prefix ties reload intent directly to the selection. With regex you would need a second mechanism (a separate flag, a naming convention, a wrapper) to express "match these models *and* reload them."
- **Multiple groups are trivial.** `[sales][finance]` reads as "sales or finance." The regex equivalent (`sales|finance`) looks simple in this case, but combining OR across groups with AND within groups gets unwieldy fast — `(?=.*foo)(?=.*bar)|baz` is not something you want in an environment variable.
- **Negation is explicit.** `[all & not:foo]` says exactly what it means. Regex negation (`^(?!.*foo).*$`) is easy to get wrong and hard to read at a glance.
- **Fewer mistakes.** Regex has footguns everywhere — unescaped dots, greedy quantifiers, anchoring issues. Tag expressions have a small surface area: tags, `&`, `|`, `not:`, `r:`, and brackets. If it parses, it does what you expect.
- **Environment-variable friendly.** Tag expressions are short, readable strings that work well as `TAGS=...` values. Complex regex patterns with special characters are awkward to pass through shell environments and easy to break with quoting issues.
