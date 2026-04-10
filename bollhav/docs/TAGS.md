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
| `[all & not:debug]` | has `all` but NOT `debug` |
| `[not:debug]` | does NOT have `debug` |
| `[foo & not:(bar\|baz)]` | has `foo` but NOT `bar` and NOT `baz` |
| `not:[foo & bar]` | does NOT have both `foo` AND `bar` |

Tag-level `not:` (inside the brackets) negates that specific condition. Group-level `not:` (outside the brackets) negates the entire group — the group matches when the inner expression does NOT match.

### Reload flag (`r:`)

Prefix `r:` to mark matched models for reload:

| Syntax | Meaning |
|--------|---------|
| `[r:foo]` | match `foo`, reload |
| `[r:foo & bar]` | match `foo` AND `bar`, reload |
| `[r:(foo\|bar)]` | match `foo` OR `bar`, reload |
| `r:[foo & bar]` | match `foo` AND `bar`, reload for all |

Group-level `r:` (outside the brackets) applies to all tags inside. Tag-level `r:` applies only to that tag.

### Combining `r:` and `not:`

The prefixes can be combined:

| Syntax | Meaning |
|--------|---------|
| `[r:sales & not:debug]` | match `sales`, exclude `debug`, reload matched |
| `r:not:[foo]` | match everything without `foo`, reload all |

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
- **Negation is explicit.** `[all & not:debug]` says exactly what it means. Regex negation (`^(?!.*debug).*$`) is easy to get wrong and hard to read at a glance.
- **Fewer mistakes.** Regex has footguns everywhere — unescaped dots, greedy quantifiers, anchoring issues. Tag expressions have a small surface area: tags, `&`, `|`, `not:`, `r:`, and brackets. If it parses, it does what you expect.
- **Environment-variable friendly.** Tag expressions are short, readable strings that work well as `TAGS=...` values. Complex regex patterns with special characters are awkward to pass through shell environments and easy to break with quoting issues.
