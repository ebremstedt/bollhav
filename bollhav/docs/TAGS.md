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
