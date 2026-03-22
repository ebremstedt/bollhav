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

### Reload flag (`!`)

Prefix `!` to mark matched models for reload:

| Syntax | Meaning |
|--------|---------|
| `[!foo]` | match `foo`, reload |
| `[!foo & bar]` | match `foo` AND `bar`, reload |
| `[!(foo\|bar)]` | match `foo` OR `bar`, reload |
| `![foo & bar]` | match `foo` AND `bar`, reload for all |

Group-level `!` (outside the brackets) applies to all tags inside. Tag-level `!` applies only to that tag.


## Usage

```python
for model, reload in match_models(folder="src/models", tags="[!sales & finance]"):
    if reload:
        interval = (model.bounds.begin, model.bounds.end)
    else:
        interval = ...  # use your default incremental interval
```

Each result is a `(model, reload)` tuple. `reload` is `True` if the matched expression included `!`.
