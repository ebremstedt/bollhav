[Home](index.md) › [Tags](TAGS.md) › **Advanced tag prefixes**

# Advanced tag prefixes

!!! note "TODO"
    This page needs a proper rewrite — the content below was lifted from the original TAGS.md and reads more like reference dump than an explainer. Restructure as a guided walkthrough of when to reach for these prefixes and what trade-offs they imply.

## Reloading

Tags select models and optionally flag them for reload. Chunking config (the interval expression and row `size`) lives on the model's `Batch`, **not** in tags — there's a single reload prefix:

| Prefix | Meaning |
|--------|---------|
| `r:` | reload the matched model(s) |
| `reload:` | full-word alias for `r:` |

To change the interval cadence for one run without editing the model, use the `INTERVAL_EXPRESSION_OVERRIDE` env var. Chunk `size` is model config — adjust it on the `Batch`.

## Combining `r:` and `not:`

The prefixes can be combined, at tag-level and group-level:

| Syntax | Meaning |
|--------|---------|
| `[r:sales & not:foo]` | match `sales`, exclude `foo`, reload matched |
| `r:not:[foo]` | match everything without `foo`, reload all |
| `reload:[foo & bar]` | group-level reload for both |
