[Home](index.md) › **Tags**

# Tags

Tags are sets of labels attached to models. Some are derived automatically from the model's properties, and some are added explicitly by the user. At runtime, `TAGS=<expression>` filters which models run.

## Syntax

A tag expression is one or more bracket-delimited **groups**. A model matches if ANY group matches.

The simplest expression — one tag in one group:

| Syntax | Meaning |
|--------|---------|
| `[foo]` | has tag `foo` |

### AND (`&`)

Match when all listed tags are present in the group.

| Syntax | Meaning |
|--------|---------|
| `[foo & bar]` | has `foo` AND `bar` |

### OR (`|`)

Match when at least one of the listed tags is present. Use parentheses to combine with `&`.

| Syntax | Meaning |
|--------|---------|
| <code>[foo&#124;bar]</code> | has `foo` OR `bar` |
| <code>[(foo&#124;bar) & baz]</code> | has (`foo` OR `bar`) AND `baz` |

### Groups (`[a][b]`)

Multiple bracket groups are OR'd at the top level — handy when each side has its own internal logic.

| Syntax | Meaning |
|--------|---------|
| `[foo][bar]` | has `foo` OR `bar` |
| `[clean & not:views][dirty & sales]` | (`clean` and NOT `views`) OR (`dirty` and `sales`) |

### NOT (`not:`)

Prefix `not:` to exclude. Tag-level `not:` (inside the brackets) negates that one condition; group-level `not:` (outside) negates the whole group.

| Syntax | Meaning |
|--------|---------|
| `[not:foo]` | does NOT have `foo` |
| `[all & not:foo]` | has `all` but NOT `foo` |
| <code>[foo & not:(bar&#124;baz)]</code> | has `foo` but NOT `bar` and NOT `baz` |
| `not:[foo & bar]` | does NOT have both `foo` AND `bar` |

### Reload (`r:` or `reload:`)

Prefix `r:` (or the long-form alias `reload:`) to mark matched models for reload. Tag-level applies only to that tag; group-level applies to all tags in the group.

| Syntax | Meaning |
|--------|---------|
| `[r:foo]` | match `foo`, reload |
| `[reload:foo]` | same as `[r:foo]` — `reload` is the long-form alias |
| `[r:foo & bar]` | match `foo` AND `bar`, reload |
| <code>[r:(foo&#124;bar)]</code> | match `foo` OR `bar`, reload |
| `r:[foo & bar]` | match `foo` AND `bar`, reload all |

For combining `r:` with `not:` and group-level reload, see [Advanced tag prefixes](ADVANCED_TAGS.md). Chunking config (interval, row `size`) lives on the model's `Batch`, not in tags.



## Why not regex instead of this shite?

Tag expressions exist because regex is a poor fit for model selection:

- **Reload is a first-class concept.** The `r:` prefix ties reload intent directly to the selection. With regex you would need a second mechanism (a separate flag, a naming convention, a wrapper) to express "match these models *and* reload them."
- **Multiple groups are trivial.** `[sales][finance]` reads as "sales or finance." The regex equivalent (`sales|finance`) looks simple in this case, but combining OR across groups with AND within groups gets unwieldy fast — `(?=.*foo)(?=.*bar)|baz` is not something you want in an environment variable.
- **Negation is explicit.** `[all & not:foo]` says exactly what it means. Regex negation (`^(?!.*foo).*$`) is easy to get wrong and hard to read at a glance.
- **Fewer mistakes.** Regex has footguns everywhere — unescaped dots, greedy quantifiers, anchoring issues. Tag expressions have a small surface area: tags, `&`, `|`, `not:`, `r:`, and brackets. If it parses, it does what you expect.
- **Environment-variable friendly.** Tag expressions are short, readable strings that work well as `TAGS=...` values. Complex regex patterns with special characters are awkward to pass through shell environments and easy to break with quoting issues.
