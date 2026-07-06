---
name: tags
description: How bollhav tags work — both how a model's tags are set (explicit tags={...} plus the set auto-derived from its name/schema/catalog) and how the TAGS= expression language targets specific models ([Name] literal, [fact] shared token, [all], [a & b] AND, [a][b] OR, not: exclude, r: reload). Use when a developer is tagging models, or figuring out the TAGS selector to run exactly the models they want.
---

# bollhav — tags

Tags are how you **select which models run**. A run sets `TAGS=<expression>`;
bollhav matches it against each model's tag set. There are two halves: what
tags a model *has*, and the *expression* that picks it.

## Setting tags on a model

Configure tagging via `Tags(...)` on the model (or the `tags=` shorthand):

```python
from bollhav.model import Tags
# on the Model:
tagging=Tags(tags={"demo", "nightly"})
```

Beyond what you set explicitly, `Tags(...)` **auto-derives** a whole set from
the model's identity, so you rarely need many explicit tags:

- the raw `name`, `schema`, and `catalog`
- `all` (the wildcard), plus dotted `schema.name` and `catalog.schema.name`
- snake splits (`cha_clean_rst` → `cha`, `clean`, `rst`) when the
  `unsnake_*_for_tags` toggle is on
- PascalCase splits (`FactCase` → `fact`, `case`) when the
  `unpascal_*_for_tags` toggle is on

So a model named `FactCase` in schema `cha_clean_rst` is matchable by
`[FactCase]`, `[cha_clean_rst]`, `[all]`, `[fact]`, `[case]`, and more.

That's the whole trick: **the literal name picks exactly one model**, while a
**shared token like `[fact]` picks the whole family** of `Fact*` models.

> Virtual-environment note: when a run applies a schema suffix, the suffix is
> also added as a tag at registration (per `Tags.suffix_add_to_tags`), so runs
> stay env-neutral in selection. See the `env-vars` skill.

## Targeting with the `TAGS=` expression

`TAGS` is a small expression language:

| Selector | Meaning |
|---|---|
| `[FactCase]` | the model whose name is `FactCase` (a name is a tag) — exactly one |
| `[fact]` | every model carrying the `fact` token — the whole `Fact*` family |
| `[all]` | every model |
| `[a & b]` | **AND** — models that have *both* `a` and `b` |
| `[a][b]` | **OR** — separate groups: models in *either* group |
| `not:a` | exclude anything tagged `a` |
| `r:[tag]` | **reload** the matched models (re-run their full contract range) |

Combine them:

```bash
TAGS="[fact & nightly]"      # facts that are also tagged nightly
TAGS="[fact][dim]"           # all facts OR all dims
TAGS="[all] not:experimental"# everything except experimental models
TAGS="[FactCase]"            # just that one model
TAGS="r:[fact]"              # reload every fact over its whole contract
```

## Rules of thumb

- **Run one model** → use its literal name: `[FactCase]`.
- **Run a family** → use a shared token: `[fact]`, `[dim]`, or a schema tag.
- **Narrow a family** → AND it with another tag: `[fact & eu]`.
- **Everything** → `[all]` (optionally `not:` a few out).

Depth: `learn/src/content/concepts/tags.md` and `tag-generation.md`. To turn a
selection into a full local run command, use the `env-vars` skill.
