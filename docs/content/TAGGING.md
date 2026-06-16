[Home](index.md) › [Model](MODEL.md) › **Tagging**

# Tagging

The `Tags` config object passed as `Model(tagging=...)`. Controls which auto-derived tags get added to a model, alongside any explicit tags you supply. For tag-expression *syntax* used at runtime to select models, see [Tags](TAGS.md) — this page is about the per-model config that builds the tag set in the first place.

## tags

Type: `set[str]` · Default: `set()`

Explicit tags you add yourself. Merged with all the auto-derived tags below.

## name_add_to_tags

Type: `bool` · Default: `True`

Add the target's `name` as a tag.

## schema_add_to_tags

Type: `bool` · Default: `True`

Add the target's `schema.name` as a tag.

## catalog_add_to_tags

Type: `bool` · Default: `True`

Add the target's `catalog` as a tag, when `catalog` is set.

## model_gets_all_tag

Type: `bool` · Default: `True`

Add the literal tag `"all"` — useful as a wildcard match.

## full_name_add_to_tags

Type: `bool` · Default: `True`

Add `schema.name` (e.g. `public.orders`) as a tag.

## fully_qualified_name_add_to_tags

Type: `bool` · Default: `True`

Add `catalog.schema.name` (e.g. `warehouse.public.orders`) as a tag — only fires when `catalog` is set.

## unsnake_name_for_tags

Type: `bool` · Default: `True`

Split snake_case `name` on `_` and add each piece as a tag (e.g. `paid_signups` → `paid`, `signups`).

## unsnake_schema_for_tags

Type: `bool` · Default: `True`

Same as above for `schema.name`.

## unsnake_catalog_for_tags

Type: `bool` · Default: `True`

Same as above for `catalog`.

## unpascal_name_for_tags

Type: `bool` · Default: `False`

Split PascalCase `name` and add each lowercased piece (e.g. `PaidSignups` → `paid`, `signups`). Off by default — flip on for PascalCase identifiers.

## unpascal_schema_for_tags

Type: `bool` · Default: `False`

Same as above for `schema.name`.

## unpascal_catalog_for_tags

Type: `bool` · Default: `False`

Same as above for `catalog`.
