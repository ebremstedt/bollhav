---
title: "Tag generation 🏭"
body: "How a model's set of tags is derived from explicit and implicit rules"
---

Beyond the explicit `tags={...}` you set, `Tags(...)` automatically derives a whole set from the model's identity, and each toggle adds more:

- the raw `name`, `schema`, and `catalog`
- `all` (the wildcard), plus the dotted `schema.name` and `catalog.schema.name`
- snake splits (`cha_clean_rst` → `cha`, `clean`, `rst`) when `unsnake_*_for_tags` is on
- PascalCase splits (`FactCase` → `fact`, `case`) when `unpascal_*_for_tags` is on

So a `FactCase` in `cha_clean_rst` becomes matchable by `[FactCase]`, `[cha_clean_rst]`, `[all]`, `[fact]`, `[case]`, and more. That's the trick behind selection: the literal name picks exactly one model, while a shared token like `[fact]` picks the whole family.
