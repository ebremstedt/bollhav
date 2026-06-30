---
title: "Tags 🏷️"
body: "Selecting which models run using tag expressions"
---

`TAGS` is a small expression language for picking models. `[a & b]` means both, separate groups `[a][b]` mean either, `not:` excludes, and `r:` marks a model for reload. Every model's own name counts as a tag, so `[FactCase]` selects exactly that one model.
