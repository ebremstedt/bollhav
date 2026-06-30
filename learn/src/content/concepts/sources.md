---
title: "Sources 📥"
body: "Where data enters from outside the framework"
---

Sources are the edges of the system, where data comes in from somewhere bollhav doesn't manage: `SourceModel` (a raw table), `SourceFile`, `SourceApi`, and `SourceHardcoded` (inline rows or SQL). Unlike managed upstreams, they aren't state-tracked — so nothing gates on them.
