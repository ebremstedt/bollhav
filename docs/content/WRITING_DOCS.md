[Home](index.md) › **Writing docs**

# Writing docs

How to write a bollhav doc page. The goal: a **data engineer** lands here mid-task and gets what they need fast. Optimize for that reader — concise, concrete, skimmable.

## 1. Breadcrumb on line 1

Every page starts with a breadcrumb that follows the **conceptual hierarchy** (what contains what), not the nav order. Each segment links except the current page, which is **bold** and last:

```markdown
[Home](index.md) › [Model](MODEL.md) › [Target](TARGET.md) › **Staging**
```

Use ` › ` (U+203A, space on each side) as the separator. The chain mirrors how the objects nest in code — `Staging` is `target.staging`, `Target` is `model.target`, so the trail is Model → Target → Staging.

The canonical hierarchy:

```
Home
├─ Model                      MODEL.md
│  ├─ Target                  TARGET.md
│  ├─ Contract                CONTRACT.md
│  ├─ Batch                   BATCH.md
│  ├─ Kind                    KINDS.md
│  ├─ State                   STATE.md
│  ├─ Upstream                UPSTREAM.md
│  └─ Tagging                 TAGGING.md
├─ Decorators                 DECORATORS.md
├─ Concepts (Modes, Library, Chunking)
├─ Runtime (Runtime overrides, Progress bar)
├─ Tags                       TAGS.md
├─ Implementations (Postgres, MSSQL)
└─ Env · About
```

## 2. Open with what + when

The first sentence says **what it is**; the second says **when you'd reach for it**. No throat-clearing, no history. A reader should know in two lines whether this page is relevant.

> Per-model progress, tracked in a per-model state table. Opt in with `state=State(...)`; re-runs become resumable.

## 3. Voice

- Second person, present tense, active. "Set `staging=Staging()`" — not "the staging parameter can be set."
- Short. Cut every word that doesn't change the meaning. If a sentence survives deletion, delete it.
- Concrete over abstract. Name the actual field, env var, status value, SQL.
- No marketing ("powerful", "simply", "just", "seamless").

## 4. Prefer tables for enumerations

Options, fields, statuses, env vars, mode×mode matrices → tables, not bullet prose. A field table is `| Field | Default | Purpose |`. A behavior table is `| Mode | What it does |`.

## 5. Link generously

- **Code references**: link to the file/line — `[state.py:1153](../../bollhav/postgres/state.py#L1153)` (relative from `docs/content/`).
- **Related concepts**: a `## See also` footer with 2–4 links to sibling/child pages.
- Inline-link the first mention of another concept (`[contracts](UPSTREAM.md)`), not every mention.

## 6. Diagrams earn their place

Use a `mermaid` flowchart only when control flow or ordering is the point (the staging per-interval flow, the state machine). Don't diagram a list.

## 7. One concept per file

A page covers one object/concept. If you're documenting two, split them and link. Deep dives live on the concept's own page; other pages link to it rather than re-explaining.

## 8. Keep code minimal and runnable

Examples show the smallest model/snippet that makes the point. Omit imports/columns that aren't load-bearing (`...`). If it can run, make sure it would.

## Checklist

- [ ] Breadcrumb on line 1, current page bold and last
- [ ] First two sentences = what + when
- [ ] Tables for any enumeration of options/fields/statuses
- [ ] Code refs and related concepts linked
- [ ] No filler words; every sentence pulls weight
- [ ] Added to `mkdocs.yml` nav
