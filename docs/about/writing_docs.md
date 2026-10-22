# Writing docs

How to write a bollhav doc page. The goal: a **data engineer** lands here mid-task and gets what they need fast. Optimize for that reader — concise, concrete, skimmable.

## 1. A file in its folder is a page

The site builds itself from `docs/`: every `.md` file is a page at its path, titled by its first heading, and the sidebar has one group per folder, with the folder's `README.md` as its overview. Nothing is registered anywhere. Put a page where it belongs in the conceptual hierarchy (what contains what):

```
docs/
├─ README.md               home
├─ model/                  the Model and its sub-objects
│  ├─ README.md            overview
│  ├─ target.md, contract.md, batch.md, temporality.md, state.md, upstream.md, …
├─ concepts/               env, decorators, tags, gui
├─ backends.md             postgres, mssql, iceberg, in short
├─ examples/
├─ articles/               README.md is the index
└─ about/
```

Filenames are lowercase. Link to another page by its relative path (`[Target](target.md)`, `[State](../model/state.md)`); the build fails on a link that points at nothing.

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

- **Code references**: link to the file/line — `[state.py:1153](../../bollhav/postgres/state.py#L1153)` (relative from the page's folder under `docs/`). The site turns it into a GitHub link.
- **Related concepts**: a `## See also` footer with 2–4 links to sibling/child pages.
- Inline-link the first mention of another concept (`[contracts](../model/upstream.md)`), not every mention.

## 6. Diagrams earn their place

Use a `mermaid` flowchart only when control flow or ordering is the point (the staging per-interval flow, the state machine). Don't diagram a list.

## 7. One concept per file

A page covers one object/concept. If you're documenting two, split them and link. Deep dives live on the concept's own page; other pages link to it rather than re-explaining.

## 8. Keep code minimal and runnable

Examples show the smallest model/snippet that makes the point. Omit imports/columns that aren't load-bearing (`...`). If it can run, make sure it would.

## Checklist

- [ ] The file sits in the right folder, lowercase name
- [ ] First two sentences = what + when
- [ ] Tables for any enumeration of options/fields/statuses
- [ ] Code refs and related concepts linked
- [ ] No filler words; every sentence pulls weight
- [ ] Links to other pages are relative paths that resolve
