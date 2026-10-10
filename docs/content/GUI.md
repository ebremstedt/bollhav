[Home](index.md) › **GUI**

# GUI

A small web app that visualizes bollhav **lineage** — the cross-pipeline model graph stored in the central `z_bollhav` library, with live state and errors. A **FastAPI** backend reads it out of Postgres via bollhav's own query functions (`bollhav.postgres.registry`) and serves JSON; a **Svelte Flow** frontend draws the graph in the browser — managed models vs unmanaged sources, per-model status lights, upstream **contracts** (and freshness) on the dependency arrows, plus name search and a **tag-expression** filter.

It lives in the repo under [`gui/`](https://github.com/ebremstedt/bollhav/tree/main/gui).

## What you see

The **detail level** is a toggle in the header (the 🎄) — **lappland** is a bare graph (just boxes, names, arrows); **stockholm** turns on every decoration.

![GUI — lappland (bare)](GUI_lappland.png)

![GUI — stockholm (full detail)](GUI_stockholm.png)

- **Managed models** have a solid yellow outline with a `model` pill and a `table` / `view` materialization pill. **Unmanaged sources** have a dashed outline coloured by kind (`model` / `api` / `file` / `hardcoded`) with an `unmanaged` pill.
- **Status lights** (top-right of a box): error (red), running (green), **blocked** (orange — an upstream hasn't produced the data yet), and **stale** (blue — an upstream is present but too old for a freshness contract).
- **The dependency arrows** are labelled (in stockholm) with the upstream **contract**: the completeness level (`exists` / `exact` / `encapsulate` / `through` / `whole`) and any **❄ freshness** bound.
- **Search** by model name, or **filter by tag / tag expression** in the second box — e.g. `[clean & fact]`, `[(customer|order) & fact]`, `[consumption & not:view]` (or a bare `clean`). Matched models stay — with their upstreams — and the matching part of each name turns green. The **🏷 tag syntax** button in the legend explains the expression syntax. (Matching reuses [`bollhav.model.tagexpr`](TAGS.md), so it behaves exactly like `TAGS=` run selection — but case-insensitively.)

## End to end — from pipelines to the browser

How a model travels from the code that defines it, into the library and state tables, out through the API, and onto the screen — and how the [TUI](TUI.md) drives the same pipelines:

```mermaid
flowchart TD
    TUI["bollhav TUI"] --> PIPE
    PIPE["Pipelines (main.py + @load_models)"] --> MODELS
    MODELS["Models to ModelRuns (matched by TAGS)"] --> LC
    LC["Lifecycle hooks (@model_lifecycle)"] -->|register + state + errors| DB
    DB[("z_bollhav: library + state + errors")] -->|read-only SELECT| API
    API["GUI backend (FastAPI / registry)"] -->|graph JSON| FE
    FE["Svelte Flow frontend"] --> BROWSER["Browser: lineage graph"]
```

**Reading it:**

1. **Pipelines define models.** Each pipeline's `main.py` is wrapped by `@load_models`, which discovers `Model`s (their `Target`, `Temporality`, `State`, and upstream contracts), matches them by `TAGS`, and hands back `ModelRun`s with the run window resolved.
2. **Running them writes the bookkeeping.** The lifecycle hooks register each model into the shared `library`, seed and flip its **state** rows (`pending → running → applied / blocked`) as units of work run, and log any failure to the shared `errors` table — all in the one central `z_bollhav` schema. State is also read *back* at the start of a run to skip already-`applied` units.
3. **The TUI drives the same pipelines.** It just runs the nearest `main.py` with the env you pick — so a TUI-triggered run flows through the identical lifecycle into the same library/state. (See [TUI](TUI.md).)
4. **The API reads, read-only.** The FastAPI backend is a thin adapter over `bollhav.postgres.registry` — every endpoint is one `SELECT` against `library` / state / `errors`. It never writes.
5. **The frontend presents it.** Svelte Flow fetches `/graph` (and the per-model endpoints on click) and renders the DAG — managed models vs unmanaged sources, status lights, and contract/freshness on the dependency arrows.

The key property: **all SQL/schema knowledge lives in bollhav** (`bollhav.postgres.registry`). The backend holds no SQL; the frontend holds no schema knowledge — it just renders what `/graph` returns.

## Get started locally

**Against your own state DB** — the GUI ships with bollhav behind the `gui` extra. One process serves the API and the UI:

```bash
pip install 'bollhav[gui]'
BOLLHAV_STATE_DSN=postgresql://user:pass@host:5432/db bollhav-gui    # then open http://localhost:8137
```

One `BOLLHAV_STATE_DSN_<NAME>` per database instead lists them as **catalogs** in the header. `LINEAGE_READ_ONLY=1` switches the state-reset controls off. All settings are in the [`gui/` README](https://github.com/ebremstedt/bollhav/tree/main/gui).

**See the demo** — brings its own Postgres and seeds a realistic `raw → clean → consume` DAG (with run history and a few errors). Nothing to set up:

```bash
cd gui
docker compose up --build      # then open http://localhost:53173
```

**Point it at your own state DB** — set the connection and disable seeding. `SEED=0` is required: without it the demo seed drops and rebuilds the `z_bollhav` schema and would destroy your real lineage:

```bash
cd gui
BOLLHAV_STATE_DSN=postgresql://user:pass@host:5432/db SEED=0 docker compose up --build
```

The GUI is read-only over your data, and the **environment switcher** in the header lists every `z_bollhav[_suffix]` library schema it finds in that database — so prod and any dev / PR envs are all browsable.

| URL | What |
|---|---|
| http://localhost:53173 | the lineage graph UI |
| http://localhost:58137/docs | the API (Swagger) |
| http://localhost:58137/graph | the raw graph JSON |

??? note "Useful one-liners"
    - Stop: `Ctrl-C`. Drop the demo DB too: `docker compose down -v`.
    - Re-seed the demo: `docker compose exec backend python seed.py`
    - Add a second demo env to toggle between: `docker compose exec backend python seed_dev.py`
    - Ports are deliberately rare (UI `53173`, API `58137`, Postgres `55432`) so nothing local needs to be free.

### Developing the GUI

From a checkout: the backend with reload, the frontend with Vite's hot reload proxying the API to it.

```bash
pip install -e '.[gui]'
BOLLHAV_STATE_DSN=... uvicorn bollhav.gui.serve:create_app --factory --port 8137

cd gui/frontend
npm ci
npm run dev               # http://127.0.0.1:5173
```

`npm run build` writes the bundle into the Python package (`bollhav/gui/static/`); the release workflow does that before building the wheel, so a published `bollhav[gui]` always carries the frontend that matches it.

## Endpoints

Every endpoint is a thin wrapper over `bollhav.postgres.state.read` (and `write` for the one reset). All take `?catalog=` and most `?env=`; `/docs` on the running app is the full list.

| Endpoint | Returns |
|---|---|
| `GET /config`, `GET /catalogs`, `GET /environments` | what the header needs: title, default catalog, the catalogs and the library schemas in one |
| `GET /graph` | the whole DAG — nodes (models + sources) and edges, for the canvas |
| `GET /match?expr=` | full names matching a tag expression (drives the tag filter) |
| `GET /models`, `GET /model/{full_name}` | every registered model; one model's metadata |
| `GET /lineage/{full_name}`, `GET /tree/{full_name}`, `GET /downstreams/{full_name}` | direct upstreams, the recursive upstream tree, who depends on it |
| `GET /state/{full_name}`, `GET /runs`, `GET /grid`, `GET /gaps` | state rows for a model; recent runs, per-model run history and backfill gaps across models |
| `GET /errors` | recent rows from the shared `errors` table |
| `GET /freshness`, `POST /refresh`, `POST /refresh/{full_name}` | when the catalog's cache was computed; recompute it, or one model |
| `POST /state/{full_name}/reset` | the one write: flip rows back to `pending` (off with `LINEAGE_READ_ONLY`) |

## Relation to the TUI

The [TUI](TUI.md) and the GUI are complementary: the **TUI runs** pipelines (it triggers `main.py` with a chosen env and streams the output), while the **GUI visualizes** what those runs have recorded (lineage, state, errors). Both work against the same central `z_bollhav` schema — the TUI through a normal pipeline run, the GUI read-only through `bollhav.postgres.registry`.
