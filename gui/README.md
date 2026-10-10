# bollhav GUI

A web app that shows the model graph with live state, runs, errors and gaps,
and can reset state so the next run redoes it. The Python side is the
`bollhav.gui` package (a FastAPI app reading the state databases through
`bollhav.postgres.state`); the frontend is the Svelte app in [frontend/](frontend/),
built into `bollhav/gui/static/` and shipped inside the wheel

## Run it

```bash
pip install 'bollhav[gui]'
BOLLHAV_STATE_DSN=postgresql://user:pass@host:5432/db bollhav-gui    # http://localhost:8137
```

One process serves the API and the UI. `LINEAGE_READ_ONLY=1` switches the
reset controls off for a deployment that should only look

**The demo** (brings its own Postgres and seeds a `raw → clean → consume` DAG
with run history and errors), from this folder:

```bash
docker compose up --build      # then open http://localhost:53173
```

`BOLLHAV_STATE_DSN=... SEED=0 docker compose up --build` points the demo stack
at a real state DB instead. `SEED=0` matters: the seed drops and rebuilds
`z_bollhav` first.

## Settings

All by environment variable.

| Variable | What |
|---|---|
| `BOLLHAV_STATE_DSN` | the state database (one catalog, called `default`) |
| `BOLLHAV_STATE_DSN_<NAME>` | one per catalog instead; the header's switcher lists them by `name` |
| `EXPLORER_DSN`, `EXPLORER_DSN_<NAME>` | aliases for the two above, for deployments that use those names |
| `LINEAGE_DEFAULT_CATALOG` | the catalog read when the URL names none (else the first by name) |
| `LINEAGE_REFRESH_SECONDS` | how often each catalog's prod library is precomputed (default 300; 0 = no cache, everything live) |
| `LINEAGE_READ_ONLY` | `1` disables `POST /state/{name}/reset` and hides the controls |
| `LINEAGE_TITLE`, `LINEAGE_DEFAULT_TAGS` | header title; a tag expression to open with instead of one random model |
| `BOLLHAV_GUI_HOST`, `BOLLHAV_GUI_PORT` | where `bollhav-gui` listens (default `0.0.0.0:8137`) |
| `STATIC_DIR` | serve the frontend from here instead of the bundle in the package |

A pipeline keeps its state and library in the database it writes to, so every
database holds a library of its own; a **catalog** is one such database and
the switch next to it in the header picks the environment (prod `z_bollhav`
or a suffixed dev run) inside it.

## The cache

A catalog's prod library is computed in the background every
`LINEAGE_REFRESH_SECONDS` and served from memory, so big catalogs open at
once; the header and footer say when it was computed. The header's ⟳ loads
the newest precompute or starts one, the ⟳ on a model recomputes that model
alone, right away, as does a state reset. Dev environments are small and
always read live.

Budget roughly 100 MB per thousand models with a year of daily runs each, and
twice that for a moment while a recompute replaces the previous snapshot. Reads
run in autocommit mode so a big library does not exhaust Postgres's lock slots
inside one transaction.

## Resetting state

The one write path. From the **Grid** tab, clicking a model's name opens its
panel on the left (reset the whole model, or every interval in a typed range)
and clicking cells opens the intervals' panel on the right (shift-click a
range, ⌘ / ctrl-click to add). The **Lineage** tab's two panels carry the same
controls. Every action asks first.

A reset flips the chosen rows `applied` → `pending` so the next run redoes
them (a flexible model's coverage is uncovered instead); rows and history are
kept and a `running` row is never touched. It is `bollhav.postgres.state.write`
behind `POST /state/{full_name}/reset` with `{"all": true}`,
`{"intervals": [{"since", "until"}, …]}` or `{"range": {"since", "until"}}`,
plus `?env=` / `?catalog=`.

## Developing the GUI

Backend with reload, frontend with Vite's hot reload proxying the API:

```bash
pip install -e '.[gui]'                                   # from the repo root
BOLLHAV_STATE_DSN=... uvicorn bollhav.gui.serve:create_app --factory --port 8137

cd gui/frontend
npm ci
npm run dev                                               # http://127.0.0.1:5173
```

`npm run build` writes the bundle to `bollhav/gui/static/` (gitignored); the
release workflow does that before building the wheel, so a published
`bollhav[gui]` always carries the frontend that matches it. The API docs are at
`/docs`.

| Path | What |
|---|---|
| `bollhav/gui/app.py` | the API: every route is a query against the selected catalog |
| `bollhav/gui/serve.py` | `create_app`: env aliases, `/healthz`, the SPA |
| `bollhav/gui/__main__.py` | `bollhav-gui`: uvicorn |
| `gui/frontend/` | the Svelte source |
| `gui/backend/` | the demo image and its seed scripts |
