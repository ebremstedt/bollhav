# bollhav dashboard

Read-only, no-auth, Airflow-style live view of the bollhav state +
library + errors tables. FastAPI backend, Svelte frontend, polling
every 2s.

## Layout

```
dashboard/
  backend/    FastAPI app + DB connection (psycopg)
    main.py
    requirements.txt
  frontend/   Svelte + Vite single-page app
    src/
      App.svelte
      lib/
        ModelRow.svelte
        IntervalCell.svelte
        ErrorsPanel.svelte
        api.ts
        types.ts
    package.json
    vite.config.ts
    index.html
```

## Setup — backend

```bash
cd dashboard/backend
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Point at the same DB bollhav uses
export TARGET_DSN="postgresql://postgres:postgres@localhost:5432/postgres"

# Optional: change host/port (default 127.0.0.1:5173)
# export DASHBOARD_HOST=0.0.0.0
# export DASHBOARD_PORT=8080

python main.py
```

The backend serves:

- `/api/library` — all model_library rows
- `/api/state/<full_name>` — per-model intervals
- `/api/summary` — count rollups (pending/running/applied/blocked/error)
- `/api/errors?limit=50` — recent errors UNION across all `*_errors` tables

It also serves the built Svelte frontend at `/` once you've run `pnpm build`.

## Setup — frontend (dev)

```bash
cd dashboard/frontend
pnpm install     # or: npm install / yarn install
pnpm dev
```

Vite serves on `http://127.0.0.1:5174` and proxies `/api/*` to the
backend at `127.0.0.1:5173`. Both processes need to be running for
the dashboard to work in dev.

## Frontend production build

```bash
cd dashboard/frontend
pnpm build
```

The built static site lands in `dashboard/frontend/dist/`. The FastAPI
backend auto-mounts that directory at `/`, so a production deploy is:

```bash
cd dashboard/backend
TARGET_DSN=... python main.py
# Visit http://127.0.0.1:5173
```

## What you'll see

- **Header**: live polling indicator (`updated 1s ago` / `2s ago` /
  `backend: <error>`).
- **Legend**: color key for the five statuses.
- **Per-model rows**: name, upstream chips, count rollup, then a
  grid of interval cells. Hover any cell for date + status + run_id.
- **Running cells spin** (blue with an animated ring).
- **Recent errors** panel at the bottom, also polled.

## What's NOT in here yet

- Real-time push (currently polling every 2s). When latency matters,
  add `LISTEN/NOTIFY` in bollhav's `mark_*` / `record_failure` and an
  SSE endpoint on the backend.
- Authentication.
- Drill-down per-model detail pages.
- Errors filtering / search.

## Troubleshooting

- **`backend: 500`** — the backend hit a DB error. Check it has
  `TARGET_DSN` set. Logs print to stdout.
- **`No models in library yet`** — you haven't run a bollhav pipeline
  against this DB yet. Run one (see `examples/staging_testing/`) and
  the rows appear.
- **CORS error in browser console (dev)** — make sure Vite is on
  5174 and the backend on 5173. The proxy in `vite.config.ts` handles
  same-origin.
