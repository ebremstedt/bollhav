# bollhav-gui

A small web app that visualizes [bollhav](https://github.com/ebremstedt/bollhav)
lineage — the cross-pipeline model graph stored in the `z_bollhav` library.

A **FastAPI** backend reads the lineage out of Postgres (via bollhav's own
query functions) and serves it as JSON; a **Svelte Flow** frontend fetches
that JSON and draws the graph — models coloured by kind, sources by kind,
upstream vs source edges, dark mode, pan/zoom/minimap.

## How it's put together

```mermaid
flowchart LR
    FE["Svelte Flow UI (frontend/)"]
    API["FastAPI (backend/app.py)"]
    REG["bollhav.postgres.registry"]
    DB[("Postgres: z_bollhav.library + errors")]

    FE -->|"GET /graph /lineage /tree"| API
    API -->|calls| REG
    REG -->|SELECT| DB
    DB -.->|rows| REG
    REG -.->|dicts| API
    API -.->|JSON| FE
```

Solid arrows = the request path; dashed = data flowing back. The key point:
**all SQL/schema knowledge lives in bollhav** (`bollhav.postgres.registry`).
The backend is a thin HTTP adapter over those functions, and the frontend
holds no schema knowledge at all — it just renders `/graph`.

## Quick start (Docker) — one command

Spins up Postgres, seeds a realistic `raw → clean → consume` demo DAG (with
run history + a few errors), starts the API, and serves the graph UI:

```bash
docker compose up --build
```

Then open **http://localhost:53173**. Other URLs:

| URL | what |
|---|---|
| http://localhost:53173 | the Svelte Flow lineage graph |
| http://localhost:58137/docs | the API (Swagger) |
| http://localhost:58137/graph | the raw graph JSON |

Three services (see `docker-compose.yml`): `db` (Postgres), `backend`
(FastAPI; installs `bollhav==3.0.0rc18` from PyPI and runs `seed.py` on
start), `frontend` (Vite dev server, proxies the API over the compose
network). Stop with `Ctrl-C`; `docker compose down -v` to also drop the DB
volume. Re-running `up` re-seeds (the seed wipes `z_bollhav` first).

To re-seed without a restart: `docker compose exec backend python seed.py`.

> Host ports are deliberately rare to avoid collisions — UI `53173`, API
> `58137`, Postgres `55432` — all mapped to the normal ports inside the
> containers. Nothing local needs to be free, and Docker runs its own
> Postgres (nothing local required).

## Layout

```
bollhav-gui/
├── docker-compose.yml    # db + backend + frontend (one command)
├── backend/
│   ├── app.py            # FastAPI: /graph /lineage /tree /state /errors /downstreams /models + viz
│   ├── seed.py           # registers a demo DAG (+ runs/errors) into z_bollhav
│   ├── pyproject.toml    # fastapi, uvicorn, psycopg, bollhav==3.0.0rc18
│   └── Dockerfile
└── frontend/             # Vite + Svelte + @xyflow/svelte (Svelte Flow)
    ├── Dockerfile
    └── src/
        ├── App.svelte         # fetch /graph -> dagre layout -> <SvelteFlow> + legend + panel
        ├── LineageNode.svelte # custom model/source card
        └── selection.svelte.js
```

## Run it manually (no Docker)

Prerequisites: a reachable **Postgres** (set `BOLLHAV_STATE_DSN`, default
`postgresql://postgres:postgres@localhost:5432/postgres`) and **Node**.
`pyproject.toml` pins `bollhav==3.0.0rc18` from PyPI, so a plain
`pip install .` in `backend/` is enough — no sibling checkout needed.

```bash
# 1. backend (serves the lineage JSON on :8137)
cd backend
pip install .
python seed.py                      # populate the demo DAG
uvicorn app:app --port 8137

# 2. frontend (Svelte Flow UI on :5173, proxies the JSON to :8137)
cd frontend
npm install
npm run dev
```

Then open `http://127.0.0.1:5173`. The backend's own quick visualization is
also at `http://127.0.0.1:8137/`, and the API docs at
`http://127.0.0.1:8137/docs`.
