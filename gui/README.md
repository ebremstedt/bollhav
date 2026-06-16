# bollhav-gui

A web app that visualizes [bollhav](https://github.com/ebremstedt/bollhav)
lineage — the cross-pipeline model graph stored in the `z_bollhav` library.

## Run it

**See the demo** (brings its own Postgres + seeded data — nothing to set up):

```bash
docker compose up --build      # then open http://localhost:53173
```

**Point it at your own state DB** (`SEED=0` keeps it read-only — without it the
demo seed would drop & rebuild your `z_bollhav` schema):

```bash
BOLLHAV_STATE_DSN=postgresql://user:pass@host:5432/db SEED=0 docker compose up --build
```

That's it. The env switcher in the header then lists every `z_bollhav[_suffix]`
schema in that database (prod + any dev/PR envs).

---

Everything below is reference — you don't need it to run the app.

| URL | what |
|---|---|
| http://localhost:53173 | the lineage graph |
| http://localhost:58137/docs | the API (Swagger) |
| http://localhost:58137/graph | the raw graph JSON |

## How it's put together

```mermaid
flowchart LR
    FE["Svelte Flow UI (frontend/)"]
    API["FastAPI (backend/app.py)"]
    REG["bollhav.postgres.registry"]
    DB[("Postgres: z_bollhav.library + errors")]

    FE -->|"GET /graph /match /environments"| API
    API -->|calls| REG
    REG -->|SELECT| DB
    DB -.->|rows| REG
    REG -.->|dicts| API
    API -.->|JSON| FE
```

**All SQL/schema knowledge lives in bollhav** (`bollhav.postgres.registry`).
The backend is a thin HTTP adapter; the frontend holds no schema knowledge —
it just renders `/graph`. Three compose services: `db` (Postgres), `backend`
(FastAPI; the in-repo `bollhav` is mounted at `/src` so registry edits show up
on restart), `frontend` (Vite dev server, proxies the API).

## Run it manually (no Docker)

Prerequisites: a reachable **Postgres** (set `BOLLHAV_STATE_DSN`, default
`postgresql://postgres:postgres@localhost:5432/postgres`) and **Node**.
`gui/` lives inside the bollhav repo, so install the in-repo `bollhav`
alongside the backend's own deps (the `bollhav==3.0.0rc19` pin in
`pyproject.toml` is for the Docker image and isn't on public PyPI):

```bash
# 1. backend (serves the lineage JSON on :8137)
cd backend
pip install fastapi uvicorn "psycopg[binary]"
pip install -e ../..                # the in-repo bollhav package
python seed.py                      # populate the demo DAG (drops z_bollhav first!)
uvicorn app:app --port 8137

# 2. frontend (Svelte Flow UI on :5173, proxies the JSON to :8137)
cd frontend
npm install
npm run dev
```

Then open `http://127.0.0.1:5173`. To read a **real** state DB instead of the
demo, skip `seed.py` and point `BOLLHAV_STATE_DSN` at your database before
starting uvicorn. The API docs are at `http://127.0.0.1:8137/docs`.
