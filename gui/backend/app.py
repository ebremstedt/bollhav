import os
import psycopg
from fastapi import FastAPI, HTTPException
from bollhav.postgres import registry
from bollhav.postgres.state import LIBRARY_SCHEMA

DSN = os.environ.get(
    "BOLLHAV_STATE_DSN", "postgresql://postgres:postgres@localhost:5432/postgres"
)

app = FastAPI(title="LINEAGE")


def _conn():
    return psycopg.connect(DSN)


# Every read endpoint takes an optional `?env=<schema>` — the bollhav library
# schema to read (prod `z_bollhav` by default, or a suffixed dev/PR env). The
# GUI's environment switcher passes it; `/environments` lists the choices.
def _schema(env: str | None) -> str:
    return env or LIBRARY_SCHEMA


@app.get("/environments")
def environments():
    """The bollhav library schemas in the connected DB (prod + suffixed envs)."""
    with _conn() as c:
        return registry.list_environments(c)


@app.get("/models")
def models(env: str | None = None):
    with _conn() as c:
        return registry.list_models(c)


@app.get("/lineage/{full_name}")
def lineage(full_name: str):
    with _conn() as c:
        result = registry.get_lineage(c, full_name)
    if result is None:
        raise HTTPException(status_code=404, detail=f"{full_name!r} is not registered")
    return result


@app.get("/tree/{full_name}")
def tree(full_name: str):
    with _conn() as c:
        result = registry.get_upstream_tree(c, full_name)
    if result is None:
        raise HTTPException(status_code=404, detail=f"{full_name!r} is not registered")
    return result


@app.get("/state/{full_name}")
def state(full_name: str, limit: int = 50, env: str | None = None):
    with _conn() as c:
        return registry.get_recent_state(c, full_name, limit=limit, schema=_schema(env))


@app.get("/downstreams/{full_name}")
def downstreams(full_name: str):
    with _conn() as c:
        return {
            "full_name": full_name,
            "downstreams": registry.get_downstreams(c, full_name),
        }


@app.get("/graph")
def graph(env: str | None = None):
    with _conn() as c:
        return registry.get_graph(c, schema=_schema(env))


@app.get("/match")
def match(expr: str, env: str | None = None):
    """Full names of models matching a bollhav tag expression (the `[group]`
    syntax), within the selected env. 400 on a malformed expression."""
    with _conn() as c:
        try:
            return {
                "expr": expr,
                "models": registry.match_tags(c, expr, schema=_schema(env)),
            }
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))


@app.get("/model/{full_name}")
def model(full_name: str, env: str | None = None):
    with _conn() as c:
        result = registry.get_model_metadata(c, full_name, schema=_schema(env))
    if result is None:
        raise HTTPException(status_code=404, detail=f"{full_name!r} is not registered")
    return result


@app.get("/errors")
def errors(full_name: str | None = None, limit: int = 100, env: str | None = None):
    with _conn() as c:
        return registry.get_errors(
            c, full_name=full_name, limit=limit, schema=_schema(env)
        )
