# The demo backend: the bollhav GUI installed from this checkout, plus the
# seed scripts. Build context is the repo root (compose sets it):
#   docker build -f gui/backend/Dockerfile -t bollhav-gui .
FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml README.md ./
COPY bollhav ./bollhav
RUN pip install --no-cache-dir ".[gui]"

COPY gui/backend/seed.py gui/backend/seed_dev.py gui/backend/seed_errors.py gui/backend/seed_runs_extra.py ./

# Overridden by compose to point at the `db` service.
ENV BOLLHAV_STATE_DSN=postgresql://postgres:postgres@db:5432/postgres

EXPOSE 8137

# Seed a demo DAG (SEED=1, default), then serve the GUI. Set SEED=0 to skip
# seeding — REQUIRED when pointing BOLLHAV_STATE_DSN at a real state DB, since
# seed.py drops & rebuilds the z_bollhav schema before populating it.
ENV SEED=1
CMD ["sh", "-c", "[ \"$SEED\" = 0 ] || python seed.py; exec bollhav-gui"]
