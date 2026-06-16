FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml app.py seed.py seed_dev.py ./
RUN pip install --no-cache-dir .

# Overridden by compose to point at the `db` service.
ENV BOLLHAV_STATE_DSN=postgresql://postgres:postgres@db:5432/postgres

EXPOSE 8137

# Seed a demo DAG (SEED=1, default), then serve the lineage API. Set SEED=0
# to skip seeding — REQUIRED when pointing BOLLHAV_STATE_DSN at a real state
# DB, since seed.py drops & rebuilds the z_bollhav schema before populating it.
ENV SEED=1
CMD ["sh", "-c", "[ \"$SEED\" = 0 ] || python seed.py; exec uvicorn app:app --host 0.0.0.0 --port 8137"]
