FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml app.py seed.py ./
RUN pip install --no-cache-dir .

# Overridden by compose to point at the `db` service.
ENV BOLLHAV_STATE_DSN=postgresql://postgres:postgres@db:5432/postgres

EXPOSE 8137

# Seed a demo DAG, then serve the lineage API.
CMD ["sh", "-c", "python seed.py && uvicorn app:app --host 0.0.0.0 --port 8137"]
