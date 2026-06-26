---
title: "Virtual environments"
body: "One env per pipeline."
---

Pin `bollhav` plus your connectors (`psycopg` for Postgres, `pyodbc` for MSSQL) and `polars`, captured in `requirements.txt` so local, CI, and the prod image match. A venv is enough for pure-Python Postgres; MSSQL needs the system ODBC Driver 18 (not pip-installable), so reach for Docker there. Keep the state/target DB separate — a throwaway Docker Postgres.
