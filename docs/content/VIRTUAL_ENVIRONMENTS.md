[Home](index.md) › [Articles](ARTICLES.md) › **Virtual environments**

# Virtual environments

!!! note "TODO — to be written"
    Placeholder. Ideas to cover:

    - One venv per pipeline (or repo); pin `bollhav` + your connectors (`psycopg` for Postgres, `pyodbc` for MSSQL) + `polars`.
    - When a venv is enough (pure-Python Postgres) vs. when you need Docker (MSSQL needs the system **ODBC Driver 18**, not pip-installable — see the `examples/mssql/staging` Dockerfile).
    - `requirements.txt` for reproducibility — same env locally, in CI, and in the prod image.
    - Keeping the state/target DB separate (a throwaway Docker Postgres).

## See also

- [Testing locally](LOCAL_TESTING.md) · [Implementations](POSTGRES.md)
