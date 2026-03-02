import psycopg


def create_view(
    conn: psycopg.Connection,
    schema: str,
    name: str,
    query: str,
) -> None:
    with conn.transaction():
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        conn.execute(f'CREATE OR REPLACE VIEW {schema}."{name}" AS {query}')
