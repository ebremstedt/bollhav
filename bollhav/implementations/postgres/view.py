from __future__ import annotations

import psycopg


def create_view(
    conn: psycopg.Connection,
    schema: str,
    view_name: str,
    source_query: str,
) -> None:
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    conn.execute(f'CREATE OR REPLACE VIEW {schema}."{view_name}" AS {source_query}')
