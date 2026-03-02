from __future__ import annotations

from .columns import PostgresColumn


def build_ddl(columns: list[PostgresColumn]) -> str:
    cols = sorted(columns, key=lambda c: (c.order is None, c.order))
    col_defs: list[str] = []
    for col in cols:
        type_str: str = col.data_type.value
        if col.length is not None:
            type_str += f"({col.length})"
        elif col.precision is not None and col.scale is not None:
            type_str += f"({col.precision}, {col.scale})"
        elif col.precision is not None:
            type_str += f"({col.precision})"
        parts: list[str] = [type_str]
        if not col.nullable:
            parts.append("NOT NULL")
        if col.primary_key:
            parts.append("PRIMARY KEY")
        if col.unique and not col.primary_key:
            parts.append("UNIQUE")
        col_defs.append(f'"{col.name}" {" ".join(parts)}')
    return ", ".join(col_defs)


def get_pk_columns(columns: list[PostgresColumn]) -> list[str]:
    return [col.name for col in columns if col.primary_key]
