[Home](index.md) › **Write modes**

# Write modes

Describes how data is written to the target table or view. This is just a naming convention, you can use the implementations in this library, or build your own for your own needs.

## APPEND

Simply appends incoming rows to the target. No deduplication, no deletes.

```mermaid
flowchart LR
    A[Incoming rows] e1@--> B[INSERT INTO target]
    B e2@--> C[Target table]

    e1@{ animation: fast }
    e2@{ animation: fast }

    style A fill:#4A90D9,stroke:#2C5F8A,color:#fff
    style B fill:#5BA85A,stroke:#3A7A39,color:#fff
    style C fill:#2C3E50,stroke:#1a252f,color:#fff
```

### When to use:
- When the resulting size does not matter
- We want to save rows immediately (as to not break our pipeline, like in raw)
- Duplicates are okay

## Pre-load flags: `recreate_table` and `truncate_table`

Two booleans on `Target` that compose with any write mode. They run **once** in `ensure_table`, before the chunked write loop — so they're safe to combine with row-chunked or interval-chunked batching without clobbering earlier chunks.

- `recreate_table=True` → `DROP TABLE IF EXISTS` then `CREATE TABLE`. Resets schema. Use when column definitions have changed.
- `truncate_table=True` → `CREATE TABLE IF NOT EXISTS` then `TRUNCATE TABLE`. Wipes rows, keeps schema. Use when you want a full reload but the schema is stable.

Both default to `False`. Setting both is an error. Neither applies to a view (views are not a write mode).

```mermaid
flowchart LR
    A[Incoming rows] --> B{recreate_table?}
    B -- Yes --> C[DROP + CREATE TABLE]
    B -- No --> D{truncate_table?}
    D -- Yes --> E[CREATE IF NOT EXISTS + TRUNCATE]
    D -- No --> F[CREATE IF NOT EXISTS]
    C --> G[chunked write loop]
    E --> G
    F --> G

    style A fill:#4A90D9,stroke:#2C5F8A,color:#fff
    style B fill:#E67E22,stroke:#A04000,color:#fff
    style C fill:#C0392B,stroke:#922B21,color:#fff
    style D fill:#E67E22,stroke:#A04000,color:#fff
    style E fill:#C0392B,stroke:#922B21,color:#fff
    style F fill:#5BA85A,stroke:#3A7A39,color:#fff
    style G fill:#2C3E50,stroke:#1a252f,color:#fff
```

Typical combinations:
- `APPEND` + `truncate_table=True` → idempotent full reload
- `APPEND` + `recreate_table=True` → idempotent full reload with schema reset
- `UPSERT_NO_DELETE` + `truncate_table=True` → wipe then upsert (useful when the incoming data has duplicates)

## UPSERT_NO_DELETE

Updates rows that already exist, inserts rows that don't. Never deletes.


```mermaid
flowchart LR
    A[Incoming rows] --> B{Row exists in target?}
    B -- Yes --> C[UPDATE existing row]
    B -- No --> D[INSERT new row]
    C --> E[Target table]
    D --> E

    style A fill:#4A90D9,stroke:#2C5F8A,color:#fff
    style B fill:#E67E22,stroke:#A04000,color:#fff
    style C fill:#8E44AD,stroke:#6C3483,color:#fff
    style D fill:#5BA85A,stroke:#3A7A39,color:#fff
    style E fill:#2C3E50,stroke:#1a252f,color:#fff
```


### When to use:
- This is much like a MERGE statement, where deduplication is handled.
- Duplicates are not okay
- When we want to make sure that deletes from source are _NOT_ handled
- This relies on a unique key constraint
- NOT IDEMPOTENT

## RECREATE_PARTITION

Deletes matching rows first, then inserts all incoming rows fresh. Effectively a targeted replace.


```mermaid
flowchart LR
    A[Incoming rows] --> B[DELETE matching rows from target]
    B --> C[INSERT all incoming rows]
    C --> D[Target table]

    style A fill:#4A90D9,stroke:#2C5F8A,color:#fff
    style B fill:#C0392B,stroke:#922B21,color:#fff
    style C fill:#5BA85A,stroke:#3A7A39,color:#fff
    style D fill:#2C3E50,stroke:#1a252f,color:#fff
```

### When to use:
- This is much like a MERGE statement, where deduplication is handled.
- Duplicates are not okay
- When we want to make sure that deletes from source _ARE_ handled
- IDEMPOTENT



---

## MERGE

Full sync — updates existing, inserts new, and deletes rows that are no longer in the source. Requires Postgres 15+.

### When to use:
- Wait until postgres 15


```mermaid
flowchart LR
    A[Incoming rows] --> B{Row exists in target?}
    B -- Yes --> C[UPDATE existing row]
    B -- No --> D[INSERT new row]
    C --> E{Row in target but not in source?}
    D --> E
    E -- Yes --> F[DELETE row]
    E -- No --> G[Target table]
    F --> G

    style A fill:#4A90D9,stroke:#2C5F8A,color:#fff
    style B fill:#E67E22,stroke:#A04000,color:#fff
    style C fill:#8E44AD,stroke:#6C3483,color:#fff
    style D fill:#5BA85A,stroke:#3A7A39,color:#fff
    style E fill:#E67E22,stroke:#A04000,color:#fff
    style F fill:#C0392B,stroke:#922B21,color:#fff
    style G fill:#2C3E50,stroke:#1a252f,color:#fff
```

## Views

A view is **not** a write mode — it's `materialization=Materialization.VIEW` on the model (timeless, or temporal with a `Contract` range it covers). It writes no data; the lifecycle runs `CREATE OR REPLACE VIEW` from the model's defining `query` (its SELECT body).

```mermaid
flowchart LR
    A[source_query] --> B[CREATE OR REPLACE VIEW]
    B --> C[View definition updated in place]

    style A fill:#4A90D9,stroke:#2C5F8A,color:#fff
    style B fill:#16A085,stroke:#0E6655,color:#fff
    style C fill:#2C3E50,stroke:#1a252f,color:#fff
```

### When to use:
- Simple renaming of columns
- Useful for segregating data
- Does not persist data, which means the result is calculated on each use




## Summary

| Mode | Inserts | Updates | Deletes | Notes |
|---|---|---|---|---|
| `APPEND` | ✅ | ❌ | ❌ | No deduplication. Pair with `truncate_table=True` or `recreate_table=True` for full reloads |
| `UPSERT_NO_DELETE` | ✅ | ✅ | ❌ | Safe upsert |
| `RECREATE_PARTITION` | ✅ | ✅ | ✅ | Deletes matches first |
| `MERGE` | ✅ | ✅ | ✅ | Requires Postgres 15+ |

A **view** is not a write mode — see [Views](#views); set `materialization=Materialization.VIEW` instead.

### Pre-load flags (on `Target`)

| Flag | Effect before load |
|---|---|
| `recreate_table=True` | `DROP` + `CREATE TABLE` (schema reset) |
| `truncate_table=True` | `CREATE IF NOT EXISTS` + `TRUNCATE TABLE` |