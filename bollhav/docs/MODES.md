[back to README](..README.md)

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

## RECREATE_INSERT

Drops and recreates the table before inserting. Stronger than TRUNCATE_INSERT as it also resets the schema.

```mermaid
flowchart LR
    A[Incoming rows] e1@--> B[DROP TABLE target]
    B e2@--> C[CREATE TABLE target]
    C e3@--> D[INSERT all rows]
    D e4@--> E[Target table]

    e1@{ animation: fast }
    e2@{ animation: fast }
    e3@{ animation: fast }
    e4@{ animation: fast }

    style A fill:#4A90D9,stroke:#2C5F8A,color:#fff
    style B fill:#C0392B,stroke:#922B21,color:#fff
    style C fill:#E67E22,stroke:#A04000,color:#fff
    style D fill:#5BA85A,stroke:#3A7A39,color:#fff
    style E fill:#2C3E50,stroke:#1a252f,color:#fff
```

### When to use:
- When the table schema may have changed and needs to be realigned with the model
- When a full schema reset is preferred over a simple truncate


## TRUNCATE_INSERT

Fully reloads the table on every run. Wipes everything first, then inserts.

```mermaid
flowchart LR
    A[Incoming rows] --> B[TRUNCATE target]
    B --> C[INSERT all rows]
    C --> D[Target table]

    style A fill:#4A90D9,stroke:#2C5F8A,color:#fff
    style B fill:#C0392B,stroke:#922B21,color:#fff
    style C fill:#5BA85A,stroke:#3A7A39,color:#fff
    style D fill:#2C3E50,stroke:#1a252f,color:#fff
```

### When to use:
- When the dataset is quite small, so reloading it completely is fine
- When we want to make sure that deletes from source are included

## UPDATE_INSERT

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

## OVERWRITE_INSERT

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

## VIEW

Does not write data. Creates or replaces a view definition in place.

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
| `APPEND` | ✅ | ❌ | ❌ | No deduplication |
| `TRUNCATE_INSERT` | ✅ | ❌ | ✅ | Full reload every run |
| `UPDATE_INSERT` | ✅ | ✅ | ❌ | Safe upsert |
| `OVERWRITE_INSERT` | ✅ | ✅ | ✅ | Deletes matches first |
| `MERGE` | ✅ | ✅ | ✅ | Requires Postgres 15+ |
| `VIEW` | ❌ | ❌ | ❌ | Updates view definition only |