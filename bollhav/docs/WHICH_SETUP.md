[back to README](../../README.md) — [model reference](MODEL.md) — [write modes](WRITEMODES.md) — [which write mode?](WHICH_TABLE_MODE.md)

# Which setup?

Pick a use case, follow the questions, land on the exact `Target` / `WriteMode` fields to set on the `Model` — plus the actions needed to make the write mode work correctly.

```mermaid
flowchart TD
    Start([What's your use case?]):::root

    Start --> UC1[same schema +<br/>reload completely every run]:::usecase
    Start --> UC2[Same schema<br/>only latest data each run]:::usecase
    Start --> UC3[changed schema +<br/>reload completely every run]:::usecase

    UC1 --> UC1Start(["Target(truncate_table=True)"]):::target
    UC1Start --> UC1Q{Need row<br/>uniqueness?}:::question
    UC1Q -->|Yes, by partition| UC1YP(["WriteMode.RECREATE_PARTITION"]):::writemode
    UC1YP --> UC1YPPB(["Set partition_on=True on a column"]):::uniqueness
    UC1YP --> UC1YPC(["Mark the unique column(s)<br/>with unique=True to create constraint"]):::uniqueness
    UC1YP --> UC1YPR(["Deduplicate rows at read or transform time"]):::deduplication
    UC1Q -->|Yes, by unique key| UC1YU(["WriteMode.UPSERT_NO_DELETE"]):::writemode
    UC1YU --> UC1YUK(["Mark the upsert key column(s) with unique=True"]):::uniqueness
    UC1Q -->|No| UC1N(["WriteMode.APPEND"]):::writemode

    UC2 --> UC2Start(["Target(truncate_table=False*)"]):::target
    UC2Start --> UC2Read(["Read: filter at the source<br/>to the latest slice only"]):::config
    UC2Read --> UC2H{Keep history<br/>of past runs?}:::question
    UC2H -->|Yes, append each slice| UC2HY(["WriteMode.APPEND<br/><i>good for raw / event</i>"]):::writemode
    UC2H -->|No, overwrite the slice| UC2HN{Need row<br/>uniqueness?}:::question
    UC2HN -->|Yes, by unique key| UC2HNY(["WriteMode.UPSERT_NO_DELETE"]):::writemode
    UC2HNY --> UC2HNYK(["Mark the upsert key column(s) with unique=True"]):::uniqueness
    UC2HN -->|Yes, by partition| UC2HNP(["WriteMode.RECREATE_PARTITION"]):::writemode
    UC2HNP --> UC2HNPPB(["Set partition_on=True on a column"]):::uniqueness
    UC2HNP --> UC2HNPC(["Mark the unique column(s)<br/>with unique=True to create constraint"]):::uniqueness
    UC2HNP --> UC2HNPR(["Deduplicate rows at read or transform time"]):::deduplication

    UC3 --> UC3Start(["Target(recreate_table=True)"]):::target

    classDef root fill:#1e293b,stroke:#0f172a,color:#fff
    classDef usecase fill:#64748b,stroke:#334155,color:#fff
    classDef question fill:#f59e0b,stroke:#d97706,color:#fff
    classDef config fill:#10b981,stroke:#059669,color:#fff
    classDef target fill:#f97316,stroke:#c2410c,color:#fff
    classDef writemode fill:#8b5cf6,stroke:#6d28d9,color:#fff
    classDef uniqueness fill:#ec4899,stroke:#be185d,color:#fff
    classDef deduplication fill:#0ea5e9,stroke:#0369a1,color:#fff
```

\* Default — you can omit it entirely. `truncate_table` and `recreate_table` both default to `False`; setting them explicitly just makes the intent visible.

## Chunking

Chunking is orthogonal to the write mode — any of the write modes above can be chunked, or not.

```mermaid
flowchart TD
    ChunkQ([Chunk the work?]):::root

    ChunkQ -->|No, run once| ChunkNone(["Leave batching unset<br/><i>Model(batching=None)<br/>reads everything in one shot</i>"]):::config
    ChunkQ -->|Yes| ChunkHow{How to chunk?}:::question
    ChunkHow -->|Time-based| ChunkI(["Batch(mode=ChunkMode.INTERVAL)<br/><i>works in every run mode</i>"]):::config
    ChunkHow -->|Row-count-based| ChunkR(["Batch(mode=ChunkMode.ROW)<br/><i>reload-only</i>"]):::config

    classDef root fill:#1e293b,stroke:#0f172a,color:#fff
    classDef question fill:#f59e0b,stroke:#d97706,color:#fff
    classDef config fill:#10b981,stroke:#059669,color:#fff
```
