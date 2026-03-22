[back to README](..README.md)

## Levels of Abstraction

Naming levels of abstractions with well thought-out words means it becomes easier for humans **to reason about data engineering**.

```mermaid
flowchart TD
    Pipeline([Pipeline]):::pipeline
    PipeA([Pipe]):::pipe
    PipeB([Pipe]):::pipe
    PipeC([Pipe]):::pipe
    ModelA([Model]):::model
    ModelB([Model]):::model
    ModelC([Model]):::model
    DataA[(Data)]:::data
    DataB[(Data)]:::data
    DataC[(Data)]:::data

    Pipeline e1@-->|schedules & orders| PipeA
    Pipeline e2@-->|schedules & orders| PipeB
    Pipeline e3@-->|schedules & orders| PipeC
    PipeA e4@-->|contains one or more| ModelA
    PipeB e5@-->|contains one or more| ModelB
    PipeC e6@-->|contains one or more| ModelC
    ModelA e7@-->|instructs to write| DataA
    ModelB e8@-->|instructs to write| DataB
    ModelC e9@-->|instructs to write| DataC

    e1@{ animation: fast }
    e2@{ animation: fast }
    e3@{ animation: fast }
    e4@{ animation: fast }
    e5@{ animation: fast }
    e6@{ animation: fast }
    e7@{ animation: fast }
    e8@{ animation: fast }
    e9@{ animation: fast }

    classDef pipeline fill:#6366f1,stroke:#4338ca,color:#fff
    classDef pipe fill:#0ea5e9,stroke:#0284c7,color:#fff
    classDef model fill:#10b981,stroke:#059669,color:#fff
    classDef data fill:#f59e0b,stroke:#d97706,color:#fff
```

### **Model level**
defines what data looks like — schema, columns, types, unique keys, and constraints. Keeping this separate means any pipe can reuse the same model definition without duplicating that logic.

### **Pipe level**
defines how and when a model runs — environment variables, cron schedules, backfill ranges, and schema suffixes. Separating this from the model means you can run the same model in different contexts (backfill, latest, dev, prod) without changing the model itself.

### **Pipeline level**
(*not in bollhav*) is the orchestrator level — it decides in what order and when pipes run, handling dependencies and scheduling across multiple pipes.
