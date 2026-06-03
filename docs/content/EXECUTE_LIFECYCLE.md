[← home](index.md)

# Execute lifecycle

`@execute_lifecycle` brackets **one unit of work** — one call to your execute, inside the model loop. Read, transform, write; the decorator handles state and staging around it.

```python
@execute_lifecycle
def run_interval(model, interval, data_conn, state_conn=None):
    rows = read(model, interval)
    write(model, rows, conn=data_conn, interval=interval)
```

`interval` is a window for an `interval` model, or `None` for `monolithic` / `view`.

## Two switches

Two independent flags decide what wraps the execute — `model.stateful` and `target.stage`:

| stateful | stage | what runs |
|:---:|:---:|---|
| no | no | the execute, directly |
| no | yes | staged execute, no state |
| yes | no | state machine around the direct execute |
| yes | yes | state machine around the staged execute |

## State machine

When stateful, each unit runs through:

1. **Gate** — already `applied`? skip.
2. **Lock** — take the per-interval advisory lock; if another worker holds it, skip.
3. **Contracts** — check every [upstream contract](UPSTREAM.md). Any unsatisfied → mark `blocked` (reason names the missing upstreams), skip.
4. **Run** — mark `running` → execute → mark `applied`.
5. On error → record the failure (status `error` + a row in the `_errors` table) and re-raise.
6. **Release** the lock (always).

## Staged execute

When `target.stage` (interval-only): create a fresh staging table → execute writes into it → apply to target in one transaction → drop it. With state, the apply and the `applied` flip commit together. See [Staging](STAGING.md).

## See also

- [Model lifecycle](MODEL_LIFECYCLE.md) — the per-model setup around the loop.
- [State](STATE.md) · [Upstream & contracts](UPSTREAM.md) · [Staging](STAGING.md)
