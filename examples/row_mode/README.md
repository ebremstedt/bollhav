# row_mode

A self-contained pipeline that mixes a **`Batch(mode=ChunkMode.ROW)`**
model and a normal `Batch(mode=ChunkMode.INTERVAL)` model in one run, so
you can see both chunking strategies dispatched side-by-side. See
[CHUNKING.md](../../docs/content/CHUNKING.md) for when ROW
is the right call.

## Models in this example

| Model | Mode | Why |
|---|---|---|
| `warehouse_raw.event_stream` | `ROW` (`batch_size=250`) | Append-only stream with no useful time column — chunks by row count. ROW models can only be reloaded. |
| `warehouse_clean.customer_summary` | `INTERVAL` (`@hourly`) | Standard time-sliced summary — runs in latest mode picking the most recent complete hour. |

## Structure

```
row_mode/
  src/
    models/
      event_stream.py       # ROW-mode model
      customer_summary.py   # INTERVAL-mode model
  main.py                   # entry point — branches on model.batching.mode
  execute.py                # @progress_bar-wrapped per-chunk handler
  mock_read.py              # returns 1000 fake rows
  mock_write.py             # prints chunk-write actions
```

## Setup

```bash
pip install bollhav polars
```

## Run it (mixed: one ROW model, one INTERVAL model)

Two run commands, same pipeline — different verbosity. Both use the
same tag expression:

- `[r:events]` — matches the ROW-mode `event_stream` (it has the `events`
  tag) and flips it into reload mode (required for ROW models).
- `[customers]` — matches the INTERVAL-mode `customer_summary` and runs
  it in latest mode (the default, since `LATEST_ENABLED=true`).

### Terse: `PROGRESS_BAR=model`

One finish-line per model with the mode label in parentheses, plus a
final all-done summary. No per-chunk noise.

```bash
TAGS="[r:events][customers]" USE_SCHEMA_SUFFIX=false \
  LATEST_ENABLED=true PROGRESS_BAR=model \
  python examples/row_mode/main.py
```

Output:

```
▸ warehouse_clean.customer_summary (interval)  256ms 1/1 á  255ms
▸ warehouse_raw.event_stream (rows)             1.0s 4/4 á  254ms
✓ 2 models done   1.3s
```

`▸` marks each model as a completed sub-step; `✓` is the final
all-done. The `(rows)` / `(interval)` label is read from
`model.batching.mode` — since `apply_runtime_overrides` bakes tag-driven
reload overrides into `batching` before the progress bar reads it, the
label automatically reflects `r_row_<N>:` / `r_interval_<@alias>:`
overrides.

### Verbose: `PROGRESS_BAR=batch`

Same pipeline, but every chunk is printed too — useful when you want
to see exactly what's being written.

```bash
TAGS="[r:events][customers]" USE_SCHEMA_SUFFIX=false \
  LATEST_ENABLED=true PROGRESS_BAR=batch \
  python examples/row_mode/main.py
```

Output:

```
  wrote chunk  1/1 (1000 rows) to warehouse_clean.customer_summary
▸ warehouse_clean.customer_summary (interval)  256ms 1/1 á  255ms
  wrote chunk  1/4 ( 250 rows) to warehouse_raw.event_stream
  wrote chunk  2/4 ( 250 rows) to warehouse_raw.event_stream
  wrote chunk  3/4 ( 250 rows) to warehouse_raw.event_stream
  wrote chunk  4/4 ( 250 rows) to warehouse_raw.event_stream
▸ warehouse_raw.event_stream (rows)             1.0s 4/4 á  254ms
✓ 2 models done   1.3s
```

Per-chunk lines only render at `PROGRESS_BAR=batch` — `mock_write.py`
checks the level and skips printing at `model`/`minimal`. (Real
pipelines would do similar with logging level.)

### Override row batch size at runtime

```bash
TAGS="[r_row_100:events][customers]" USE_SCHEMA_SUFFIX=false \
  LATEST_ENABLED=true PROGRESS_BAR=batch \
  python examples/row_mode/main.py
```

Now `event_stream` runs ROW with `batch_size=100` (10 chunks instead
of 4) — the runtime override takes precedence over the static
`row.batch_size=250`.

## Single-model variants

Just the ROW model:

```bash
TAGS="[r:events]" USE_SCHEMA_SUFFIX=false python examples/row_mode/main.py
```

Just the INTERVAL model:

```bash
TAGS="[customers]" USE_SCHEMA_SUFFIX=false LATEST_ENABLED=true \
  python examples/row_mode/main.py
```

## Failure path

`event_stream` is ROW-mode and can only be reloaded. Without the `r:`
prefix, the dispatcher raises:

```
ValueError: Model 'warehouse_raw.event_stream' is ROW-mode — it can
only be reloaded. Re-run with the `r:` tag prefix, e.g. TAGS="[r:events]"
```

(bollhav itself raises a similar error if you read `model.intervals`
on a ROW model outside reload — the example bypasses that path and
enforces the same contract directly in its dispatch.)
