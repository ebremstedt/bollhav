# progress_bar

A focused demo of all three `PROGRESS_BAR` levels — `minimal`, `model`,
`batch`. Three INTERVAL models with backfill windows of 4, 12, and 24
hourly chunks each, plus a 150ms per-chunk sleep so you can actually see
the spinner spin in `batch` mode.

## Models

| Model | Chunks | Total runtime (with 150ms/chunk delay) |
|---|---|---|
| `warehouse_clean.quick_facts` | 4 | ~0.6s |
| `warehouse_clean.medium_facts` | 12 | ~1.8s |
| `warehouse_clean.slow_facts` | 24 | ~3.6s |

All three are `@hourly` INTERVAL models with explicit backfill bounds.

## Setup

```bash
pip install bollhav polars
```

## Run it — three verbosity levels, same pipeline

### `PROGRESS_BAR=batch` — live spinner per chunk

```bash
PROGRESS_BAR=batch TAGS="[facts]" USE_SCHEMA_SUFFIX=false \
  BACKFILL_ENABLED=true python examples/progress_bar/main.py
```

What you'll see (animated in your terminal):

```
⠋ warehouse_clean.quick_facts        ████████████░░░░░░░░ 60% 3/5  á 150ms ~150ms left
▸ warehouse_clean.quick_facts         600ms 4/4 á 150ms
⠙ warehouse_clean.medium_facts       ██████░░░░░░░░░░░░░░ 33% 4/12 á 150ms ~1.2s left
▸ warehouse_clean.medium_facts        1.8s 12/12 á 150ms
⠼ warehouse_clean.slow_facts         █████░░░░░░░░░░░░░░░ 25% 6/24 á 150ms ~2.7s left
▸ warehouse_clean.slow_facts          3.6s 24/24 á 150ms
✓ 3 models done   6.0s
```

The spinner overwrites itself in place; in CI/CD logs each frame
becomes its own line, so use `model` or `minimal` there.

### `PROGRESS_BAR=model` — one finish-line per model

```bash
PROGRESS_BAR=model TAGS="[facts]" USE_SCHEMA_SUFFIX=false \
  BACKFILL_ENABLED=true python examples/progress_bar/main.py
```

Output:

```
▸ warehouse_clean.quick_facts (interval)   600ms 4/4 á 150ms
▸ warehouse_clean.medium_facts (interval)  1.8s 12/12 á 150ms
▸ warehouse_clean.slow_facts (interval)    3.6s 24/24 á 150ms
✓ 3 models done   6.0s
```

`▸` marks each model done; `✓` is the final all-done summary.

### `PROGRESS_BAR=minimal` — only the all-done line

```bash
PROGRESS_BAR=minimal TAGS="[facts]" USE_SCHEMA_SUFFIX=false \
  BACKFILL_ENABLED=true python examples/progress_bar/main.py
```

Output:

```
✓ 3 models done   6.0s
```

Best for production / scheduled runs where you don't need per-model
visibility in logs.

## Notes

- The label next to each model name is the model's interval expression
  (e.g. `(daily)`), read from `model.batching.interval.expression`.
- See [PROGRESS_BAR.md](../../docs/content/PROGRESS_BAR.md) for the
  full level reference.
