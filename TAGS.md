# Tag Expression Filter

Discovers and returns `execute` functions from Python modules in a folder recursively, filtered by a tag expression.

---

## Tag Expression Syntax

Tags are matched against the `tags` attribute on each module's `model` object.

### Structure
```
[group1],[group2],[group3]
```

A model is included if it matches **any** group (outer OR).

---

### Group Types

| Syntax | Meaning |
|--------|---------|
| `[wee]` | has tag `wee` |
| `[wee\|x]` | has `wee` OR `x` |
| `[xyz&abc]` | has `xyz` AND `abc` |
| `[xyz&(c\|e)]` | has `xyz` AND (`c` OR `e`) |
| `[wee\|x],[xyz&(c\|e)]` | matches first OR second group |

---


---

### Examples
```bash
# Match models tagged "wee" or "x"
export TAGS="[wee|x]"

# Match models tagged "xyz" AND ("c" OR "e")
export TAGS="[xyz&(c|e)]"

# Combined
export TAGS="[wee|x],[xyz&(c|e)]"
```

---

## Limitations

- Square brackets are required around every group
- Only one level of parentheses is supported
- `&` and `|` cannot be mixed at the top level without parentheses

---

## Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `folder` | `str` | `src/models` | Path to folder containing model modules |
| `tags` | `str` | required | Tag filter expression string |

**Returns** `list[Callable]` — execute functions from matched modules.

**Raises** `ValueError` — if `tags` is not provided or the expression is invalid.