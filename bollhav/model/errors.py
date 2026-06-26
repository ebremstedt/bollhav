"""Shared base error families for the model runtime.

This module holds only the abstract base classes — the four error families
shared across the model package. Leaf errors now live with the code that
raises them, so an exception's `__module__` points at its origin module.

Three families plus a definition base, each with its own base:

- `RuntimeConfigError` — an invalid `@load_models` env/config combination
  (user-fixable: wrong env var, bad value). The original use of this module.
- `LifecycleError` — an internal lifecycle invariant was violated (e.g. a
  decorator called with no `run`). Signals a misuse/bug, not bad config.
- `ModelDiscoveryError` — a problem found while scanning a folder and building
  the model set (e.g. a duplicate model name, an empty tag expression).
- `ModelDefinitionError` — an invalid model/target definition caught in a
  dataclass `__post_init__` field validator.

Each leaf subclass owns its own message so the call sites stay free of error
prose — they just `raise <SpecificError>(...)`. All bases subclass
`ValueError`, so callers catching `ValueError` (and the existing tests) keep
working unchanged. Sibling of `warning.py` and `info.py` in the `messages`
package."""

from __future__ import annotations


class RuntimeConfigError(ValueError):
    """Base for an invalid combination/value of `@load_models` env vars.

    Subclasses `ValueError`, so existing `except ValueError` handlers keep
    catching every config error unchanged."""


class LifecycleError(ValueError):
    """Base for a violated model-lifecycle invariant — a misuse or internal
    bug (e.g. a lifecycle decorator invoked with no `run`), not bad config.

    Subclasses `ValueError` so existing `except ValueError` handlers keep
    catching it unchanged."""


class ModelDiscoveryError(ValueError):
    """Base for a problem found while discovering models — scanning a folder
    and building the model set (duplicate names, bad tag expression, etc.).

    Subclasses `ValueError` so existing `except ValueError` handlers keep
    catching it unchanged."""


class ModelDefinitionError(ValueError):
    """Base for an invalid model/target definition caught in a dataclass
    `__post_init__` field validator — a contradictory or incomplete set of
    fields on a `Model`, `Target`, `Contract`, `TZInterval`, `Source*`, or
    `Staging` (e.g. `recreate_table` and `truncate_table` both set, or a
    timezone-naive bound).

    Subclasses `ValueError` so existing `except ValueError` handlers keep
    catching it unchanged."""


__all__ = [
    "RuntimeConfigError",
    "LifecycleError",
    "ModelDiscoveryError",
    "ModelDefinitionError",
]
