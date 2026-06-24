"""User-facing message text, centralized so call sites stay free of prose.

Three submodules, split by severity / mechanism:

- `error`   — typed exceptions (raised, fatal); each owns its message.
- `warning` — `warn_*(logger, ...)` emitters for non-fatal `logger.warning`s.
- `info`    — `info_*(logger, ...)` emitters for routine `logger.info` lines.

The error classes are re-exported here for convenience; the warning/info
emitters are imported from their submodules directly (`messages.warning`,
`messages.info`)."""

from __future__ import annotations

from bollhav.model.messages.error import *  # noqa: F401,F403
from bollhav.model.messages.error import __all__ as _error_all

__all__ = list(_error_all)
