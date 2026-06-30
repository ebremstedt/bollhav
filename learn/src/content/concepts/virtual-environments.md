---
title: "Virtual environments 🏖️"
body: "Use a schema suffix so dev runs don't overwrite production"
---

Setting `SCHEMA_SUFFIX` gives a run its own isolated environment: every schema it touches — the target tables, state, library, errors, and staging — gets the suffix appended, turning `z_bollhav` into something like `z_bollhav_pr123`. A development or CI run then writes into that sandbox instead of overwriting the production models. Without a suffix, a dev run would point straight at the prod schemas — exactly what you don't want. The suffix can also carry a timestamp appendix, so the whole environment is ephemeral and easy to tear down once you're done.
