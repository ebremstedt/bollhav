# Changelog

## [2.0.131] - 2026-05-22

### Fixed

- `TargetSchema.full_name` produced the wrong week number around year boundaries. The default `suffix_appendix` was `%y%W`, which uses Python's "Monday-starts-the-week, week 0 before the first Monday" numbering — that disagrees with ISO 8601 and produced off-by-one weeks (most visibly in early January, where ISO week 1 was rendered as week 0 or 52). Switched the default to `%y%V`, which is the ISO 8601 week number. Thanks to [@koutakis](https://github.com/koutakis) for the fix (PR #8).
- `TargetSchema.full_name` resolved the suffix timestamp against the host's local time. That meant the week-suffix could flip a day early or late depending on the runner's timezone — two pipelines on different machines could write to differently suffixed schemas for the same logical run. Now uses `datetime.now(tz=timezone.utc)`, so the suffix is timezone-stable. Also via [@koutakis](https://github.com/koutakis) in PR #8.

### Internal

- Cleaned up ruff E402 ("module level import not at top of file") warnings across the `examples/` directory. The example entry points called `os.chdir(__file__)` before importing, so the relative `src/models` lookup inside `@load_models` would resolve. Moved the `chdir` into the `if __name__ == "__main__":` block — same behaviour at runtime (it still runs before `main()` is invoked), imports now sit at the top where ruff expects them.

## [2.0.130] - 2026-05-20

### Added

- `LOOKBACK_OVERRIDE` env var and `lookback_override` parameter on `apply_runtime_overrides`, completing the runtime override set alongside `INTERVAL_EXPRESSION_OVERRIDE` and `WINDOW_EXPRESSION_OVERRIDE`. Lets a job widen every matched model's `lookback` without editing model definitions — useful for daily incrementals that need to re-check the last N intervals for late-arriving rows. Applies in latest, backfill, and reload modes. Validates non-negative at startup. See [RUNTIME_OVERRIDES.md](bollhav/docs/RUNTIME_OVERRIDES.md#lookback) for the units gotcha (it's cron-ticks of the interval expression, not calendar days).

### Documentation

- New `## Lookback` section in [RUNTIME_OVERRIDES.md](bollhav/docs/RUNTIME_OVERRIDES.md) with a worked-examples table covering the common interval/lookback combinations, since the cron-tick unit is the most common footgun.
- Clarified the `lookback` parameter row in [MODEL.md](bollhav/docs/MODEL.md) with the same caveat and a link to the runtime-overrides reference.
