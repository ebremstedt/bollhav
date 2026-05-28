"""Dry-run summary for @load_models.

Activated by DRY_RUN=true (concise) or DRY_RUN_EXTRA=true (verbose).
The decorator runs everything up to and including apply_runtime_overrides,
then calls into here instead of invoking the user's main(). Strictly
read-only — no side effects."""

from bollhav.model.batch import ChunkMode
from bollhav.model.load_models import _RuntimeConfig
from bollhav.model.model import Model
from bollhav.model.tagexpr import explain_groups


def print_summary(models: list[Model], cfg: _RuntimeConfig) -> None:
    """Print the dry-run summary to stdout.

    Models are listed alphabetically by `target.full_name` regardless of
    the original ordering — easier to scan and compare across runs."""
    width = 60
    print()
    suffix = " (extra)" if cfg.dry_run_extra else ""
    print(f"── dry run{suffix} " + "─" * (width - 11 - len(suffix)))
    plural = "s" if len(models) != 1 else ""
    print(f" {len(models)} model{plural} matched, mode = {_mode_label(cfg)}")
    print()
    _print_tag_table(cfg.tags)

    ordered = sorted(models, key=lambda m: m.target.full_name)
    if cfg.dry_run_extra:
        for model in ordered:
            _print_model_extra(model)
            print()
    else:
        _print_models_concise(ordered)
    print("─" * width)


def _print_tag_table(expression: str) -> None:
    """Show the tag expression broken into groups, each with its
    plain-English translation. Skipped silently if the expression
    can't be parsed (defensive — tests/programmatic callers may
    pass a stub)."""
    try:
        pairs = explain_groups(expression)
    except Exception:
        return
    if not pairs:
        return
    raw_width = max(len(raw) for raw, _ in pairs)
    header = "tags:" if len(pairs) == 1 else "tags (matches any):"
    print(header)
    for raw, english in pairs:
        print(f"  {raw.ljust(raw_width)}   →  {english}")
    print()


def _mode_label(cfg: _RuntimeConfig) -> str:
    if cfg.latest:
        return "latest"
    if cfg.backfill_enabled:
        return "backfill"
    return "reload"


# ── concise ──────────────────────────────────────────────────────────


def _print_models_concise(models: list[Model]) -> None:
    """Models grouped by schema (alphabetical), table names padded
    within each group so the right-hand column lines up. Unbatched
    models (views) get no trailing column."""
    by_schema: dict[str, list[Model]] = {}
    for model in models:
        by_schema.setdefault(model.target.schema.resolved, []).append(model)

    schemas = sorted(by_schema)
    for i, schema in enumerate(schemas):
        group = by_schema[schema]
        name_width = max(len(m.target.name_resolved) for m in group)
        print(f"{schema}:")
        for model in group:
            tail = _concise_tail(model)
            if tail is None:
                print(f"  {model.target.name_resolved}")
            else:
                name = model.target.name_resolved.ljust(name_width)
                print(f"  {name}   {tail}")
        if i < len(schemas) - 1:
            print()


def _concise_tail(model: Model) -> str | None:
    """Right-hand column for a model row. None means no tail (view / no
    batching). ROW-mode and INTERVAL-mode get different shapes."""
    if model.batching is None:
        return None
    if model.batching.mode is ChunkMode.ROW:
        return f"{model.batching.row.batch_size} rows/chunk"
    count = _format_interval_count(model.intervals)
    return f"{count} × {model.batching.interval.expression}"


# ── extra ────────────────────────────────────────────────────────────


def _print_model_extra(model: Model) -> None:
    print(f"▸ {model.target.full_name}")
    if model.target.catalog:
        print(f"    catalog      : {model.target.catalog}")
    print(f"    schema       : {model.target.schema.resolved}")
    print(f"    write mode   : {model.target.write_mode.value}")

    if model.batching is not None:
        if model.batching.mode is ChunkMode.ROW:
            print("    mode         : row")
            print(f"    batch size   : {model.batching.row.batch_size}")
        else:
            intervals = model.intervals
            print(f"    cron         : {model.batching.interval.expression}")
            print(f"    window       : {_format_window(intervals)}")
            print(f"    intervals    : {_format_interval_count(intervals)}")
            if model.batching.interval.lookback:
                print(f"    lookback     : {model.batching.interval.lookback}")

    print(f"    bounds       : {_format_bounds(model)}")
    print(f"    tags         : {_format_tags(model)}")
    print(f"    upstream     : {_format_upstream(model)}")
    print(f"    source       : {_format_source(model)}")
    if model.description:
        print(f"    description  : {model.description}")


def _format_bounds(model: Model) -> str:
    b = model.bounds
    if b.begin is None and b.end is None:
        return "(none)"
    begin = b.begin.isoformat() if b.begin else "—"
    end = b.end.isoformat() if b.end else "—"
    return f"{begin} → {end}"


def _format_tags(model: Model) -> str:
    return ", ".join(sorted(model.tags)) if model.tags else "(none)"


def _format_upstream(model: Model) -> str:
    return ", ".join(model.upstream) if model.upstream else "(none)"


def _format_source(model: Model) -> str:
    src = model.source
    if src is None:
        return "(none)"
    cls = type(src).__name__
    name = getattr(src, "name", None) or getattr(src, "path", "?")
    schema = getattr(src, "schema", None)
    if schema:
        return f"{cls}({schema}.{name})"
    return f"{cls}({name})"


# ── shared formatters ───────────────────────────────────────────────


def _format_window(intervals: list) -> str:
    real = [iv for iv in intervals if iv is not None]
    if not real:
        return "(unfiltered)"
    return f"{real[0].since.isoformat()} → {real[-1].until.isoformat()}"


def _format_interval_count(intervals: list) -> str:
    return str(len([iv for iv in intervals if iv is not None]))
