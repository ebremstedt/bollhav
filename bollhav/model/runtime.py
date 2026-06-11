from __future__ import annotations

from dataclasses import replace
from datetime import datetime, tzinfo

from bollhav.model.batch import Batch, TimeChunking
from bollhav.model.window import resolve_window
from bollhav.model.matching import matched_with_reload
from bollhav.model.model import Model
from bollhav.model.modelrun import ModelRun
from bollhav.model.ordering import UpstreamMode
from bollhav.model.target import Target


def apply_runtime_overrides(
    folder: str = "src/models",
    *,
    tags: str,
    schema_suffix: str = "",
    table_suffix: str = "",
    upstream_mode: UpstreamMode = UpstreamMode.ENFORCE,
    latest: bool = False,
    backfill_since: datetime | None = None,
    backfill_until: datetime | None = None,
    interval_expression_override: str | None = None,
    window_expression_override: str | None = None,
    lookback_override: int | None = None,
    tz_override: tzinfo | None = None,
    state_disabled: bool = False,
) -> list[ModelRun]:
    """Match models against the tag expression and return one `ModelRun` per
    match — a new (immutable) `Model` with all pipe-/tag-driven settings baked
    in, paired with this run's resolved `window`.

    Each run's `model` has:
        * `target.schema_suffix` set to `schema_suffix`.
        * `target.suffix` set to `table_suffix`.
        * `batching.time.chunk` overridden by
          `interval_expression_override` (pipe) when set.
        * `batching.time.window` overridden by
          `window_expression_override` when set.
        * `batching.time.lookback` overridden by `lookback_override` when
          set.
        * `batching.time.tz` overridden by `tz_override` when set.
        * `state` / `target.staging` nulled when `state_disabled`.
    And each run's `window` is resolved from bounds + the run instruction
    (tag-driven `reload`, pipe `latest` / `backfill_since` / `backfill_until`).

    The discovered source models are not mutated."""
    matched = matched_with_reload(folder=folder, tags=tags, upstream_mode=upstream_mode)
    return [
        _apply_to_model(
            m,
            reload=reload,
            schema_suffix=schema_suffix,
            table_suffix=table_suffix,
            latest=latest,
            backfill_since=backfill_since,
            backfill_until=backfill_until,
            interval_expression_override=interval_expression_override,
            window_expression_override=window_expression_override,
            lookback_override=lookback_override,
            tz_override=tz_override,
            state_disabled=state_disabled,
        )
        for m, reload in matched
    ]


def _apply_to_model(
    model: Model,
    *,
    reload: bool,
    schema_suffix: str,
    latest: bool,
    backfill_since: datetime | None,
    backfill_until: datetime | None,
    interval_expression_override: str | None,
    window_expression_override: str | None,
    lookback_override: int | None,
    tz_override: tzinfo | None,
    table_suffix: str = "",
    state_disabled: bool = False,
) -> ModelRun:
    """Build a `ModelRun` — a NEW (immutable) model with all pipe/tag-driven
    settings baked in, paired with this run's resolved `window`. The discovered
    source model is never mutated and the new one is born complete (nothing is
    stamped on after construction)."""
    batching = _batching_with_overrides(
        model.batching,
        interval_expression_override=interval_expression_override,
        window_expression_override=window_expression_override,
        lookback_override=lookback_override,
        tz_override=tz_override,
    )
    # `reload` (from matching) and the pipe args together pick the window mode;
    # `resolve_window` applies the precedence (reload > latest > backfill).
    window = resolve_window(
        batching,
        model.bounds,
        reload=reload,
        latest=latest,
        since=backfill_since,
        until=backfill_until,
        name=model.target.full_name,
    )
    # STATE_DISABLED forces no-state semantics — null `state` + `target.staging`
    # at construction so the lifecycle hooks pass through and write() goes
    # direct. Born-complete: never mutated onto the model after the fact.
    target = _target_with_suffix(model.target, schema_suffix, table_suffix)
    if state_disabled:
        target = replace(target, staging=None)
    new_model = Model(
        target=target,
        bounds=model.bounds,
        batching=batching,
        kind=model.kind,
        state=None if state_disabled else model.state,
        enabled=model.enabled,
        debug=False,  # avoid re-printing pretty() on the copy
        description=model.description,
        upstream=list(model.upstream),
        tags=set(model.tags),
        **model.extra,
    )
    return ModelRun(model=new_model, window=window)


def _target_with_suffix(
    target: Target, schema_suffix: str, table_suffix: str = ""
) -> Target:
    return Target(
        name=target.name,
        suffix=table_suffix or target.suffix,
        suffix_appendix=target.suffix_appendix,
        schema=target.schema,
        schema_suffix=schema_suffix,
        schema_suffix_appendix=target.schema_suffix_appendix,
        catalog=target.catalog,
        database=target.database,
        columns=list(target.columns),
        indexes=list(target.indexes),
        write_mode=target.write_mode,
        dsn_env_var=target.dsn_env_var,
        column_sorting=target.column_sorting,
        extra=target.extra,
        recreate_table=target.recreate_table,
        truncate_table=target.truncate_table,
        staging=target.staging,
    )


def _batching_with_overrides(
    batching: Batch | None,
    *,
    interval_expression_override: str | None,
    window_expression_override: str | None,
    lookback_override: int | None,
    tz_override: tzinfo | None,
) -> Batch | None:
    if batching is None:
        return None
    # Pipe-level override wins over the model's static interval expression.
    expression = interval_expression_override or batching.time.chunk
    window_expression = window_expression_override or batching.time.window
    lookback = (
        lookback_override if lookback_override is not None else batching.time.lookback
    )
    tz = tz_override or batching.time.tz
    return Batch(
        time=TimeChunking(
            chunk=expression,
            window=window_expression,
            tz=tz,
            lookback=lookback,
        ),
        size=batching.size,
        retries=batching.retries,
    )
