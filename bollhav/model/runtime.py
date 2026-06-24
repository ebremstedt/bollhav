from __future__ import annotations

from dataclasses import replace
from datetime import datetime, tzinfo

from bollhav.model.batch import Batch
from bollhav.model.window import resolve_window
from bollhav.model.matching import matched_with_reload
from bollhav.model.model import Model
from bollhav.model.modelrun import ModelRun
from bollhav.model.state import StateMode
from bollhav.model.target import Target


def apply_runtime_overrides(
    folder: str = "src/models",
    *,
    tags: str,
    schema_suffix: str = "",
    table_suffix: str = "",
    latest: bool = False,
    backfill_since: datetime | None = None,
    backfill_until: datetime | None = None,
    interval_override: str | None = None,
    window_override: str | None = None,
    lookback_override: int | None = None,
    tz_override: tzinfo | None = None,
    state_disabled: bool = False,
    state_mode: StateMode = StateMode.DISCOVER,
) -> list[ModelRun]:
    """Match models against the tag expression and return one `ModelRun` per
    match — a new (immutable) `Model` with all pipe-/tag-driven settings baked
    in, paired with this run's resolved `window`.

    Each run's `model` has:
        * `target.schema_suffix` set to `schema_suffix`.
        * `target.suffix` set to `table_suffix`.
        * `batching.time.chunk` overridden by
          `interval_override` (pipe) when set.
        * `batching.time.window` overridden by
          `window_override` when set.
        * `batching.time.lookback` overridden by `lookback_override` when
          set.
        * `batching.time.tz` overridden by `tz_override` when set.
        * `state` / `target.staging` nulled when `state_disabled`.
    And each run's `window` is resolved from bounds + the run instruction
    (tag-driven `reload`, pipe `latest` / `backfill_since` / `backfill_until`).

    The discovered source models are not mutated."""
    matched = matched_with_reload(folder=folder, tags=tags)
    return [
        _apply_to_model(
            m,
            reload=reload,
            schema_suffix=schema_suffix,
            table_suffix=table_suffix,
            latest=latest,
            backfill_since=backfill_since,
            backfill_until=backfill_until,
            interval_override=interval_override,
            window_override=window_override,
            lookback_override=lookback_override,
            tz_override=tz_override,
            state_disabled=state_disabled,
            state_mode=state_mode,
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
    interval_override: str | None,
    window_override: str | None,
    lookback_override: int | None,
    tz_override: tzinfo | None,
    table_suffix: str = "",
    state_disabled: bool = False,
    state_mode: StateMode = StateMode.DISCOVER,
) -> ModelRun:
    """Build a `ModelRun` — a NEW (immutable) model with all pipe/tag-driven
    settings baked in, paired with this run's resolved `window`. The discovered
    source model is never mutated and the new one is born complete (nothing is
    stamped on after construction)."""
    batching = _batching_with_overrides(
        model.batching,
        interval_override=interval_override,
        window_override=window_override,
        lookback_override=lookback_override,
        tz_override=tz_override,
    )
    # `reload` (from matching) and the pipe args together pick the window mode;
    # `resolve_window` applies the precedence (reload > latest > backfill).
    window = resolve_window(
        batching,
        model.contract,
        reload=reload,
        latest=latest,
        since=backfill_since,
        until=backfill_until,
        name=model.target.full_name,
    )
    # STATE_DISABLED forces no-state semantics — null `state` + `target.staging`
    # at construction so the lifecycle hooks pass through and write() goes
    # direct. Otherwise carry the model's state, with the run's STATE_MODE
    # (discover / bulldozer / torch) stamped on — the env override only takes
    # effect here. Born-complete: never mutated onto the model after the fact.
    target = _target_with_suffix(model.target, schema_suffix, table_suffix)
    if state_disabled:
        target = replace(target, staging=None)
    if state_disabled or model.state is None:
        state = None
    else:
        state = replace(model.state, mode=state_mode)
    new_model = Model(
        target=target,
        contract=model.contract,
        batching=batching,
        temporality=model.temporality,
        view=model.view,
        state=state,
        curfew=model.curfew,
        enabled=model.enabled,
        debug=False,  # avoid re-printing pretty() on the copy
        description=model.description,
        upstream=list(model.upstream),
        tags=set(model.tags),
        tagging=model.tagging,
        **model.extra,
    )
    # Record which mode resolved the window — same precedence resolve_window
    # uses (reload > latest > backfill). Exactly one is True.
    return ModelRun(
        model=new_model,
        window=window,
        is_reload=reload,
        is_latest=not reload and latest,
        is_backfill=not reload and not latest,
    )


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
    interval_override: str | None,
    window_override: str | None,
    lookback_override: int | None,
    tz_override: tzinfo | None,
) -> Batch | None:
    if batching is None:
        return None
    # `replace` carries through every field NOT overridden here — so any new
    # `TimeChunking` / `Batch` field survives the rebuild automatically, instead
    # of silently reverting to its default (an explicit constructor was an
    # accidental allowlist). Pipe-level overrides win over the model's static
    # values; `lookback` uses an explicit None check because `lookback=0` is
    # valid and `0 or x` would wrongly fall through.
    return replace(
        batching,
        time=replace(
            batching.time,
            chunk=interval_override or batching.time.chunk,
            window=window_override or batching.time.window,
            lookback=lookback_override
            if lookback_override is not None
            else batching.time.lookback,
            tz=tz_override or batching.time.tz,
        ),
    )
