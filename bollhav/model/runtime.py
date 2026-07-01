from __future__ import annotations

from dataclasses import replace
from datetime import datetime, tzinfo

from bollhav.model.batch import Batch
from bollhav.model.window import resolve_window, window_holds_a_grid_cell
from bollhav.model.matching import matched_with_reload
from bollhav.model.model import Model
from bollhav.model.modelrun import ModelRun
from bollhav.model.state import StateMode
from bollhav.model.target import Target


# ── errors ──


class FixedModelIntervalOverrideError(ValueError):
    """`INTERVAL_OVERRIDE` was set for a fixed-grid (`ChunkFix`) model. A fixed
    model's chunk *is* its state identity, so re-chunking at runtime would fork
    its state into mixed granularity — it is not allowed. Change the model's
    declared chunk and run `STATE_MODE=torch`, or make it flexible (`ChunkFlex`)
    if arbitrary re-chunking is intended."""

    def __init__(self, full_name: str, override: str, chunk: str) -> None:
        super().__init__(
            f"INTERVAL_OVERRIDE={override!r} cannot be applied to {full_name!r}: "
            f"it is a fixed grid (ChunkFix, chunk={chunk!r}) whose chunk is its "
            f"state identity — re-chunking would fork state. Change its declared "
            f"chunk and run STATE_MODE=torch, or make it flexible (ChunkFlex)."
        )


class FixedModelWindowTooNarrowError(ValueError):
    """A fixed-grid model's run window holds no whole `[tick, next_tick)` grid
    cell — narrower than the chunk, or misaligned. A fixed grid runs whole cells
    only (a cell may aggregate over its interval), so a partial window would
    silently run nothing, or a wrong partial result. Widen the window to a chunk
    boundary, or make the model flexible (`ChunkFlex`)."""

    def __init__(self, full_name: str, chunk: str, since: object, until: object) -> None:
        super().__init__(
            f"run window [{since}, {until}) for {full_name!r} holds no whole "
            f"{chunk!r} grid cell (narrower than the chunk, or misaligned). A "
            f"fixed grid runs whole cells only — widen the window to a chunk "
            f"boundary, or make it flexible (ChunkFlex)."
        )


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
        * `batching.time.latest_window` overridden by
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
        full_name=model.target.full_name,
    )
    # torch wipes *all* state at the bootstrap; the window only scopes what runs
    # *now* (prefill refills the whole contract, so nothing is orphaned — the
    # unrun remainder sits pending for a later discover run to drain). With no
    # explicit window a bare torch reloads the whole contract range — a clean
    # full reload; with a BACKFILL window it runs just that slice now.
    if (
        state_mode is StateMode.TORCH
        and backfill_since is None
        and backfill_until is None
    ):
        window = resolve_window(
            batching,
            model.contract,
            temporality=model.temporality,
            reload=True,
            name=model.target.full_name,
        )
    else:
        # `reload` (from matching) and the pipe args together pick the window
        # mode; `resolve_window` applies the precedence (reload > latest > backfill).
        window = resolve_window(
            batching,
            model.contract,
            temporality=model.temporality,
            reload=reload,
            latest=latest,
            since=backfill_since,
            until=backfill_until,
            name=model.target.full_name,
        )
    # A fixed grid runs whole cells only; a run window that can't hold one
    # (narrower than the chunk, or misaligned) would silently run nothing — or a
    # wrong partial of a cell that may aggregate over its interval — so refuse it.
    if (
        window is not None
        and batching is not None
        and not batching.time.is_flexible
        and not window_holds_a_grid_cell(window, batching.time.chunk)
    ):
        raise FixedModelWindowTooNarrowError(
            model.target.full_name,
            batching.time.chunk,
            window.since,
            window.until,
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
        materialization=model.materialization,
        query=model.query,
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
    full_name: str | None = None,
) -> Batch | None:
    if batching is None:
        return None
    # INTERVAL_OVERRIDE re-chunks at runtime, but only a flexible model
    # (ChunkFlex) can absorb that — re-chunking a FIXED grid would fork its state
    # into mixed granularity. A fixed model cannot be re-chunked at runtime;
    # asking to is a hard error (change its chunk via STATE_MODE=torch instead).
    chunk = batching.time.chunk
    if interval_override:
        if not batching.time.is_flexible:
            raise FixedModelIntervalOverrideError(
                full_name or "this model", interval_override, batching.time.chunk
            )
        chunk = interval_override
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
            chunk=chunk,
            latest_window=window_override or batching.time.latest_window,
            lookback=lookback_override
            if lookback_override is not None
            else batching.time.lookback,
            tz=tz_override or batching.time.tz,
        ),
    )
