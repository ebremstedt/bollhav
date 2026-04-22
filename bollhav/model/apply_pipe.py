from __future__ import annotations

from typing import TYPE_CHECKING

from bollhav.model.batch import Batch, IntervalChunks, RowChunks
from bollhav.model.directives import Directives
from bollhav.model.matching import match_models
from bollhav.model.model import Model
from bollhav.model.target_schema import TargetSchema
from bollhav.model.target import Target

if TYPE_CHECKING:
    from bollhav.pipe.pipe_config import PipeConfig


def apply_pipe_to_models(pipe: PipeConfig, folder: str = "src/models") -> list[Model]:
    """Match models against the pipe's tag expression and return a list of
    new Model objects with all pipe- and tag-driven settings baked in.

    Combines model discovery (via `match_models`) with per-model pipe
    application. Each returned model has:

        * `target.schema.suffix` set to `pipe.schema_suffix` — resolve via
          `schema.resolved` / `target.full_name` to get the suffixed form.
        * `batching.interval.expression` overridden by
          `directives.reload_interval_expression` (tag) or
          `pipe.interval_expression_override` (pipe) when set.
        * `batching.interval.window_expression` overridden by
          `pipe.window_expression_override` when set.
        * `batching.interval.tz` overridden by `pipe.tz_override` when set.
        * `batching.mode` overridden by `directives.reload_mode` when set.
        * `batching.row.batch_size` overridden by
          `directives.reload_batch_size` when set.
        * `directives.reload` preserved; the tag-driven override fields
          (`reload_mode`, `reload_batch_size`, `reload_interval_expression`)
          are cleared — `batching` is the effective truth afterwards.
        * `directives.latest`, `since`, `until` set from the pipe.

    The discovered source models are not mutated."""
    matched = match_models(
        folder=folder, tags=pipe.tags, upstream_mode=pipe.upstream_mode
    )
    return [_apply_to_model(m, pipe) for m in matched]


def _apply_to_model(model: Model, pipe: PipeConfig) -> Model:
    new_model = Model(
        target=_target_with_pipe(model.target, pipe),
        source=model.source,
        bounds=model.bounds,
        batching=_batching_with_overrides(model.batching, model.directives, pipe),
        enabled=model.enabled,
        debug=False,  # avoid re-printing the pretty() dump on the copy
        description=model.description,
        upstream=list(model.upstream),
        **model.extra,
    )
    # Model.__init__ always installs fresh Directives and re-assembles tags
    # from the raw name/schema args. We preserve the original's assembled
    # tag set (honours any Tags(tags={...}) customisation) and carry over
    # directives with the pipe transformations applied.
    new_model.directives = _directives_with_pipe(model.directives, pipe)
    new_model.tags = set(model.tags)
    return new_model


def _target_with_pipe(target: Target, pipe: PipeConfig) -> Target:
    return Target(
        name=target.name,
        schema=TargetSchema(
            name=target.schema.name,
            suffix=pipe.schema_suffix,
            suffix_appendix=target.schema.suffix_appendix,
        ),
        database=target.database,
        columns=list(target.columns),
        indexes=list(target.indexes),
        model_type=target.model_type,
        write_mode=target.write_mode,
        partitioned_by=target.partitioned_by,
        dsn_env_var=target.dsn_env_var,
        column_sorting=target.column_sorting,
        extra=target.extra,
        recreate_table=target.recreate_table,
        truncate_table=target.truncate_table,
    )


def _batching_with_overrides(
    batching: Batch | None, d: Directives, pipe: PipeConfig
) -> Batch | None:
    if batching is None:
        return None
    # Tag reload overrides win over the model's static batching. Pipe-level
    # overrides then win over tag-driven ones (explicit env > tag).
    expression = (
        pipe.interval_expression_override
        or d.reload_interval_expression
        or batching.interval.expression
    )
    window_expression = (
        pipe.window_expression_override or batching.interval.window_expression
    )
    tz = pipe.tz_override or batching.interval.tz
    mode = d.reload_mode or batching.mode
    batch_size = (
        d.reload_batch_size
        if d.reload_batch_size is not None
        else batching.row.batch_size
    )
    return Batch(
        mode=mode,
        interval=IntervalChunks(
            expression=expression,
            window_expression=window_expression,
            tz=tz,
            lookback=batching.interval.lookback,
        ),
        row=RowChunks(batch_size=batch_size),
        retries=batching.retries,
    )


def _directives_with_pipe(d: Directives, pipe: PipeConfig) -> Directives:
    return Directives(
        reload=d.reload,
        # Tag-driven overrides have been absorbed into batching — clear them
        # so batching is the single source of truth post-apply.
        reload_mode=None,
        reload_batch_size=None,
        reload_interval_expression=None,
        latest=pipe.latest.enabled and not d.reload,
        since=pipe.backfill.since,
        until=pipe.backfill.until,
    )
