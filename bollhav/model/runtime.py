from __future__ import annotations

from datetime import datetime, tzinfo

from bollhav.model.batch import Batch, IntervalChunks, RowChunks
from bollhav.model.directives import Directives
from bollhav.model.matching import match_models
from bollhav.model.model import Model
from bollhav.model.ordering import UpstreamMode
from bollhav.model.target import Target
from bollhav.model.target_schema import TargetSchema


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
) -> list[Model]:
    """Match models against the tag expression and return new Model objects
    with all pipe- and tag-driven settings baked in.

    Each returned model has:
        * `target.schema.suffix` set to `schema_suffix`.
        * `target.suffix` set to `table_suffix`.
        * `batching.interval.expression` overridden by
          `directives.reload_interval_expression` (tag) or
          `interval_expression_override` (pipe) when set.
        * `batching.interval.window_expression` overridden by
          `window_expression_override` when set.
        * `batching.interval.lookback` overridden by `lookback_override` when
          set.
        * `batching.interval.tz` overridden by `tz_override` when set.
        * `batching.mode` overridden by `directives.reload_mode` when set.
        * `batching.row.batch_size` overridden by
          `directives.reload_batch_size` when set.
        * `directives.reload` preserved; tag-driven override fields cleared.
        * `directives.latest`, `since`, `until` set from the pipe args.

    The discovered source models are not mutated."""
    matched = match_models(folder=folder, tags=tags, upstream_mode=upstream_mode)
    return [
        _apply_to_model(
            m,
            schema_suffix=schema_suffix,
            table_suffix=table_suffix,
            latest=latest,
            backfill_since=backfill_since,
            backfill_until=backfill_until,
            interval_expression_override=interval_expression_override,
            window_expression_override=window_expression_override,
            lookback_override=lookback_override,
            tz_override=tz_override,
        )
        for m in matched
    ]


def _apply_to_model(
    model: Model,
    *,
    schema_suffix: str,
    latest: bool,
    backfill_since: datetime | None,
    backfill_until: datetime | None,
    interval_expression_override: str | None,
    window_expression_override: str | None,
    lookback_override: int | None,
    tz_override: tzinfo | None,
    table_suffix: str = "",
) -> Model:
    new_model = Model(
        target=_target_with_suffix(model.target, schema_suffix, table_suffix),
        source=model.source,
        bounds=model.bounds,
        batching=_batching_with_overrides(
            model.batching,
            model.directives,
            interval_expression_override=interval_expression_override,
            window_expression_override=window_expression_override,
            lookback_override=lookback_override,
            tz_override=tz_override,
        ),
        enabled=model.enabled,
        debug=False,  # avoid re-printing pretty() on the copy
        description=model.description,
        upstream=list(model.upstream),
        **model.extra,
    )
    # Model.__init__ always installs fresh Directives and re-assembles tags
    # from the raw name/schema args. Carry over the original's assembled tag
    # set + transformed directives.
    new_model.directives = _directives_with_pipe(
        model.directives,
        latest=latest,
        backfill_since=backfill_since,
        backfill_until=backfill_until,
    )
    new_model.tags = set(model.tags)
    return new_model


def _target_with_suffix(
    target: Target, schema_suffix: str, table_suffix: str = ""
) -> Target:
    return Target(
        name=target.name,
        suffix=table_suffix or target.suffix,
        suffix_appendix=target.suffix_appendix,
        schema=TargetSchema(
            name=target.schema.name,
            suffix=schema_suffix,
            suffix_appendix=target.schema.suffix_appendix,
        ),
        catalog=target.catalog,
        database=target.database,
        columns=list(target.columns),
        indexes=list(target.indexes),
        model_type=target.model_type,
        write_mode=target.write_mode,
        dsn_env_var=target.dsn_env_var,
        column_sorting=target.column_sorting,
        extra=target.extra,
        recreate_table=target.recreate_table,
        truncate_table=target.truncate_table,
    )


def _batching_with_overrides(
    batching: Batch | None,
    d: Directives,
    *,
    interval_expression_override: str | None,
    window_expression_override: str | None,
    lookback_override: int | None,
    tz_override: tzinfo | None,
) -> Batch | None:
    if batching is None:
        return None
    # Tag reload overrides win over the model's static batching. Pipe-level
    # overrides win over tag-driven ones (explicit env > tag).
    expression = (
        interval_expression_override
        or d.reload_interval_expression
        or batching.interval.expression
    )
    window_expression = (
        window_expression_override or batching.interval.window_expression
    )
    lookback = (
        lookback_override
        if lookback_override is not None
        else batching.interval.lookback
    )
    tz = tz_override or batching.interval.tz
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
            lookback=lookback,
        ),
        row=RowChunks(batch_size=batch_size),
        retries=batching.retries,
    )


def _directives_with_pipe(
    d: Directives,
    *,
    latest: bool,
    backfill_since: datetime | None,
    backfill_until: datetime | None,
) -> Directives:
    return Directives(
        reload=d.reload,
        reload_mode=None,
        reload_batch_size=None,
        reload_interval_expression=None,
        latest=latest and not d.reload,
        since=backfill_since,
        until=backfill_until,
    )
