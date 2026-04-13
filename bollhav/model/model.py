import logging
from bollhav.model.source import Source
from bollhav.model.target import Target
from bollhav.model.bounds import Bounds
from bollhav.model.batch import Batch
from bollhav.model.tags import Tags

logger = logging.getLogger(__name__)


class Model:
    def __init__(
        self,
        name: str,
        target: Target,
        source: Source | None = None,
        bounds: Bounds | None = None,
        batching: Batch | None = None,
        tagging: Tags | None = None,
        enabled: bool = True,
        debug: bool = False,
        description: str | None = None,
        upstream: list[str] | None = None,
        **kwargs,
    ):
        self.name = name
        self.source = source
        self.target = target
        self.bounds = bounds or Bounds()
        self.batching = batching or Batch()
        self.enabled = enabled
        self.debug = debug
        self.description = description
        self.upstream: list[str] = upstream or []

        self.tags: set[str] = (tagging or Tags()).assemble(
            self.name, self.target.schema.name
        )

        for key, val in kwargs.items():
            if callable(val):
                kwargs[key] = val(
                    **{k: v for k, v in kwargs.items() if not callable(v)}
                )
        self.extra = kwargs

        logger.debug("Initialized model %r (enabled=%s)", self.name, self.enabled)
        if self.debug:
            self.pretty()

    def pretty(self) -> None:
        cols = self.target.columns
        unique_cols = [c.name for c in self.target.unique_columns]
        col_summary = ", ".join(
            f"{c.name}*" if c.name in unique_cols else c.name for c in cols
        )
        lines = [
            f"Model: {self.name}",
            f"  enabled:       {self.enabled}",
            f"  description:   {self.description}",
            f"  tags:          {', '.join(sorted(self.tags))}",
            f"  upstream:      {', '.join(self.upstream) if self.upstream else '(none)'}",
            "",
            "  target:",
            f"    name:        {self.target.name}",
            f"    schema:      {self.target.schema.resolved}",
            f"    write_mode:  {self.target.write_mode.value}",
            f"    model_type:  {self.target.model_type.value}",
            f"    partitioned: {self.target.partitioned_by}",
            f"    columns ({len(cols)}): {col_summary}",
        ]
        if self.source:
            lines += [
                "",
                "  source:",
                f"    name:        {self.source.name}",
                f"    schema:      {self.source.schema}",
                f"    dsn_env_var: {self.source.dsn_env_var}",
            ]
        lines += [
            "",
            "  batching:",
            f"    expression:  {self.batching.batch_expression}",
            f"    lookback:    {self.batching.lookback}",
            f"    retries:     {self.batching.retries}",
            "",
            "  bounds:",
            f"    begin:       {self.bounds.begin}",
            f"    end:         {self.bounds.end}",
        ]
        logger.debug("\n".join(lines))

    def __repr__(self) -> str:
        return (
            f"Model("
            f"name={self.name!r}, "
            f"source={self.source!r}, "
            f"target={self.target!r}, "
            f"bounds={self.bounds!r}, "
            f"batching={self.batching!r}, "
            f"tags={self.tags!r}, "
            f"enabled={self.enabled}, "
            f"debug={self.debug}, "
            f"description={self.description!r}, "
            f"upstream={self.upstream!r}, "
            f"extra={self.extra!r})"
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Model):
            return NotImplemented
        return self.__dict__ == other.__dict__
