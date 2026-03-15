from bollhav.model.source import Source
from bollhav.model.target import Target
from bollhav.model.bounds import Bounds
from bollhav.model.batch import Batch
from bollhav.model.tags import Tags


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

        self.tags: set[str] = (tagging or Tags()).assemble(
            self.name, self.target.schema.name
        )

        if self.debug:
            from pprint import pprint

            pprint(self.__dict__)

        for key, val in kwargs.items():
            if callable(val):
                kwargs[key] = val(
                    **{k: v for k, v in kwargs.items() if not callable(v)}
                )
        self.extra = kwargs

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
            f"extra={self.extra!r})"
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Model):
            return NotImplemented
        return self.__dict__ == other.__dict__
