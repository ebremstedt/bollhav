from typing import Callable, Annotated
from bollhav.model.model_config import ModelConfig
from dataclasses import dataclass


def _named_execute(fn: Callable) -> Callable:
    if fn.__name__ != "execute":
        raise ValueError(f"Expected a function named 'execute', got '{fn.__name__}'")
    return fn


ExecuteFunction = Annotated[Callable, _named_execute]


@dataclass
class Model:
    model_config: ModelConfig
    execute: ExecuteFunction

    def __post_init__(self) -> None:
        self.execute = _named_execute(self.execute)
