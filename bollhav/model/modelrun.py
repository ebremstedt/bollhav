from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

from bollhav.model.intervals import TZInterval

if TYPE_CHECKING:
    from psycopg import sql

    from bollhav.model.model import Model


class MissingQueryBuilderError(ValueError):
    """`resolve_query` on a model that has no `query_builder`. A model without
    one has no query to resolve, so asking for it is a programming error."""

    def __init__(self, full_name: str) -> None:
        super().__init__(f"{full_name!r} has no query_builder to resolve")


class QueryNotSqlStringError(TypeError):
    """`resolve_sql` on a model whose `query_builder` yields something other
    than a `str`, typically a psycopg `sql.Composable`. Those only run on
    Postgres; use `resolve_query` there."""

    def __init__(self, full_name: str, got: type) -> None:
        super().__init__(
            f"{full_name!r}: query_builder must return a str for resolve_sql, "
            f"got {got.__name__}"
        )


@dataclass
class ModelRun:
    """One invocation of a model — the immutable `model` definition paired with
    the run of it.

    `Model` answers *what / where / how* (target, batching, contract, kind); a
    `ModelRun` answers *when / how-far*:

        window     — the single time window this run targets (resolved once by
                     `runtime.resolve_window` from contract + the run instruction).
        intervals  — the window split into the chunk contract; narrowed to the
                     still-actionable subset during the state bootstrap. `(None,)`
                     for a model with no window (oneshot / view).
        run_id     — minted once here and shared across this run's state
                     transitions (insert / mark_running / mark_applied / …).

    `is_reload` / `is_latest` / `is_backfill` record **which mode** resolved the
    window — exactly one is `True` on a run built by `apply_runtime_overrides`
    (precedence: reload > latest > backfill). Check them at runtime to branch on
    the job kind, e.g. `if run.is_backfill: ...`. All `False` on a bare
    `ModelRun()` (built directly, outside the runtime).

    `runtime.apply_runtime_overrides` mints one `ModelRun` per matched model;
    it's what flows through `@model_lifecycle` and the user's run loop. The
    definition is immutable; the run-state on this object is what evolves."""

    model: "Model"
    window: TZInterval | None = None
    intervals: tuple[TZInterval, ...] | tuple[None] = (None,)
    run_id: UUID = field(default_factory=uuid4)
    is_reload: bool = False
    is_latest: bool = False
    is_backfill: bool = False

    def resolve_query(
        self, since: datetime | None = None, until: datetime | None = None
    ) -> "str | sql.Composable":
        """The model's defining SELECT as runnable SQL. A string `query_builder`
        passes through; a callable is invoked as `query_builder(self, since,
        until)` so it can use `self.model.ref(...)` and the window. Returns
        whatever the builder yields — a `str`, a psycopg `sql.Composable`, … —
        and raises `MissingQueryBuilderError` if the model has none.
        `since`/`until` are `None` for a windowless build (a view / timeless
        model)."""
        qb = self.model.query_builder
        if qb is None:
            raise MissingQueryBuilderError(self.model.target.full_name)
        return qb(self, since, until) if callable(qb) else qb

    def resolve_sql(
        self, *, since: datetime | None = None, until: datetime | None = None
    ) -> str:
        """`resolve_query` for sources that take plain SQL text (MSSQL, files,
        anything not psycopg): the builder must yield a `str`, or
        `QueryNotSqlStringError` is raised."""
        query = self.resolve_query(since, until)
        if not isinstance(query, str):
            raise QueryNotSqlStringError(self.model.target.full_name, type(query))
        return query


__all__ = ["ModelRun", "MissingQueryBuilderError", "QueryNotSqlStringError"]
