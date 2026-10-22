"""Notifications: `notifications=Notifications(...)` on a `Model` says where a
run's outcome goes, and a channel (`bollhav.channels`, or your own `Channel`)
says how it gets there. The decorators do the sending: `@model_lifecycle` sets
each model's `Outcome` on its `ModelRun`, `@load_models` sends one message per
channel from the runs when `main()` returns or raises."""

from __future__ import annotations

import logging
import os
import socket
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Callable, Iterable, Protocol, TypeVar

from bollhav.model.modelrun import ModelRun, Outcome, duration_string

if TYPE_CHECKING:
    from bollhav.model.model import Model
    from bollhav.model.people import Contact

T = TypeVar("T")

logger = logging.getLogger(__name__)

ENABLED_ENV = "NOTIFICATIONS_ENABLED"


class NotificationsError(ValueError):
    """A `Notifications` that could never do what it says."""


def _enabled_override() -> bool | None:
    """`NOTIFICATIONS_ENABLED` as a bool, None when unset. A value that is not
    true/false is logged and ignored: a typo must not fail or silence a run."""
    value = os.environ.get(ENABLED_ENV, "")
    if not value:
        return None
    if value.upper() == "TRUE":
        return True
    if value.upper() == "FALSE":
        return False
    logger.warning("%s must be true or false, not %r; ignored", ENABLED_ENV, value)
    return None


class Channel(Protocol):
    """Anything that can deliver a text message. `bollhav.channels` has the
    shipped ones; a custom channel only needs `send`. `mentions` are the people to
    address, from the models' `People`; a channel that can't mention may
    ignore them."""

    def send(self, text: str, mentions: tuple[Contact, ...] = ()) -> None: ...


class Detail(Enum):
    """How much a message says. MINIMAL: the outcome, the model count and the
    total time. MODELS: every model with its own run time, and on a failure
    also the models that completed before it. A channel shared by models that
    disagree uses the more detailed level."""

    MINIMAL = "minimal"
    MODELS = "models"


class Mention(Enum):
    """Whether to address the model's `people` in the message; the
    `mention_*` flags on `Notifications` say which roles. ON_FAILURE pings
    them only when the run raises."""

    NOBODY = "nobody"
    ON_FAILURE = "on_failure"
    ALWAYS = "always"


@dataclass(frozen=True)
class Notifications:
    on_any: Channel | None = None
    on_success: Channel | None = None
    on_failure: Channel | None = None
    detail: Detail = Detail.MINIMAL
    mention: Mention = Mention.NOBODY
    mention_owners: bool = True
    mention_maintainers: bool = True
    mention_creators: bool = False
    enabled: bool = True
    suffixed_runs: bool = False

    def __post_init__(self) -> None:
        if self.mention is not Mention.NOBODY and not (
            self.mention_owners or self.mention_maintainers or self.mention_creators
        ):
            raise NotificationsError(
                f"mention={self.mention.value!r} with every mention_* flag off "
                "would address nobody; turn a role on or set mention=Mention.NOBODY"
            )

    def is_enabled(self) -> bool:
        override = _enabled_override()
        return self.enabled if override is None else override

    def mentions_on(self, failure: bool) -> bool:
        if self.mention is Mention.ALWAYS:
            return True
        return failure and self.mention is Mention.ON_FAILURE

    @property
    def success_channel(self) -> Channel | None:
        return self.on_success if self.on_success is not None else self.on_any

    @property
    def failure_channel(self) -> Channel | None:
        return self.on_failure if self.on_failure is not None else self.on_any

    def channel(self, *, failure: bool, suffixed: bool) -> Channel | None:
        if not self.is_enabled():
            return None
        if suffixed and not self.suffixed_runs and not _enabled_override():
            return None
        return self.failure_channel if failure else self.success_channel


def _header(kind: str, tags: str | None) -> str:
    host = os.environ.get("HOSTNAME", socket.gethostname())
    header = f"Pipeline {kind}: **{host}**"
    return f"{header}\n\nTAGS: `{tags}`" if tags else header


def _by_channel(
    items: Iterable[T], model_of: Callable[[T], Model], *, failure: bool
) -> list[tuple[Channel, list[T]]]:
    """`items` grouped by the channel their model reports the outcome to;
    models that stay quiet (see `Notifications.channel`) are left out.
    Grouping is by equality, so two `MicrosoftTeams("X")` are one channel."""
    groups: list[tuple[Channel, list[T]]] = []
    for item in items:
        model = model_of(item)
        notifications = model.notifications
        if notifications is None:
            continue
        channel = notifications.channel(failure=failure, suffixed=model.suffixed)
        if channel is None:
            continue
        for known, members in groups:
            if known == channel:
                members.append(item)
                break
        else:
            groups.append((channel, [item]))
    return groups


def resolve_detail_level(models: Iterable[Model]) -> Detail:
    """The group's level: MODELS if any member asks for it."""
    for model in models:
        notifications = model.notifications
        if notifications is not None and notifications.detail is Detail.MODELS:
            return Detail.MODELS
    return Detail.MINIMAL


def resolve_mentions(models: Iterable[Model], failure: bool) -> tuple[Contact, ...]:
    """The people of the models whose `mention` setting applies to this
    outcome, from the roles each model's `mention_*` flags select, each
    person once."""
    seen: set[str] = set()
    people: list[Contact] = []
    for model in models:
        notifications = model.notifications
        if notifications is None or not notifications.mentions_on(failure):
            continue
        if model.people is None:
            continue
        roles = (
            model.people.owners if notifications.mention_owners else None,
            model.people.maintainers if notifications.mention_maintainers else None,
            model.people.creators if notifications.mention_creators else None,
        )
        for contact in (c for role in roles for c in role or ()):
            key = (contact.email or contact.name).lower()
            if key not in seen:
                seen.add(key)
                people.append(contact)
    return tuple(people)


def send_quietly(send: Callable[..., None], *args, **kwargs) -> None:
    """Call a sender and log, never raise, whatever it throws: a notification
    must not fail a run, nor replace the exception the run failed with."""
    try:
        send(*args, **kwargs)
    except Exception:
        logger.exception("Notification failed; the run is unaffected")


Finished = tuple[ModelRun, Outcome]
"""A run that reached its model, with what happened."""


def _finished(runs: Iterable[ModelRun]) -> list[Finished]:
    return [(run, run.outcome) for run in runs if run.outcome is not None]


def _line(run: ModelRun, outcome: Outcome) -> str:
    """The model and its run time, as a list item."""
    return f"- {run.model.target.full_name}: {duration_string(outcome.seconds)}"


def _failure_line(run: ModelRun, outcome: Outcome) -> str:
    """The model, how long it ran, and what it raised."""
    return (
        f"- {run.model.target.full_name} after {duration_string(outcome.seconds)}: "
        f"{type(outcome.error).__name__}: {outcome.error}"
    )


def send_success(runs: Iterable[ModelRun], tags: str | None = None) -> None:
    """After a clean `main()`: one message per `on_success` channel, listing the
    models that completed on it with their run times. A suffixed run only
    reaches models with `suffixed_runs=True`."""
    completed = [(r, o) for r, o in _finished(runs) if o.error is None]
    for channel, group in _by_channel(completed, lambda p: p[0].model, failure=False):
        models = [r.model for r, _ in group]
        total = duration_string(sum(o.seconds for _, o in group))
        summary = f"{len(group)} model(s) done in {total}"
        text = f"{_header('success', tags)}\n\n"
        if resolve_detail_level(models) is Detail.MODELS:
            text += "\n".join(_line(r, o) for r, o in group) + "\n\n"
        channel.send(text + summary, resolve_mentions(models, failure=False))


def send_failure(
    exc: BaseException, runs: Iterable[ModelRun], tags: str | None = None
) -> None:
    """After a `main()` that raised: one message per `on_failure` channel. A
    model that raised reports on its own channel with its exception; when the
    run died of something else afterwards (a model's error was caught and the
    loop went on, then a connection dropped), that exception is named too.
    When the run failed outside any model (a connection that never opened,
    say), every matched model's failure channel hears it instead. A suffixed
    run only reaches models with `suffixed_runs=True`."""
    runs = list(runs)
    header = _header("failure", tags)
    finished = _finished(runs)
    failed = [(r, o) for r, o in finished if o.error is not None]
    completed = [(r, o) for r, o in finished if o.error is None]
    if failed:
        own = any(o.error is exc for _, o in failed)
        for channel, group in _by_channel(failed, lambda p: p[0].model, failure=True):
            models = [r.model for r, _ in group]
            text = f"{header}\n\n" + "\n".join(_failure_line(r, o) for r, o in group)
            if not own:
                text += f"\n\nThe run then failed with {type(exc).__name__}: {exc}"
            if completed and resolve_detail_level(models) is Detail.MODELS:
                done = "\n".join(_line(r, o) for r, o in completed)
                text += f"\n\nDone before the failure:\n{done}"
            channel.send(text, resolve_mentions(models, failure=True))
        return
    for channel, group in _by_channel(runs, lambda r: r.model, failure=True):
        channel.send(
            f"{header}\n\n{type(exc).__name__}: {exc}",
            resolve_mentions((r.model for r in group), failure=True),
        )


__all__ = [
    "Channel",
    "Detail",
    "Mention",
    "Notifications",
    "NotificationsError",
    "send_quietly",
]
