"""Notifications: `notifications=Notifications(...)` on a `Model` says where a
run's outcome goes, and a channel (`bollhav.channels`, or your own `Channel`)
says how it gets there. The decorators do the sending: `@model_lifecycle` records each model's
outcome and run time, `@load_models` sends one message per channel when
`main()` returns or raises."""

from __future__ import annotations

import logging
import os
import socket
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Callable, Iterable, Protocol

if TYPE_CHECKING:
    from bollhav.model.model import Model
    from bollhav.model.modelrun import ModelRun
    from bollhav.model.people import Contact

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


@dataclass
class Outcome:
    """One model's run as `@model_lifecycle` saw it: how long it took and the
    exception, if it raised."""

    model: Model
    seconds: float
    error: BaseException | None = None

    @property
    def line(self) -> str:
        """The model and its run time, as a list item on a card."""
        return f"- {self.model.target.full_name}: {_duration(self.seconds)}"

    @property
    def failure_line(self) -> str:
        """The model, how long it ran, and what it raised."""
        return (
            f"- {self.model.target.full_name} after {_duration(self.seconds)}: "
            f"{type(self.error).__name__}: {self.error}"
        )


@dataclass
class _Outcomes:
    """The outcomes of the current `@load_models` run. Reset per run."""

    items: list[Outcome] = field(default_factory=list)

    def reset(self) -> None:
        self.items = []

    def record(
        self, model: Model, seconds: float, error: BaseException | None = None
    ) -> None:
        self.items.append(Outcome(model=model, seconds=seconds, error=error))


OUTCOMES = _Outcomes()


def _duration(seconds: float) -> str:
    minutes, secs = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes}m {secs}s" if hours else f"{minutes}m {secs}s"


def _header(kind: str, tags: str | None) -> str:
    host = os.environ.get("HOSTNAME", socket.gethostname())
    header = f"Pipeline {kind}: **{host}**"
    return f"{header}\n\nTAGS: `{tags}`" if tags else header


def _grouped(
    outcomes: Iterable[Outcome], *, failure: bool
) -> list[tuple[Channel, list[Outcome]]]:
    """Outcomes grouped by the channel their model reports the outcome to;
    models that stay quiet (see `Notifications.channel`) are left out.
    Grouping is by equality, so two `MicrosoftTeams("X")` are one channel."""
    groups: list[tuple[Channel, list[Outcome]]] = []
    for outcome in outcomes:
        notifications = outcome.model.notifications
        if notifications is None:
            continue
        channel = notifications.channel(
            failure=failure, suffixed=outcome.model.suffixed
        )
        if channel is None:
            continue
        for known, members in groups:
            if known == channel:
                members.append(outcome)
                break
        else:
            groups.append((channel, [outcome]))
    return groups


def _detail(outcomes: Iterable[Outcome]) -> Detail:
    """The group's level: MODELS if any member asks for it."""
    for outcome in outcomes:
        notifications = outcome.model.notifications
        if notifications is not None and notifications.detail is Detail.MODELS:
            return Detail.MODELS
    return Detail.MINIMAL


def _mentions(models: Iterable[Model], failure: bool) -> tuple[Contact, ...]:
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
    try:
        send(*args, **kwargs)
    except Exception:
        logger.exception("Notification failed; the run is unaffected")


def send_success(tags: str | None = None) -> None:
    """After a clean `main()`: one message per `on_success` channel, listing the
    models that completed on it with their run times. A suffixed run only
    reaches models with `suffixed_runs=True`."""
    completed = (o for o in OUTCOMES.items if o.error is None)
    for channel, outcomes in _grouped(completed, failure=False):
        total = _duration(sum(o.seconds for o in outcomes))
        summary = f"{len(outcomes)} model(s) done in {total}"
        text = f"{_header('success', tags)}\n\n"
        if _detail(outcomes) is Detail.MODELS:
            text += "\n".join(o.line for o in outcomes) + "\n\n"
        channel.send(
            text + summary, _mentions((o.model for o in outcomes), failure=False)
        )


def send_failure(
    exc: BaseException, runs: Iterable[ModelRun], tags: str | None = None
) -> None:
    header = _header("failure", tags)
    failed = [o for o in OUTCOMES.items if o.error is not None]
    completed = [o for o in OUTCOMES.items if o.error is None]
    if failed:
        own = any(o.error is exc for o in failed)
        for channel, outcomes in _grouped(failed, failure=True):
            text = f"{header}\n\n" + "\n".join(o.failure_line for o in outcomes)
            if not own:
                text += f"\n\nThe run then failed with {type(exc).__name__}: {exc}"
            if completed and _detail(outcomes) is Detail.MODELS:
                done = "\n".join(o.line for o in completed)
                text += f"\n\nDone before the failure:\n{done}"
            channel.send(text, _mentions((o.model for o in outcomes), failure=True))
        return
    by_channel: list[tuple[Channel, list[Model]]] = []
    for run in runs:
        notifications = run.model.notifications
        if notifications is None:
            continue
        channel = notifications.channel(failure=True, suffixed=run.model.suffixed)
        if channel is None:
            continue
        for known, members in by_channel:
            if known == channel:
                members.append(run.model)
                break
        else:
            by_channel.append((channel, [run.model]))
    for channel, models in by_channel:
        channel.send(
            f"{header}\n\n{type(exc).__name__}: {exc}",
            _mentions(models, failure=True),
        )


__all__ = [
    "Channel",
    "Detail",
    "Mention",
    "Notifications",
    "NotificationsError",
    "Outcome",
    "OUTCOMES",
    "send_quietly",
]
