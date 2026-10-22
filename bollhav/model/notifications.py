from __future__ import annotations

import logging
import os
import socket
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Callable, Iterable, Protocol

from roskarl import env_var_bool

from bollhav.model.modelrun import ModelRun, duration_string, total_seconds

if TYPE_CHECKING:
    from bollhav.model.model import Model
    from bollhav.model.people import Contact

logger = logging.getLogger(__name__)

ENABLED_ENV = "NOTIFICATIONS_ENABLED"


class NotificationsError(ValueError):
    """A `Notifications` that could never do what it says."""


def _enabled_override() -> bool | None:
    """`NOTIFICATIONS_ENABLED` as a bool, None when unset. A value that is not
    true/false is logged and ignored: a typo must not fail or silence a run."""
    try:
        return env_var_bool(ENABLED_ENV, should_print_unset=False)
    except ValueError:
        logger.warning("%s must be true or false; ignored", ENABLED_ENV)
        return None


class Channel(Protocol):
    """Delivers a message. See `bollhav.channels` for the shipped ones"""

    def send(self, text: str, mentions: tuple[Contact, ...] = ()) -> None: ...


class Detail(Enum):
    """How much a message says."""

    MINIMAL = "minimal"
    MODELS = "models"


class Mention(Enum):
    """How and whether to address the model's People"""

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


def resolve_host() -> str:
    """Where the run happened: the pod name on Kubernetes, else the hostname."""
    return os.environ.get("HOSTNAME", socket.gethostname())


def message_header(kind: str, tags: str | None) -> str:
    """The opening of every message: the outcome, the host, the run's tags."""
    header = f"Pipeline {kind}: **{resolve_host()}**"
    return f"{header}\n\nTAGS: `{tags}`" if tags else header


def group_runs_by_channel(
    runs: Iterable[ModelRun], *, failure: bool
) -> list[tuple[Channel, list[ModelRun]]]:
    """Runs grouped by the channel their model reports the outcome to; models
    that stay quiet (see `Notifications.channel`) are left out. Grouping is by
    equality, not hashing, since a channel need not be hashable; it makes two
    `MicrosoftTeams("X")` one channel."""
    groups: list[tuple[Channel, list[ModelRun]]] = []
    for run in runs:
        notifications = run.model.notifications
        if notifications is None:
            continue
        channel = notifications.channel(failure=failure, suffixed=run.model.suffixed)
        if channel is None:
            continue
        for known, members in groups:
            if known == channel:
                members.append(run)
                break
        else:
            groups.append((channel, [run]))
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
    try:
        send(*args, **kwargs)
    except Exception:
        logger.exception("Notification failed; the run is unaffected")


def lines_string(runs: Iterable[ModelRun]) -> str:
    """One list item per run: the model and its run time."""
    return "\n".join(
        f"- {r.model.target.full_name}: {duration_string(o.seconds)}"
        for r in runs
        if (o := r.outcome) is not None
    )


def failure_string(runs: Iterable[ModelRun]) -> str:
    """One list item per run: the model, how long it ran, and what it raised."""
    return "\n".join(
        f"- {r.model.target.full_name} after {duration_string(o.seconds)}: "
        f"{type(o.error).__name__}: {o.error}"
        for r in runs
        if (o := r.outcome) is not None
    )


def send_success(runs: Iterable[ModelRun], tags: str | None = None) -> None:
    """After a clean `main()`: one message per `on_success` channel, listing the
    models that completed on it with their run times. A suffixed run only
    reaches models with `suffixed_runs=True`."""
    completed = [r for r in runs if r.outcome is not None and r.outcome.error is None]
    for channel, group in group_runs_by_channel(completed, failure=False):
        models = [r.model for r in group]
        summary = (
            f"{len(group)} model(s) done in {duration_string(total_seconds(group))}"
        )
        text = f"{message_header('success', tags)}\n\n"
        if resolve_detail_level(models) is Detail.MODELS:
            text += f"{lines_string(group)}\n\n"
        channel.send(text + summary, resolve_mentions(models, failure=False))


def send_failure(
    exc: BaseException, runs: Iterable[ModelRun], tags: str | None = None
) -> None:
    """One message per main() raise"""
    runs = list(runs)
    header = message_header("failure", tags)
    failed = [r for r in runs if r.outcome is not None and r.outcome.error is not None]
    completed = [r for r in runs if r.outcome is not None and r.outcome.error is None]
    if failed:
        own = any(o.error is exc for r in failed if (o := r.outcome) is not None)
        for channel, group in group_runs_by_channel(failed, failure=True):
            models = [r.model for r in group]
            text = f"{header}\n\n{failure_string(group)}"
            if not own:
                text += f"\n\nThe run then failed with {type(exc).__name__}: {exc}"
            if completed and resolve_detail_level(models) is Detail.MODELS:
                text += f"\n\nDone before the failure:\n{lines_string(completed)}"
            channel.send(text, resolve_mentions(models, failure=True))
        return
    for channel, group in group_runs_by_channel(runs, failure=True):
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
