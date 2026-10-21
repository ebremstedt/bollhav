"""Notifications: `notifications=Notifications(...)` on a `Model` says where a
run's outcome goes, and a channel type (`Teams`) says how it gets there. The
decorators do the sending: `@model_lifecycle` records each model's outcome and
run time, `@load_models` sends one message per channel when `main()` returns
or raises."""

from __future__ import annotations

import json
import logging
import os
import socket
import urllib.request
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Iterable, Protocol

if TYPE_CHECKING:
    from bollhav.model.model import Model
    from bollhav.model.modelrun import ModelRun
    from bollhav.model.people import Contact

logger = logging.getLogger(__name__)

ENABLED_ENV = "NOTIFICATIONS_ENABLED"


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
    """Anything that can deliver a text message. `Teams` is the one shipped;
    a custom channel only needs `send`. `mentions` are the people to address,
    from the models' `People`; a channel that can't mention may ignore them."""

    def send(self, text: str, mentions: tuple[Contact, ...] = ()) -> None: ...


TEAMS_MAX_BYTES = 20_000
"""Teams rejects a webhook message above about 28 KB of JSON; this leaves room
for the card envelope and the mentions."""


def _capped(text: str, limit: int = TEAMS_MAX_BYTES) -> str:
    """`text` untouched when it fits in `limit` bytes of UTF-8. Otherwise its
    head and tail with the middle cut out, and a note in the gap saying how
    much went: the start names the run and the model, the end has the last
    lines of the error, and the middle is the part nobody reads in a card."""
    data = text.encode("utf-8")
    if len(data) <= limit:
        return text
    note = "\n\n… message capped for Teams, {cut:,} bytes cut here …\n\n"
    budget = limit - len(note.format(cut=len(data)).encode("utf-8"))
    head_len, tail_len = budget * 2 // 3, budget // 3
    head = data[:head_len].decode("utf-8", errors="ignore")
    tail = data[len(data) - tail_len :].decode("utf-8", errors="ignore")
    return head + note.format(cut=len(data) - head_len - tail_len) + tail


def _post_json(url: str, payload: dict, timeout: float = 10.0) -> None:
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        response.read()


@dataclass(frozen=True)
class Teams:
    """A Microsoft Teams incoming webhook. The env var named `webhook_env` holds
    the URL; unset means nothing is sent, so local runs stay quiet. A failed
    send is logged, never raised — a notification must not fail a run. Every
    send logs one line: sent, skipped or failed. A message over
    `TEAMS_MAX_BYTES` is cut in the middle with a note, so an oversized error
    still arrives instead of being rejected."""

    webhook_env: str

    def send(self, text: str, mentions: tuple[Contact, ...] = ()) -> None:
        url = os.environ.get(self.webhook_env)
        if not url:
            logger.info("%s not set, skipping Teams notification", self.webhook_env)
            return
        text = _capped(text)
        content: dict = {
            "type": "AdaptiveCard",
            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
            "version": "1.4",
            "body": [{"type": "TextBlock", "text": text, "wrap": True}],
        }
        if mentions:
            # A contact with an email is a real @mention (the email is the UPN
            # Teams resolves); one without is named in plain text.
            tags = [f"<at>{c.name}</at>" if c.email else c.name for c in mentions]
            content["body"][0]["text"] = f"{text}\n\n{' '.join(tags)}"
            entities = [
                {
                    "type": "mention",
                    "text": f"<at>{c.name}</at>",
                    "mentioned": {"id": c.email, "name": c.name},
                }
                for c in mentions
                if c.email
            ]
            if entities:
                content["msteams"] = {"entities": entities}
        payload = {
            "type": "message",
            "attachments": [
                {
                    "contentType": "application/vnd.microsoft.card.adaptive",
                    "content": content,
                }
            ],
        }
        try:
            _post_json(url, payload)
        except Exception as exc:
            logger.warning(
                "Teams notification via %s failed to send: %s", self.webhook_env, exc
            )
            return
        logger.info("Teams notification sent via %s", self.webhook_env)


class Detail(Enum):
    """How much a message says. MINIMAL: the outcome, the model count and the
    total time. MODELS: every model with its own run time, and on a failure
    also the models that completed before it. A channel shared by models that
    disagree uses the more detailed level."""

    MINIMAL = "minimal"
    MODELS = "models"


class Mention(Enum):
    """Whether to address the model's `people` (owners and maintainers) in the
    message. ON_FAILURE pings them only when the run raises."""

    NOBODY = "nobody"
    ON_FAILURE = "on_failure"
    ALWAYS = "always"


@dataclass(frozen=True)
class Notifications:
    """Where a model's outcome goes, how much to say, and whom to address.
    `on_any` hears both outcomes; `on_success` and `on_failure` override it
    for their outcome, so one channel for everything, two to split, or one
    plus an override all work. Models that share a channel are reported
    together: one message per channel per run. `enabled` is the switch, and
    the `NOTIFICATIONS_ENABLED` env var overrides it for a whole run. A run
    with a schema or table suffix is a dev run and stays quiet unless
    `suffixed_runs` is True."""

    on_any: Channel | None = None
    on_success: Channel | None = None
    on_failure: Channel | None = None
    detail: Detail = Detail.MINIMAL
    mention: Mention = Mention.NOBODY
    enabled: bool = True
    suffixed_runs: bool = False

    def is_enabled(self) -> bool:
        """`enabled`, unless `NOTIFICATIONS_ENABLED` is set: the env var wins
        either way, so a run can be silenced or forced from outside without
        touching the model."""
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


@dataclass
class Outcome:
    """One model's run as `@model_lifecycle` saw it: how long it took and the
    exception, if it raised."""

    model: Model
    seconds: float
    error: BaseException | None = None


class _Outcomes:
    """The outcomes of the current `@load_models` run. Reset per run."""

    def __init__(self) -> None:
        self.items: list[Outcome] = []

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


def _channel(model: Model, attr: str, suffixed: bool = False) -> Channel | None:
    """The model's channel for `attr` (`success_channel` / `failure_channel`),
    or None when it has no notifications, they are disabled, or the run is
    suffixed and the model did not opt in to those. An explicit
    `NOTIFICATIONS_ENABLED=true` lifts the suffix rule too: it means "this run
    reports", whatever kind of run it is."""
    notifications = model.notifications
    if notifications is None or not notifications.is_enabled():
        return None
    if suffixed and not notifications.suffixed_runs and not _enabled_override():
        return None
    return getattr(notifications, attr)


def _grouped(
    outcomes: Iterable[Outcome], attr: str, suffixed: bool = False
) -> list[tuple[Channel, list[Outcome]]]:
    """Outcomes grouped by the channel their model resolves for `attr`
    (`success_channel` / `failure_channel`); models without one, with
    notifications disabled, or not opted in to a suffixed run are left out.
    Grouping is by equality, so two `Teams("X")` are one channel."""
    groups: list[tuple[Channel, list[Outcome]]] = []
    for outcome in outcomes:
        channel = _channel(outcome.model, attr, suffixed)
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
    outcome, owners and maintainers alike, each person once."""
    seen: set[str] = set()
    people: list[Contact] = []
    for model in models:
        notifications = model.notifications
        if notifications is None or not notifications.mentions_on(failure):
            continue
        if model.people is None:
            continue
        for contact in (model.people.owners or ()) + (model.people.maintainers or ()):
            key = (contact.email or contact.name).lower()
            if key not in seen:
                seen.add(key)
                people.append(contact)
    return tuple(people)


def _model_lines(outcomes: Iterable[Outcome]) -> str:
    return "\n".join(
        f"- {o.model.target.full_name}: {_duration(o.seconds)}" for o in outcomes
    )


def send_success(
    tags: str | None = None, schema_suffix: str = "", table_suffix: str = ""
) -> None:
    """After a clean `main()`: one message per `on_success` channel, listing the
    models that completed on it with their run times. A suffixed run only
    reaches models with `suffixed_runs=True`."""
    suffixed = bool(schema_suffix or table_suffix)
    completed = (o for o in OUTCOMES.items if o.error is None)
    for channel, outcomes in _grouped(completed, "success_channel", suffixed):
        total = _duration(sum(o.seconds for o in outcomes))
        summary = f"{len(outcomes)} model(s) done in {total}"
        text = f"{_header('success', tags)}\n\n"
        if _detail(outcomes) is Detail.MODELS:
            text += f"{_model_lines(outcomes)}\n\n"
        channel.send(
            text + summary, _mentions((o.model for o in outcomes), failure=False)
        )


def send_failure(
    exc: BaseException,
    runs: Iterable[ModelRun],
    tags: str | None = None,
    schema_suffix: str = "",
    table_suffix: str = "",
) -> None:
    """After a `main()` that raised: one message per `on_failure` channel. A
    model that raised reports on its own channel with its exception. When the
    run failed outside any model (a connection that never opened, say), every
    matched model's failure channel hears it instead. A suffixed run only
    reaches models with `suffixed_runs=True`."""
    suffixed = bool(schema_suffix or table_suffix)
    header = _header("failure", tags)
    failed = [o for o in OUTCOMES.items if o.error is not None]
    completed = [o for o in OUTCOMES.items if o.error is None]
    if failed:
        for channel, outcomes in _grouped(failed, "failure_channel", suffixed):
            lines = [
                f"- {o.model.target.full_name} after {_duration(o.seconds)}: "
                f"{type(o.error).__name__}: {o.error}"
                for o in outcomes
            ]
            text = f"{header}\n\n" + "\n".join(lines)
            if completed and _detail(outcomes) is Detail.MODELS:
                text += f"\n\nDone before the failure:\n{_model_lines(completed)}"
            channel.send(text, _mentions((o.model for o in outcomes), failure=True))
        return
    by_channel: list[tuple[Channel, list[Model]]] = []
    for run in runs:
        channel = _channel(run.model, "failure_channel", suffixed)
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
    "Teams",
    "Outcome",
    "OUTCOMES",
]
