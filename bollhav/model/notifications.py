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

logger = logging.getLogger(__name__)


class Channel(Protocol):
    """Anything that can deliver a text message. `Teams` is the one shipped;
    a custom channel only needs `send`."""

    def send(self, text: str) -> None: ...


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
    send is logged, never raised — a notification must not fail a run."""

    webhook_env: str

    def send(self, text: str) -> None:
        url = os.environ.get(self.webhook_env)
        if not url:
            logger.info("%s not set, skipping Teams notification", self.webhook_env)
            return
        payload = {
            "type": "message",
            "attachments": [
                {
                    "contentType": "application/vnd.microsoft.card.adaptive",
                    "content": {
                        "type": "AdaptiveCard",
                        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                        "version": "1.4",
                        "body": [{"type": "TextBlock", "text": text, "wrap": True}],
                    },
                }
            ],
        }
        try:
            _post_json(url, payload)
        except Exception as exc:
            logger.warning(
                "Teams notification via %s failed to send: %s", self.webhook_env, exc
            )


class Detail(Enum):
    """How much a message says. MINIMAL: the outcome, the model count and the
    total time. MODELS: every model with its own run time, and on a failure
    also the models that completed before it. A channel shared by models that
    disagree uses the more detailed level."""

    MINIMAL = "minimal"
    MODELS = "models"


@dataclass(frozen=True)
class Notifications:
    """Where a model's outcome goes, and how much to say. `on_any` hears both
    outcomes; `on_success` and `on_failure` override it for their outcome, so
    one channel for everything, two to split, or one plus an override all
    work. Models that share a channel are reported together: one message per
    channel per run."""

    on_any: Channel | None = None
    on_success: Channel | None = None
    on_failure: Channel | None = None
    detail: Detail = Detail.MINIMAL

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


def _grouped(
    outcomes: Iterable[Outcome], attr: str
) -> list[tuple[Channel, list[Outcome]]]:
    """Outcomes grouped by the channel their model resolves for `attr`
    (`success_channel` / `failure_channel`); models without one are left out.
    Grouping is by equality, so two `Teams("X")` are one channel."""
    groups: list[tuple[Channel, list[Outcome]]] = []
    for outcome in outcomes:
        notifications = outcome.model.notifications
        channel = getattr(notifications, attr, None) if notifications else None
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


def _model_lines(outcomes: Iterable[Outcome]) -> str:
    return "\n".join(
        f"- {o.model.target.full_name}: {_duration(o.seconds)}" for o in outcomes
    )


def send_success(tags: str | None = None) -> None:
    """After a clean `main()`: one message per `on_success` channel, listing the
    models that completed on it with their run times."""
    completed = (o for o in OUTCOMES.items if o.error is None)
    for channel, outcomes in _grouped(completed, "success_channel"):
        total = _duration(sum(o.seconds for o in outcomes))
        summary = f"{len(outcomes)} model(s) done in {total}"
        text = f"{_header('success', tags)}\n\n"
        if _detail(outcomes) is Detail.MODELS:
            text += f"{_model_lines(outcomes)}\n\n"
        channel.send(text + summary)


def send_failure(
    exc: BaseException, runs: Iterable[ModelRun], tags: str | None = None
) -> None:
    """After a `main()` that raised: one message per `on_failure` channel. A
    model that raised reports on its own channel with its exception. When the
    run failed outside any model (a connection that never opened, say), every
    matched model's failure channel hears it instead."""
    failed = [o for o in OUTCOMES.items if o.error is not None]
    completed = [o for o in OUTCOMES.items if o.error is None]
    if failed:
        for channel, outcomes in _grouped(failed, "failure_channel"):
            lines = [
                f"- {o.model.target.full_name} after {_duration(o.seconds)}: "
                f"{type(o.error).__name__}: {o.error}"
                for o in outcomes
            ]
            text = f"{_header('failure', tags)}\n\n" + "\n".join(lines)
            if completed and _detail(outcomes) is Detail.MODELS:
                text += f"\n\nDone before the failure:\n{_model_lines(completed)}"
            channel.send(text)
        return
    channels: list[Channel] = []
    for run in runs:
        notifications = run.model.notifications
        channel = notifications.failure_channel if notifications else None
        if channel is not None and channel not in channels:
            channels.append(channel)
    for channel in channels:
        channel.send(f"{_header('failure', tags)}\n\n{type(exc).__name__}: {exc}")


__all__ = ["Channel", "Detail", "Notifications", "Teams", "Outcome", "OUTCOMES"]
