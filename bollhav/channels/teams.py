"""The Microsoft Teams channel: an incoming webhook that gets an Adaptive
Card per run outcome. See `bollhav.model.notifications` for what is sent and
when; this module only knows how to deliver a message to Teams."""

from __future__ import annotations

import json
import logging
import os
import urllib.request
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from bollhav.model.people import Contact

logger = logging.getLogger(__name__)


TEAMS_MAX_BYTES = 26_000
"""Teams rejects a webhook message above about 28 KB of JSON. The whole
payload, card, mentions and envelope, is kept under this."""


def _capped(text: str, limit: int) -> str:
    """`text` untouched when it fits in `limit` bytes of UTF-8. Otherwise its
    head and tail with the middle cut out, and a note in the gap saying how
    much went: the start names the run and the model, the end has the last
    lines of the error, and the middle is the part nobody reads in a card."""
    data = text.encode("utf-8")
    if len(data) <= limit:
        return text
    note = "\n\n… message capped for Teams, {cut:,} bytes cut here …\n\n"
    budget = max(limit - len(note.format(cut=len(data)).encode("utf-8")), 0)
    head_len, tail_len = budget * 2 // 3, budget // 3
    head = data[:head_len].decode("utf-8", errors="ignore")
    tail = data[len(data) - tail_len :].decode("utf-8", errors="ignore")
    return head + note.format(cut=len(data) - head_len - tail_len) + tail


def _json_bytes(payload: dict) -> bytes:
    """The payload as it goes on the wire: UTF-8, not ASCII escapes, so an
    `ä` costs two bytes rather than six and the size check means something."""
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")


def _post_json(url: str, payload: dict, timeout: float = 10.0) -> None:
    request = urllib.request.Request(
        url,
        data=_json_bytes(payload),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        response.read()


@dataclass(frozen=True)
class MicrosoftTeams:
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
        # A contact with an email is a real @mention (the email is the UPN
        # Teams resolves); one without is named in plain text.
        tags = [f"<at>{c.name}</at>" if c.email else c.name for c in mentions]
        entities = [
            {
                "type": "mention",
                "text": f"<at>{c.name}</at>",
                "mentioned": {"id": c.email, "name": c.name},
            }
            for c in mentions
            if c.email
        ]
        suffix = f"\n\n{' '.join(tags)}" if tags else ""

        def payload_for(body: str) -> dict:
            content: dict = {
                "type": "AdaptiveCard",
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "version": "1.4",
                "body": [{"type": "TextBlock", "text": body, "wrap": True}],
            }
            if entities:
                content["msteams"] = {"entities": entities}
            return {
                "type": "message",
                "attachments": [
                    {
                        "contentType": "application/vnd.microsoft.card.adaptive",
                        "content": content,
                    }
                ],
            }

        # Budget the text so the whole payload fits: the envelope, the mentions
        # and JSON escaping all cost bytes on top of the text itself, so trim
        # the text by the overshoot and measure again. Two rounds in practice.
        budget = TEAMS_MAX_BYTES
        payload = payload_for(_capped(text, budget) + suffix)
        for _ in range(4):
            excess = len(_json_bytes(payload)) - TEAMS_MAX_BYTES
            if excess <= 0 or budget <= 0:
                break
            budget -= excess + 16
            payload = payload_for(_capped(text, max(budget, 0)) + suffix)
        try:
            _post_json(url, payload)
        except Exception as exc:
            logger.warning(
                "Teams notification via %s failed to send: %s", self.webhook_env, exc
            )
            return
        logger.info("Teams notification sent via %s", self.webhook_env)


__all__ = ["MicrosoftTeams", "TEAMS_MAX_BYTES"]
