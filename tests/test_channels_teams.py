"""The Microsoft Teams channel: the webhook env var, the Adaptive Card, the
size cap and the mentions."""

import logging

from bollhav.channels import MicrosoftTeams, teams
from bollhav.model.people import Contact

ERIK = Contact(name="Erik", email="erik@example.test")
ANNA = Contact(name="Anna", email="anna@example.test")
NO_EMAIL = Contact(name="Ops desk")


# ── Teams ──


def test_teams_is_silent_without_the_env_var(monkeypatch, caplog):
    monkeypatch.delenv("HOOK", raising=False)
    posted = []
    monkeypatch.setattr(
        teams, "_post_json", lambda url, payload, timeout=10.0: posted.append(url)
    )
    with caplog.at_level(logging.INFO):
        MicrosoftTeams(webhook_env="HOOK").send("hello")
    assert posted == []
    assert "HOOK not set" in caplog.text


def test_teams_posts_an_adaptive_card_to_the_url_in_the_env_var(monkeypatch):
    monkeypatch.setenv("HOOK", "https://example.test/webhook")
    posted = []
    monkeypatch.setattr(
        teams,
        "_post_json",
        lambda url, payload, timeout=10.0: posted.append((url, payload)),
    )
    MicrosoftTeams(webhook_env="HOOK").send("hello **world**")
    ((url, payload),) = posted
    assert url == "https://example.test/webhook"
    card = payload["attachments"][0]["content"]
    assert card["type"] == "AdaptiveCard"
    assert card["body"] == [
        {"type": "TextBlock", "text": "hello **world**", "wrap": True}
    ]


def test_teams_logs_a_sent_notification(monkeypatch, caplog):
    monkeypatch.setenv("HOOK", "https://example.test/webhook")
    monkeypatch.setattr(teams, "_post_json", lambda url, payload, timeout=10.0: None)
    with caplog.at_level(logging.INFO):
        MicrosoftTeams(webhook_env="HOOK").send("hello")
    assert "Teams notification sent via HOOK" in caplog.text


def test_teams_send_failure_is_logged_not_raised(monkeypatch, caplog):
    monkeypatch.setenv("HOOK", "https://example.test/webhook")

    def boom(url, payload, timeout=10.0):
        raise OSError("connection refused")

    monkeypatch.setattr(teams, "_post_json", boom)
    with caplog.at_level(logging.WARNING):
        MicrosoftTeams(webhook_env="HOOK").send("hello")
    assert "failed to send" in caplog.text


def test_teams_caps_an_oversized_message_in_the_middle(monkeypatch):
    monkeypatch.setenv("HOOK", "https://example.test/webhook")
    posted = []
    monkeypatch.setattr(
        teams, "_post_json", lambda url, payload, timeout=10.0: posted.append(payload)
    )
    text = "HEAD " + "x" * 60_000 + " TAIL"
    MicrosoftTeams(webhook_env="HOOK").send(text)
    sent = posted[0]["attachments"][0]["content"]["body"][0]["text"]
    assert len(sent.encode("utf-8")) <= teams.TEAMS_MAX_BYTES
    assert sent.startswith("HEAD ") and sent.endswith(" TAIL")
    assert "message capped for Teams" in sent and "bytes cut here" in sent


def _sent_payloads(monkeypatch) -> list:
    monkeypatch.setenv("HOOK", "https://example.test/webhook")
    posted: list = []
    monkeypatch.setattr(
        teams, "_post_json", lambda url, payload, timeout=10.0: posted.append(payload)
    )
    return posted


def test_the_whole_payload_stays_under_the_limit_with_many_mentions(monkeypatch):
    posted = _sent_payloads(monkeypatch)
    crowd = tuple(
        Contact(name=f"Person Number {i}", email=f"person{i}@example.test")
        for i in range(50)
    )
    MicrosoftTeams(webhook_env="HOOK").send("x" * 100_000, mentions=crowd)
    assert len(teams._json_bytes(posted[0])) <= teams.TEAMS_MAX_BYTES
    card = posted[0]["attachments"][0]["content"]
    assert len(card["msteams"]["entities"]) == 50
    assert card["body"][0]["text"].endswith("<at>Person Number 49</at>")
    assert "message capped for Teams" in card["body"][0]["text"]


def test_the_whole_payload_stays_under_the_limit_with_swedish_text(monkeypatch):
    posted = _sent_payloads(monkeypatch)
    MicrosoftTeams(webhook_env="HOOK").send("å" * 60_000)
    assert len(teams._json_bytes(posted[0])) <= teams.TEAMS_MAX_BYTES
    assert (
        "message capped for Teams"
        in posted[0]["attachments"][0]["content"]["body"][0]["text"]
    )


def test_a_fitting_message_is_sent_untouched_even_with_mentions(monkeypatch):
    posted = _sent_payloads(monkeypatch)
    MicrosoftTeams(webhook_env="HOOK").send(
        "hello " + "ä" * 5_000, mentions=(ERIK, ANNA)
    )
    text = posted[0]["attachments"][0]["content"]["body"][0]["text"]
    assert text == "hello " + "ä" * 5_000 + "\n\n<at>Erik</at> <at>Anna</at>"


def test_json_goes_on_the_wire_as_utf8_not_ascii_escapes():
    assert teams._json_bytes({"t": "ä"}) == '{"t": "ä"}'.encode("utf-8")


def test_capped_leaves_a_fitting_message_alone_and_respects_utf8():
    assert teams._capped("short", limit=100) == "short"
    capped = teams._capped("ä" * 30_000, limit=1_000)
    assert len(capped.encode("utf-8")) <= 1_000
    assert capped.startswith("ä") and capped.endswith("ä")
    assert teams._capped("x" * 500, limit=0).strip().startswith("… message capped")


def test_teams_channels_with_the_same_env_var_are_equal():
    assert MicrosoftTeams("HOOK") == MicrosoftTeams("HOOK")
    assert MicrosoftTeams("HOOK") != MicrosoftTeams("OTHER")


def test_teams_renders_mentions_as_entities_and_names_the_emailless(monkeypatch):
    monkeypatch.setenv("HOOK", "https://example.test/webhook")
    posted = []
    monkeypatch.setattr(
        teams, "_post_json", lambda url, payload, timeout=10.0: posted.append(payload)
    )
    MicrosoftTeams(webhook_env="HOOK").send("broke", mentions=(ERIK, NO_EMAIL))
    card = posted[0]["attachments"][0]["content"]
    assert card["body"][0]["text"] == "broke\n\n<at>Erik</at> Ops desk"
    assert card["msteams"]["entities"] == [
        {
            "type": "mention",
            "text": "<at>Erik</at>",
            "mentioned": {"id": "erik@example.test", "name": "Erik"},
        }
    ]
