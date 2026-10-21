"""Notifications: the Teams channel, the per-run outcome registry, and the
one-message-per-channel grouping that load_models sends from."""

import logging

import pytest

from bollhav.model import notifications as n
from bollhav.model.model import Model
from bollhav.model.modelrun import ModelRun
from bollhav.model.notifications import Detail, Notifications, Teams
from bollhav.model.target import Target
from bollhav.model.temporality import Temporality


class Recorder:
    """A channel that keeps what it was asked to send."""

    def __init__(self) -> None:
        self.sent: list[str] = []

    def send(self, text: str) -> None:
        self.sent.append(text)


def _model(name: str, notifications: Notifications | None) -> Model:
    return Model(
        target=Target(name=name, schema="warehouse"),
        temporality=Temporality.TIMELESS,
        notifications=notifications,
    )


@pytest.fixture(autouse=True)
def _fresh_outcomes():
    n.OUTCOMES.reset()
    yield
    n.OUTCOMES.reset()


# ── Teams ──


def test_teams_is_silent_without_the_env_var(monkeypatch, caplog):
    monkeypatch.delenv("HOOK", raising=False)
    posted = []
    monkeypatch.setattr(
        n, "_post_json", lambda url, payload, timeout=10.0: posted.append(url)
    )
    with caplog.at_level(logging.INFO):
        Teams(webhook_env="HOOK").send("hello")
    assert posted == []
    assert "HOOK not set" in caplog.text


def test_teams_posts_an_adaptive_card_to_the_url_in_the_env_var(monkeypatch):
    monkeypatch.setenv("HOOK", "https://example.test/webhook")
    posted = []
    monkeypatch.setattr(
        n,
        "_post_json",
        lambda url, payload, timeout=10.0: posted.append((url, payload)),
    )
    Teams(webhook_env="HOOK").send("hello **world**")
    ((url, payload),) = posted
    assert url == "https://example.test/webhook"
    card = payload["attachments"][0]["content"]
    assert card["type"] == "AdaptiveCard"
    assert card["body"] == [
        {"type": "TextBlock", "text": "hello **world**", "wrap": True}
    ]


def test_teams_send_failure_is_logged_not_raised(monkeypatch, caplog):
    monkeypatch.setenv("HOOK", "https://example.test/webhook")

    def boom(url, payload, timeout=10.0):
        raise OSError("connection refused")

    monkeypatch.setattr(n, "_post_json", boom)
    with caplog.at_level(logging.WARNING):
        Teams(webhook_env="HOOK").send("hello")
    assert "failed to send" in caplog.text


def test_teams_channels_with_the_same_env_var_are_equal():
    assert Teams("HOOK") == Teams("HOOK")
    assert Teams("HOOK") != Teams("OTHER")


# ── the model ──


def test_model_carries_notifications_and_defaults_to_none():
    assert _model("orders", None).notifications is None
    ok = Recorder()
    model = _model("orders", Notifications(on_success=ok))
    assert model.notifications.on_success is ok
    assert model.notifications.on_failure is None


def test_on_any_covers_both_outcomes_and_the_specific_ones_override_it():
    both, errors = Recorder(), Recorder()
    only = Notifications(on_any=both)
    assert only.success_channel is both and only.failure_channel is both
    split = Notifications(on_any=both, on_failure=errors)
    assert split.success_channel is both and split.failure_channel is errors
    assert Notifications().success_channel is None


def test_on_any_hears_a_success_and_a_failure(monkeypatch):
    both = Recorder()
    model = _model("orders", Notifications(on_any=both))
    n.OUTCOMES.record(model, 3.0)
    n.send_success()
    n.OUTCOMES.reset()
    n.OUTCOMES.record(model, 3.0, RuntimeError("x"))
    n.send_failure(RuntimeError("x"), runs=[ModelRun(model=model)])
    assert [t.split(":")[0] for t in both.sent] == [
        "Pipeline success",
        "Pipeline failure",
    ]


# ── success ──


def test_minimal_success_has_tags_and_the_summary_but_no_model_lines(monkeypatch):
    monkeypatch.setenv("HOSTNAME", "daily-x-123")
    ok = Recorder()
    n.OUTCOMES.record(_model("orders", Notifications(on_success=ok)), 65.0)
    n.OUTCOMES.record(_model("customers", Notifications(on_success=ok)), 10.0)
    n.send_success(tags="[all]")
    (text,) = ok.sent
    assert text == (
        "Pipeline success: **daily-x-123**\n\nTAGS: `[all]`\n\n2 model(s) done in 1m 15s"
    )


def test_a_channel_is_detailed_when_any_of_its_models_asks_for_it():
    ok = Recorder()
    n.OUTCOMES.record(_model("orders", Notifications(on_success=ok)), 1.0)
    n.OUTCOMES.record(
        _model("customers", Notifications(on_success=ok, detail=Detail.MODELS)), 2.0
    )
    n.send_success()
    assert "- warehouse.orders: 0m 1s" in ok.sent[0]


def test_success_sends_one_message_per_channel_listing_its_models(monkeypatch):
    monkeypatch.setenv("HOSTNAME", "daily-x-123")
    a, b = Recorder(), Recorder()
    detailed = lambda ch: Notifications(on_success=ch, detail=Detail.MODELS)  # noqa: E731
    n.OUTCOMES.record(_model("orders", detailed(a)), 65.0)
    n.OUTCOMES.record(_model("customers", detailed(a)), 10.0)
    n.OUTCOMES.record(_model("audit", detailed(b)), 1.0)
    n.OUTCOMES.record(_model("silent", None), 1.0)
    n.send_success(tags="[all]")
    assert len(a.sent) == 1 and len(b.sent) == 1
    text = a.sent[0]
    assert text.startswith("Pipeline success: **daily-x-123**")
    assert "TAGS: `[all]`" in text
    assert "- warehouse.orders: 1m 5s" in text
    assert "- warehouse.customers: 0m 10s" in text
    assert "2 model(s) done in 1m 15s" in text
    assert "audit" not in text
    assert "warehouse.audit" in b.sent[0]


def test_success_skips_models_that_failed():
    ok = Recorder()
    n.OUTCOMES.record(
        _model("orders", Notifications(on_success=ok)), 1.0, RuntimeError("x")
    )
    n.send_success()
    assert ok.sent == []


# ── failure ──


def test_failure_reports_the_raising_model_on_its_own_channel():
    errors = Recorder()
    model = _model("orders", Notifications(on_failure=errors))
    n.OUTCOMES.record(model, 12.0, ValueError("bad rows"))
    n.send_failure(
        ValueError("bad rows"), runs=[ModelRun(model=model)], tags="[orders]"
    )
    (text,) = errors.sent
    assert text.startswith("Pipeline failure: **")
    assert "- warehouse.orders after 0m 12s: ValueError: bad rows" in text


def test_detailed_failure_also_lists_what_completed_before_it():
    errors = Recorder()
    done = _model("orders", Notifications(on_failure=errors, detail=Detail.MODELS))
    bad = _model("customers", Notifications(on_failure=errors, detail=Detail.MODELS))
    n.OUTCOMES.record(done, 5.0)
    n.OUTCOMES.record(bad, 2.0, RuntimeError("x"))
    n.send_failure(RuntimeError("x"), runs=[ModelRun(model=done), ModelRun(model=bad)])
    (text,) = errors.sent
    assert "- warehouse.customers after 0m 2s: RuntimeError: x" in text
    assert "Done before the failure:\n- warehouse.orders: 0m 5s" in text


def test_minimal_failure_does_not_list_the_completed_models():
    errors = Recorder()
    done = _model("orders", Notifications(on_failure=errors))
    bad = _model("customers", Notifications(on_failure=errors))
    n.OUTCOMES.record(done, 5.0)
    n.OUTCOMES.record(bad, 2.0, RuntimeError("x"))
    n.send_failure(RuntimeError("x"), runs=[ModelRun(model=done), ModelRun(model=bad)])
    assert "Done before" not in errors.sent[0]


def test_failure_outside_any_model_reaches_every_failure_channel_once():
    errors = Recorder()
    runs = [
        ModelRun(model=_model("orders", Notifications(on_failure=errors))),
        ModelRun(model=_model("customers", Notifications(on_failure=errors))),
        ModelRun(model=_model("silent", None)),
    ]
    n.send_failure(ConnectionError("no state db"), runs=runs)
    (text,) = errors.sent
    assert "ConnectionError: no state db" in text


def test_failure_sends_nothing_without_failure_channels():
    ok = Recorder()
    model = _model("orders", Notifications(on_success=ok))
    n.OUTCOMES.record(model, 1.0, RuntimeError("x"))
    n.send_failure(RuntimeError("x"), runs=[ModelRun(model=model)])
    assert ok.sent == []


# ── duration formatting ──


@pytest.mark.parametrize(
    "seconds, text",
    [(0, "0m 0s"), (59.9, "0m 59s"), (754, "12m 34s"), (3725, "1h 2m 5s")],
)
def test_duration(seconds, text):
    assert n._duration(seconds) == text
