"""Notifications: the run-level outcome report that `@load_models` sends, one
message per outcome. The Teams channel has its own tests."""

import logging

import pytest

from bollhav.channels import MicrosoftTeams
from bollhav.model import notifications as n
from bollhav.model.model import Model
from bollhav.model.modelrun import ModelRun, Outcome, duration_string
from bollhav.model.notifications import (
    Detail,
    Mention,
    Notifications,
    NotificationsError,
    default_notifications,
)
from bollhav.model.people import Contact, People
from bollhav.model.target import Target
from bollhav.model.temporality import Temporality


class Recorder:
    """A channel that keeps what it was asked to send, and whom to mention."""

    def __init__(self) -> None:
        self.sent: list[str] = []
        self.mentions: list[tuple[Contact, ...]] = []

    def send(self, text: str, mentions: tuple[Contact, ...] = ()) -> None:
        self.sent.append(text)
        self.mentions.append(mentions)


ERIK = Contact(name="Erik", email="erik@example.test")
ANNA = Contact(name="Anna", email="anna@example.test")
NO_EMAIL = Contact(name="Ops desk")
BOT = Contact(name="Build bot", email="bot@example.test")


def _model(name: str, people: People | None = None, suffixed: bool = False) -> Model:
    """`suffixed` is what the runtime stamps on its copy for a SCHEMA_SUFFIX /
    TABLE_SUFFIX run; a Model is frozen, so it is set at construction."""
    return Model(
        target=Target(name=name, schema="warehouse"),
        temporality=Temporality.TIMELESS,
        people=people,
        suffixed=suffixed,
    )


def _run(
    model: Model, seconds: float | None = None, error: BaseException | None = None
) -> ModelRun:
    """A ModelRun as `@model_lifecycle` leaves it: with an outcome once the
    model ran (`seconds`), carrying the exception if it raised; without one
    when the model was never reached."""
    run = ModelRun(model=model)
    if seconds is not None:
        run.outcome = Outcome(seconds=seconds, error=error)
    return run


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch, env_var_bool_from_environ):
    monkeypatch.delenv("NOTIFICATIONS_ENABLED", raising=False)
    monkeypatch.delenv("TEAMS_WEBHOOK_ERRORS", raising=False)


# ── the default ──


def test_the_default_reports_failures_to_teams_and_nothing_else():
    failure = default_notifications.failure_channel
    assert isinstance(failure, MicrosoftTeams)
    assert failure.webhook_env == "TEAMS_WEBHOOK_ERRORS"
    assert default_notifications.success_channel is None
    assert default_notifications.detail is Detail.MINIMAL
    assert default_notifications.mention is Mention.NOBODY
    assert default_notifications.enabled is True


def test_the_default_never_sends_a_success_card(monkeypatch, caplog):
    monkeypatch.setenv("TEAMS_WEBHOOK_SUCCESS", "https://example.test/success")
    with caplog.at_level(logging.INFO):
        n.send_success(default_notifications, [_run(_model("orders"), 1.0)])
    assert "Teams" not in caplog.text


def test_the_default_is_quiet_while_the_errors_webhook_is_unset(caplog):
    exc = RuntimeError("x")
    with caplog.at_level(logging.INFO):
        n.send_failure(default_notifications, exc, [_run(_model("orders"), 1.0, exc)])
    assert "TEAMS_WEBHOOK_ERRORS not set, skipping Teams notification" in caplog.text


def test_none_never_reports():
    model = _model("orders")
    n.send_success(None, [_run(model, 1.0)])
    n.send_failure(None, RuntimeError("x"), [_run(model, 1.0, RuntimeError("x"))])
    n.send_failure(None, ConnectionError("no db"), [_run(model)])


# ── channels ──


def test_on_any_covers_both_outcomes_and_the_specific_ones_override_it():
    both, errors = Recorder(), Recorder()
    only = Notifications(on_any=both)
    assert only.success_channel is both and only.failure_channel is both
    split = Notifications(on_any=both, on_failure=errors)
    assert split.success_channel is both and split.failure_channel is errors
    assert Notifications().success_channel is None


def test_on_any_hears_a_success_and_a_failure():
    both = Recorder()
    notif = Notifications(on_any=both)
    model = _model("orders")
    n.send_success(notif, [_run(model, 3.0)])
    n.send_failure(notif, RuntimeError("x"), [_run(model, 3.0, RuntimeError("x"))])
    assert [t.split(":")[0] for t in both.sent] == [
        "Pipeline success",
        "Pipeline failure",
    ]


def test_a_run_without_an_outcome_never_counts_as_done():
    ok = Recorder()
    n.send_success(Notifications(on_success=ok), [_run(_model("orders"))])
    assert ok.sent == []


# ── enabled ──


def test_disabled_notifications_send_nothing():
    both = Recorder()
    notif = Notifications(on_any=both, enabled=False)
    model = _model("orders")
    n.send_success(notif, [_run(model, 1.0)])
    n.send_failure(notif, RuntimeError("x"), [_run(model, 1.0, RuntimeError("x"))])
    n.send_failure(notif, ConnectionError("no db"), [_run(model)])
    assert both.sent == []


def test_env_var_false_silences_the_run(monkeypatch):
    monkeypatch.setenv("NOTIFICATIONS_ENABLED", "False")
    ok = Recorder()
    n.send_success(Notifications(on_success=ok), [_run(_model("orders"), 1.0)])
    assert ok.sent == []


def test_env_var_true_forces_a_disabled_run_on(monkeypatch):
    monkeypatch.setenv("NOTIFICATIONS_ENABLED", "true")
    ok = Recorder()
    notif = Notifications(on_success=ok, enabled=False)
    n.send_success(notif, [_run(_model("orders"), 1.0)])
    assert len(ok.sent) == 1


def test_env_var_garbage_is_ignored_with_a_warning(monkeypatch, caplog):
    monkeypatch.setenv("NOTIFICATIONS_ENABLED", "yes")
    ok = Recorder()
    with caplog.at_level(logging.WARNING):
        n.send_success(Notifications(on_success=ok), [_run(_model("orders"), 1.0)])
    assert len(ok.sent) == 1
    assert "NOTIFICATIONS_ENABLED must be true or false" in caplog.text


def test_an_empty_env_var_counts_as_unset(monkeypatch):
    monkeypatch.setenv("NOTIFICATIONS_ENABLED", "")
    assert Notifications(enabled=False).is_enabled() is False
    assert Notifications().is_enabled() is True


# ── suffixed runs ──


def test_a_suffixed_run_is_quiet_by_default():
    both = Recorder()
    notif = Notifications(on_any=both)
    model = _model("orders", suffixed=True)
    n.send_success(notif, [_run(model, 1.0)])
    n.send_failure(notif, RuntimeError("x"), [_run(model, 1.0, RuntimeError("x"))])
    n.send_failure(notif, ConnectionError("no db"), [_run(model)])
    assert both.sent == []


def test_env_var_true_lifts_the_suffix_rule_for_the_run(monkeypatch):
    monkeypatch.setenv("NOTIFICATIONS_ENABLED", "true")
    ok = Recorder()
    model = _model("orders", suffixed=True)
    n.send_success(Notifications(on_success=ok), [_run(model, 1.0)])
    assert len(ok.sent) == 1


def test_suffixed_runs_opt_in_and_the_card_reads_as_usual(monkeypatch):
    monkeypatch.setenv("HOSTNAME", "dev-1")
    both = Recorder()
    notif = Notifications(on_any=both, suffixed_runs=True)
    model = _model("orders", suffixed=True)
    n.send_success(notif, [_run(model, 1.0)], tags="[orders]")
    (text,) = both.sent
    assert text == (
        "Pipeline success: **dev-1**\n\nTAGS: `[orders]`\n\n1 model(s) done in 0m 1s"
    )


def test_suffixed_opt_in_covers_failures_too():
    errors = Recorder()
    notif = Notifications(on_failure=errors, suffixed_runs=True)
    model = _model("orders", suffixed=True)
    n.send_failure(notif, RuntimeError("x"), [_run(model, 2.0, RuntimeError("x"))])
    n.send_failure(notif, ConnectionError("no db"), [_run(model)])
    assert len(errors.sent) == 2
    assert "RuntimeError: x" in errors.sent[0]
    assert "ConnectionError: no db" in errors.sent[1]


def test_env_var_false_beats_the_suffixed_opt_in(monkeypatch):
    monkeypatch.setenv("NOTIFICATIONS_ENABLED", "false")
    both = Recorder()
    notif = Notifications(on_any=both, suffixed_runs=True)
    n.send_success(notif, [_run(_model("orders", suffixed=True), 1.0)])
    n.send_success(notif, [_run(_model("orders"), 1.0)])
    assert both.sent == []


# ── success ──


def test_minimal_success_has_tags_and_the_summary_but_no_model_lines(monkeypatch):
    monkeypatch.setenv("HOSTNAME", "daily-x-123")
    ok = Recorder()
    n.send_success(
        Notifications(on_success=ok),
        [_run(_model("orders"), 65.0), _run(_model("customers"), 10.0)],
        tags="[all]",
    )
    (text,) = ok.sent
    assert text == (
        "Pipeline success: **daily-x-123**\n\nTAGS: `[all]`\n\n2 model(s) done in 1m 15s"
    )


def test_detailed_success_is_one_message_listing_every_completed_model(monkeypatch):
    monkeypatch.setenv("HOSTNAME", "daily-x-123")
    ok = Recorder()
    n.send_success(
        Notifications(on_success=ok, detail=Detail.MODELS),
        [
            _run(_model("orders"), 65.0),
            _run(_model("customers"), 10.0),
            _run(_model("skipped")),
        ],
        tags="[all]",
    )
    (text,) = ok.sent
    assert text.startswith("Pipeline success: **daily-x-123**")
    assert "TAGS: `[all]`" in text
    assert "- warehouse.orders: 1m 5s" in text
    assert "- warehouse.customers: 0m 10s" in text
    assert "2 model(s) done in 1m 15s" in text
    assert "skipped" not in text


def test_success_skips_models_that_failed():
    ok = Recorder()
    n.send_success(
        Notifications(on_success=ok),
        [_run(_model("orders"), 1.0, RuntimeError("x"))],
    )
    assert ok.sent == []


def test_success_with_nothing_completed_sends_nothing():
    ok = Recorder()
    n.send_success(Notifications(on_success=ok), [_run(_model("orders"))])
    n.send_success(Notifications(on_success=ok), [])
    assert ok.sent == []


# ── failure ──


def test_failure_reports_the_raising_model():
    errors = Recorder()
    n.send_failure(
        Notifications(on_failure=errors),
        ValueError("bad rows"),
        [_run(_model("orders"), 12.0, ValueError("bad rows"))],
        tags="[orders]",
    )
    (text,) = errors.sent
    assert text.startswith("Pipeline failure: **")
    assert "TAGS: `[orders]`" in text
    assert "- warehouse.orders after 0m 12s: ValueError: bad rows" in text


def test_detailed_failure_also_lists_what_completed_before_it():
    errors = Recorder()
    n.send_failure(
        Notifications(on_failure=errors, detail=Detail.MODELS),
        RuntimeError("x"),
        [
            _run(_model("orders"), 5.0),
            _run(_model("customers"), 2.0, RuntimeError("x")),
        ],
    )
    (text,) = errors.sent
    assert "- warehouse.customers after 0m 2s: RuntimeError: x" in text
    assert "Done before the failure:\n- warehouse.orders: 0m 5s" in text


def test_minimal_failure_does_not_list_the_completed_models():
    errors = Recorder()
    n.send_failure(
        Notifications(on_failure=errors),
        RuntimeError("x"),
        [
            _run(_model("orders"), 5.0),
            _run(_model("customers"), 2.0, RuntimeError("x")),
        ],
    )
    assert "Done before" not in errors.sent[0]


def test_failure_outside_any_model_reports_the_exception_once():
    errors = Recorder()
    n.send_failure(
        Notifications(on_failure=errors),
        ConnectionError("no state db"),
        [_run(_model("orders")), _run(_model("customers"))],
    )
    (text,) = errors.sent
    assert text.endswith("ConnectionError: no state db")


def test_failure_before_any_model_was_matched_still_reports():
    errors = Recorder()
    n.send_failure(Notifications(on_failure=errors), ConnectionError("no db"), [])
    (text,) = errors.sent
    assert "ConnectionError: no db" in text


def test_failure_sends_nothing_without_a_failure_channel():
    ok = Recorder()
    n.send_failure(
        Notifications(on_success=ok),
        RuntimeError("x"),
        [_run(_model("orders"), 1.0, RuntimeError("x"))],
    )
    assert ok.sent == []


def test_failure_names_the_runs_own_exception_when_a_model_error_was_caught():
    errors = Recorder()
    n.send_failure(
        Notifications(on_failure=errors),
        ConnectionError("db went away"),
        [_run(_model("orders"), 1.0, RuntimeError("locked, skipped"))],
    )
    (text,) = errors.sent
    assert "- warehouse.orders after 0m 1s: RuntimeError: locked, skipped" in text
    assert "The run then failed with ConnectionError: db went away" in text


def test_failure_does_not_repeat_the_exception_that_is_the_models_own():
    errors = Recorder()
    exc = RuntimeError("x")
    n.send_failure(
        Notifications(on_failure=errors), exc, [_run(_model("orders"), 1.0, exc)]
    )
    assert "The run then failed" not in errors.sent[0]


def test_send_quietly_logs_and_swallows(caplog):
    def bad() -> None:
        raise RuntimeError("channel down")

    with caplog.at_level(logging.ERROR):
        n.send_quietly(bad)
    assert "channel down" in caplog.text


# ── mentions ──


def test_nobody_is_mentioned_by_default():
    ok = Recorder()
    model = _model("orders", People(owners=(ERIK,)))
    n.send_success(Notifications(on_success=ok), [_run(model, 1.0)])
    assert ok.mentions == [()]


def test_on_failure_mentions_owners_and_maintainers_only_when_it_fails():
    both = Recorder()
    notif = Notifications(on_any=both, mention=Mention.ON_FAILURE)
    model = _model("orders", People(owners=(ERIK,), maintainers=(ANNA,)))
    n.send_success(notif, [_run(model, 1.0)])
    assert both.mentions == [()]
    n.send_failure(notif, RuntimeError("x"), [_run(model, 1.0, RuntimeError("x"))])
    assert both.mentions[-1] == (ERIK, ANNA)


def test_always_mentions_on_success_and_each_person_once():
    ok = Recorder()
    notif = Notifications(on_success=ok, mention=Mention.ALWAYS)
    n.send_success(
        notif,
        [
            _run(_model("orders", People(owners=(ERIK,))), 1.0),
            _run(_model("customers", People(maintainers=(ERIK, ANNA))), 1.0),
            _run(_model("audit", People(owners=(NO_EMAIL,))), 1.0),
        ],
    )
    assert ok.mentions == [(ERIK, ANNA, NO_EMAIL)]


def test_only_the_models_that_raised_are_mentioned_on_a_failure():
    errors = Recorder()
    notif = Notifications(on_failure=errors, mention=Mention.ON_FAILURE)
    n.send_failure(
        notif,
        RuntimeError("x"),
        [
            _run(_model("orders", People(owners=(ERIK,))), 1.0),
            _run(_model("customers", People(owners=(ANNA,))), 1.0, RuntimeError("x")),
        ],
    )
    assert errors.mentions == [(ANNA,)]


def test_a_run_that_fails_before_any_model_mentions_the_matched_models_people():
    errors = Recorder()
    notif = Notifications(on_failure=errors, mention=Mention.ON_FAILURE)
    runs = [_run(_model("orders", People(owners=(ERIK,))))]
    n.send_failure(notif, ConnectionError("no db"), runs)
    assert errors.mentions == [(ERIK,)]


def test_mention_flags_pick_the_roles():
    both = Recorder()
    notif = Notifications(
        on_any=both,
        mention=Mention.ALWAYS,
        mention_owners=False,
        mention_creators=True,
    )
    people = People(owners=(ERIK,), creators=(BOT,), maintainers=(ANNA,))
    n.send_success(notif, [_run(_model("orders", people), 1.0)])
    assert both.mentions == [(ANNA, BOT)]


def test_creators_are_not_mentioned_by_default():
    both = Recorder()
    notif = Notifications(on_any=both, mention=Mention.ALWAYS)
    model = _model("orders", People(owners=(ERIK,), creators=(BOT,)))
    n.send_success(notif, [_run(model, 1.0)])
    assert both.mentions == [(ERIK,)]


def test_a_mention_that_would_address_nobody_is_rejected():
    with pytest.raises(NotificationsError, match="nobody"):
        Notifications(
            on_any=Recorder(),
            mention=Mention.ON_FAILURE,
            mention_owners=False,
            mention_maintainers=False,
        )


# ── duration formatting ──


@pytest.mark.parametrize(
    "seconds, text",
    [(0, "0m 0s"), (59.9, "0m 59s"), (754, "12m 34s"), (3725, "1h 2m 5s")],
)
def test_duration(seconds, text):
    assert duration_string(seconds) == text
