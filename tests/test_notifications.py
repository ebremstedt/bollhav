"""Notifications: the outcome each run carries and the one-message-per-channel
grouping that load_models sends from. The Teams channel has its own tests."""

import logging

import pytest

from bollhav.model import notifications as n
from bollhav.model.model import Model
from bollhav.model.modelrun import ModelRun, Outcome, duration_string
from bollhav.model.notifications import (
    Detail,
    Mention,
    Notifications,
    NotificationsError,
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


def _model(
    name: str,
    notifications: Notifications | None,
    people: People | None = None,
    suffixed: bool = False,
) -> Model:
    """`suffixed` is what the runtime stamps on its copy for a SCHEMA_SUFFIX /
    TABLE_SUFFIX run; a Model is frozen, so it is set at construction."""
    return Model(
        target=Target(name=name, schema="warehouse"),
        temporality=Temporality.TIMELESS,
        notifications=notifications,
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


def test_on_any_hears_a_success_and_a_failure():
    both = Recorder()
    model = _model("orders", Notifications(on_any=both))
    n.send_success([_run(model, 3.0)])
    n.send_failure(RuntimeError("x"), [_run(model, 3.0, RuntimeError("x"))])
    assert [t.split(":")[0] for t in both.sent] == [
        "Pipeline success",
        "Pipeline failure",
    ]


def test_a_run_without_an_outcome_never_counts_as_done():
    ok = Recorder()
    model = _model("orders", Notifications(on_success=ok))
    n.send_success([_run(model)])
    assert ok.sent == []


# ── enabled ──


def test_disabled_notifications_send_nothing():
    both = Recorder()
    model = _model("orders", Notifications(on_any=both, enabled=False))
    n.send_success([_run(model, 1.0)])
    n.send_failure(RuntimeError("x"), [_run(model, 1.0, RuntimeError("x"))])
    n.send_failure(ConnectionError("no db"), [_run(model)])
    assert both.sent == []


def test_env_var_false_silences_every_model(monkeypatch):
    monkeypatch.setenv("NOTIFICATIONS_ENABLED", "False")
    ok = Recorder()
    n.send_success([_run(_model("orders", Notifications(on_success=ok)), 1.0)])
    assert ok.sent == []


def test_env_var_true_forces_a_disabled_model_on(monkeypatch):
    monkeypatch.setenv("NOTIFICATIONS_ENABLED", "true")
    ok = Recorder()
    model = _model("orders", Notifications(on_success=ok, enabled=False))
    n.send_success([_run(model, 1.0)])
    assert len(ok.sent) == 1


def test_env_var_garbage_is_ignored_with_a_warning(monkeypatch, caplog):
    monkeypatch.setenv("NOTIFICATIONS_ENABLED", "yes")
    ok = Recorder()
    with caplog.at_level(logging.WARNING):
        n.send_success([_run(_model("orders", Notifications(on_success=ok)), 1.0)])
    assert len(ok.sent) == 1
    assert "NOTIFICATIONS_ENABLED must be true or false" in caplog.text


# ── suffixed runs ──


def test_a_suffixed_run_is_quiet_by_default():
    both = Recorder()
    model = _model("orders", Notifications(on_any=both), suffixed=True)
    n.send_success([_run(model, 1.0)])
    n.send_failure(RuntimeError("x"), [_run(model, 1.0, RuntimeError("x"))])
    n.send_failure(ConnectionError("no db"), [_run(model)])
    assert both.sent == []


def test_env_var_true_lifts_the_suffix_rule_for_the_run(monkeypatch):
    monkeypatch.setenv("NOTIFICATIONS_ENABLED", "true")
    ok = Recorder()
    model = _model("orders", Notifications(on_success=ok), suffixed=True)
    n.send_success([_run(model, 1.0)])
    assert len(ok.sent) == 1


def test_suffixed_runs_opt_in_and_the_card_reads_as_usual(monkeypatch):
    monkeypatch.setenv("HOSTNAME", "dev-1")
    both = Recorder()
    model = _model(
        "orders", Notifications(on_any=both, suffixed_runs=True), suffixed=True
    )
    n.send_success([_run(model, 1.0)], tags="[orders]")
    (text,) = both.sent
    assert text == (
        "Pipeline success: **dev-1**\n\nTAGS: `[orders]`\n\n1 model(s) done in 0m 1s"
    )


def test_suffixed_opt_in_covers_failures_too():
    errors = Recorder()
    model = _model(
        "orders", Notifications(on_failure=errors, suffixed_runs=True), suffixed=True
    )
    n.send_failure(RuntimeError("x"), [_run(model, 2.0, RuntimeError("x"))])
    n.send_failure(ConnectionError("no db"), [_run(model)])
    assert len(errors.sent) == 2
    assert "RuntimeError: x" in errors.sent[0]
    assert "ConnectionError: no db" in errors.sent[1]


def test_env_var_false_beats_the_suffixed_opt_in(monkeypatch):
    monkeypatch.setenv("NOTIFICATIONS_ENABLED", "false")
    both = Recorder()
    notif = Notifications(on_any=both, suffixed_runs=True)
    n.send_success([_run(_model("orders", notif, suffixed=True), 1.0)])
    n.send_success([_run(_model("orders", notif), 1.0)])
    assert both.sent == []


def test_an_empty_env_var_counts_as_unset(monkeypatch):
    monkeypatch.setenv("NOTIFICATIONS_ENABLED", "")
    assert Notifications(enabled=False).is_enabled() is False
    assert Notifications().is_enabled() is True


def test_a_shared_channel_lists_only_the_models_that_may_report():
    ok = Recorder()
    on = Notifications(on_success=ok, detail=Detail.MODELS)
    off = Notifications(on_success=ok, detail=Detail.MODELS, enabled=False)
    n.send_success(
        [
            _run(_model("orders", on), 1.0),
            _run(_model("customers", off), 1.0),
            _run(_model("audit", None), 1.0),
        ]
    )
    (text,) = ok.sent
    assert "warehouse.orders" in text
    assert "customers" not in text and "audit" not in text
    assert "1 model(s) done" in text


# ── success ──


def test_minimal_success_has_tags_and_the_summary_but_no_model_lines(monkeypatch):
    monkeypatch.setenv("HOSTNAME", "daily-x-123")
    ok = Recorder()
    n.send_success(
        [
            _run(_model("orders", Notifications(on_success=ok)), 65.0),
            _run(_model("customers", Notifications(on_success=ok)), 10.0),
        ],
        tags="[all]",
    )
    (text,) = ok.sent
    assert text == (
        "Pipeline success: **daily-x-123**\n\nTAGS: `[all]`\n\n2 model(s) done in 1m 15s"
    )


def test_a_channel_is_detailed_when_any_of_its_models_asks_for_it():
    ok = Recorder()
    n.send_success(
        [
            _run(_model("orders", Notifications(on_success=ok)), 1.0),
            _run(
                _model("customers", Notifications(on_success=ok, detail=Detail.MODELS)),
                2.0,
            ),
        ]
    )
    assert "- warehouse.orders: 0m 1s" in ok.sent[0]


def test_success_sends_one_message_per_channel_listing_its_models(monkeypatch):
    monkeypatch.setenv("HOSTNAME", "daily-x-123")
    a, b = Recorder(), Recorder()
    detailed = lambda ch: Notifications(on_success=ch, detail=Detail.MODELS)  # noqa: E731
    n.send_success(
        [
            _run(_model("orders", detailed(a)), 65.0),
            _run(_model("customers", detailed(a)), 10.0),
            _run(_model("audit", detailed(b)), 1.0),
            _run(_model("silent", None), 1.0),
        ],
        tags="[all]",
    )
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
    model = _model("orders", Notifications(on_success=ok))
    n.send_success([_run(model, 1.0, RuntimeError("x"))])
    assert ok.sent == []


# ── failure ──


def test_failure_reports_the_raising_model_on_its_own_channel():
    errors = Recorder()
    model = _model("orders", Notifications(on_failure=errors))
    n.send_failure(
        ValueError("bad rows"),
        [_run(model, 12.0, ValueError("bad rows"))],
        tags="[orders]",
    )
    (text,) = errors.sent
    assert text.startswith("Pipeline failure: **")
    assert "- warehouse.orders after 0m 12s: ValueError: bad rows" in text


def test_detailed_failure_also_lists_what_completed_before_it():
    errors = Recorder()
    done = _model("orders", Notifications(on_failure=errors, detail=Detail.MODELS))
    bad = _model("customers", Notifications(on_failure=errors, detail=Detail.MODELS))
    n.send_failure(
        RuntimeError("x"), [_run(done, 5.0), _run(bad, 2.0, RuntimeError("x"))]
    )
    (text,) = errors.sent
    assert "- warehouse.customers after 0m 2s: RuntimeError: x" in text
    assert "Done before the failure:\n- warehouse.orders: 0m 5s" in text


def test_minimal_failure_does_not_list_the_completed_models():
    errors = Recorder()
    done = _model("orders", Notifications(on_failure=errors))
    bad = _model("customers", Notifications(on_failure=errors))
    n.send_failure(
        RuntimeError("x"), [_run(done, 5.0), _run(bad, 2.0, RuntimeError("x"))]
    )
    assert "Done before" not in errors.sent[0]


def test_failure_outside_any_model_reaches_every_failure_channel_once():
    errors = Recorder()
    runs = [
        _run(_model("orders", Notifications(on_failure=errors))),
        _run(_model("customers", Notifications(on_failure=errors))),
        _run(_model("silent", None)),
    ]
    n.send_failure(ConnectionError("no state db"), runs)
    (text,) = errors.sent
    assert "ConnectionError: no state db" in text


def test_failure_sends_nothing_without_failure_channels():
    ok = Recorder()
    model = _model("orders", Notifications(on_success=ok))
    n.send_failure(RuntimeError("x"), [_run(model, 1.0, RuntimeError("x"))])
    assert ok.sent == []


def test_failure_names_the_runs_own_exception_when_a_model_error_was_caught():
    errors = Recorder()
    model = _model("orders", Notifications(on_failure=errors))
    n.send_failure(
        ConnectionError("db went away"),
        [_run(model, 1.0, RuntimeError("locked, skipped"))],
    )
    (text,) = errors.sent
    assert "- warehouse.orders after 0m 1s: RuntimeError: locked, skipped" in text
    assert "The run then failed with ConnectionError: db went away" in text


def test_failure_does_not_repeat_the_exception_that_is_the_models_own():
    errors = Recorder()
    model = _model("orders", Notifications(on_failure=errors))
    exc = RuntimeError("x")
    n.send_failure(exc, [_run(model, 1.0, exc)])
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
    model = _model("orders", Notifications(on_success=ok), People(owners=(ERIK,)))
    n.send_success([_run(model, 1.0)])
    assert ok.mentions == [()]


def test_on_failure_mentions_owners_and_maintainers_only_when_it_fails():
    both = Recorder()
    people = People(owners=(ERIK,), maintainers=(ANNA,))
    model = _model(
        "orders", Notifications(on_any=both, mention=Mention.ON_FAILURE), people
    )
    n.send_success([_run(model, 1.0)])
    assert both.mentions == [()]
    n.send_failure(RuntimeError("x"), [_run(model, 1.0, RuntimeError("x"))])
    assert both.mentions[-1] == (ERIK, ANNA)


def test_always_mentions_on_success_and_each_person_once():
    ok = Recorder()
    notif = Notifications(on_success=ok, mention=Mention.ALWAYS)
    n.send_success(
        [
            _run(_model("orders", notif, People(owners=(ERIK,))), 1.0),
            _run(_model("customers", notif, People(maintainers=(ERIK, ANNA))), 1.0),
            _run(
                _model(
                    "audit", Notifications(on_success=ok), People(owners=(NO_EMAIL,))
                ),
                1.0,
            ),
        ]
    )
    assert ok.mentions == [(ERIK, ANNA)]


def test_a_run_that_fails_before_any_model_mentions_the_matched_models_people():
    errors = Recorder()
    notif = Notifications(on_failure=errors, mention=Mention.ON_FAILURE)
    runs = [_run(_model("orders", notif, People(owners=(ERIK,))))]
    n.send_failure(ConnectionError("no db"), runs)
    assert errors.mentions == [(ERIK,)]


def test_mention_flags_pick_the_roles():
    both = Recorder()
    people = People(owners=(ERIK,), creators=(BOT,), maintainers=(ANNA,))
    notif = Notifications(
        on_any=both,
        mention=Mention.ALWAYS,
        mention_owners=False,
        mention_creators=True,
    )
    n.send_success([_run(_model("orders", notif, people), 1.0)])
    assert both.mentions == [(ANNA, BOT)]


def test_creators_are_not_mentioned_by_default():
    both = Recorder()
    people = People(owners=(ERIK,), creators=(BOT,))
    model = _model("orders", Notifications(on_any=both, mention=Mention.ALWAYS), people)
    n.send_success([_run(model, 1.0)])
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
