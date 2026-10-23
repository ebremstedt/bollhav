# Notifications

Where a run's outcome gets reported. When `main()` returns or raises, `@load_models` posts one message: a success card or a failure card, each to its own channel. The models say nothing about it, `main()` says nothing about it, and a notification can never fail a run.

## The default

A run that says nothing reports failures, and only failures, to Microsoft Teams:

```python
Notifications(on_failure=MicrosoftTeams(webhook_env="TEAMS_WEBHOOK_ERRORS"))
```

When `main()` raises, a card goes to the webhook in `TEAMS_WEBHOOK_ERRORS` naming the models that raised and what they raised. Unset, the variable means nothing is sent, with one log line saying so. A clean run sends nothing. So a pod with `TEAMS_WEBHOOK_ERRORS` in its environment hears about failures and nothing else, and a laptop without it stays quiet.

## Your own

Pass a `Notifications` to the decorator for a success channel, other channels, the completed models on a failure card, or mentions; pass `None` to never report:

```python
from bollhav.channels import MicrosoftTeams
from bollhav.model import Detail, Mention, Notifications, load_models


@load_models(
    folder="src/models",
    notifications=Notifications(
        on_failure=MicrosoftTeams(webhook_env="TEAMS_WEBHOOK_FINANCE"),
        detail=Detail.MODELS,
        mention=Mention.ON_FAILURE,
    ),
)
def main(runs, debug):
    ...
```

| Field | Type | Default | Purpose |
|---|---|---|---|
| `on_any` | channel | `None` | Where to report either outcome |
| `on_success` | channel | `None` | Where to report a clean run; overrides `on_any` for that outcome |
| `on_failure` | channel | `None` | Where to report a raise; overrides `on_any` for that outcome |
| `detail` | `Detail` | `Detail.MINIMAL` | `MINIMAL`: the outcome, the model count and the total time. `MODELS`: every model with its run time, and on a failure the models that completed before it |
| `mention` | `Mention` | `Mention.NOBODY` | When to address the [`people`](README.md) of the models that ran: `ON_FAILURE` pings them when the run raises, `ALWAYS` on every message. Each person once per message |
| `mention_owners` | `bool` | `True` | Include `people.owners` in the mention |
| `mention_maintainers` | `bool` | `True` | Include `people.maintainers` |
| `mention_creators` | `bool` | `False` | Include `people.creators` |
| `enabled` | `bool` | `True` | The switch. [`NOTIFICATIONS_ENABLED`](../concepts/env.md#notifications_enabled) in the environment overrides it either way |
| `suffixed_runs` | `bool` | `False` | Whether a run with a [schema or table suffix](../concepts/env.md#schema_suffix), a dev run, is reported. Off, so a developer's test run never posts to the team channel |

A `mention` other than `NOBODY` with all three `mention_*` flags off raises `NotificationsError` at construction, since it would address nobody.

So one channel for everything is `Notifications(on_any=...)`, two separate ones are `Notifications(on_success=..., on_failure=...)`, and `Notifications(on_any=..., on_failure=...)` sends everything to one place with failures diverted.

## The Microsoft Teams channel

`bollhav.channels.MicrosoftTeams(webhook_env="...")` posts an Adaptive Card to a Teams incoming webhook. It takes the **name of an env var**, never the URL: the URL is a secret and belongs in the pod's environment, and an unset env var means nothing is sent, so local runs stay quiet without any configuration.

- **Every send logs one line**, so the run's log says what happened: `Teams notification sent via TEAMS_WEBHOOK_ERRORS`, `TEAMS_WEBHOOK_ERRORS not set, skipping Teams notification`, or `Teams notification via TEAMS_WEBHOOK_ERRORS failed to send: <error>`. A failed send is logged, never raised.
- **Oversized messages are capped.** Teams rejects cards above about 28 KB, so the whole payload is kept under 26 KB. The text is cut in the middle, where nothing of interest sits, and a note in the gap says how many bytes went. A failure card keeps its header and model line at the top and the last lines of the error at the bottom.
- **Mentions** become real @mentions for contacts with an email, which is the UPN Teams resolves. A contact without one is named in plain text

### Getting a webhook URL

In the Teams channel: the channel menu, **Workflows**, then **Post to a channel when a webhook request is received**. The flow gives a URL; put it in a secret and inject it as the env var the channel names. The flow answers `202 Accepted` as soon as it receives the request, so a card that never shows up while the log says `sent` means the flow itself failed afterwards. Its run history in Power Automate says why.

### Behind a corporate CA

The channel posts with Python's standard library, which trusts the system certificate store and reads `SSL_CERT_FILE`, not `REQUESTS_CA_BUNDLE`. A pod that only sets the latter gets `CERTIFICATE_VERIFY_FAILED` in the log. Set `SSL_CERT_FILE` to the same bundle.

### Your own channel

A channel is any object with `send(text: str, mentions: tuple[Contact, ...] = ())`. The text uses the Markdown subset Teams renders: bold, backticks and list lines. A channel that cannot mention may ignore `mentions`.

```python
import logging
from dataclasses import dataclass


@dataclass(frozen=True)
class Log:
    """A channel that only logs: handy in tests, or for a pipeline that should
    record outcomes without posting anywhere."""

    name: str = "notifications"

    def send(self, text, mentions=()):
        logging.getLogger(self.name).info(text)
```

## What gets sent, and when

`@model_lifecycle` records each model's outcome and run time. When `main()` returns or raises, `@load_models` sends **one message for the run**. Every message starts with the host and the run's `TAGS`. The host is the pod name on Kubernetes and `TAGS` the run's tag expression, so a message says which job it came from without any configuration.

### Success

At `Detail.MINIMAL`:

```
Pipeline success: **daily-orders-29876070-b2z6r**

TAGS: `[orders]`

2 model(s) done in 5m 0s
```

At `Detail.MODELS` the models are listed with their run times:

```
Pipeline success: **daily-orders-29876070-b2z6r**

TAGS: `[orders]`

- warehouse.orders: 4m 12s
- warehouse.order_lines: 0m 48s

2 model(s) done in 5m 0s
```

A run in which no model completed, every one skipped by a curfew say, sends no success card.

### Failure

When `main()` raises, the failure card names each model that raised with its exception. If the run then died of something else, because a model's error was caught and the loop went on until a connection dropped, say, that exception is named under the model lines. At `Detail.MODELS` the models that completed before it are listed too:

```
Pipeline failure: **daily-orders-29876070-b2z6r**

TAGS: `[orders]`

- warehouse.order_lines after 0m 48s: ProgrammingError: Invalid object name 'staging.order_lines'

Done before the failure:
- warehouse.orders: 4m 12s
```

A run that fails before reaching any model, a connection that never opened for instance, reports the exception alone:

```
Pipeline failure: **daily-orders-29876070-b2z6r**

TAGS: `[orders]`

OperationalError: could not connect to server: Connection refused
```

A failed run sends no success message, even for the models that completed before the failure. The exception is re-raised once the message is out, so the job still fails exactly as before. A failure in a model's own setup, its table DDL or state bootstrap, counts as that model's failure and is reported under its name.

Sending itself can never fail a run: a channel that raises while building or posting a message is logged with its traceback, the run's own outcome stands, and on a failure the original exception is what gets re-raised.

### Mentions

With `mention=Mention.ON_FAILURE` the failure card @mentions the people of the models that raised, or of every matched model when the run failed before reaching one. `Mention.ALWAYS` also mentions the people of every completed model on the success card. The `mention_*` flags pick the roles, and each person is mentioned once per message. People are declared per model, with [`people`](README.md), since who owns a table is the table's business even though where the run reports is not.

## Keeping it quiet

A run sends one message per outcome, however many models it has, and the default sends none for a clean run. A pipeline that wants green cards passes an `on_success` channel, best a different one from `on_failure`, so the alerts channel stays alerts.

A run with `SCHEMA_SUFFIX` or `TABLE_SUFFIX` in effect is a dev run and sends nothing, whatever the channels say, unless `main` passes `suffixed_runs=True`.

To silence a run without touching code, set `NOTIFICATIONS_ENABLED=false` in its environment: a dev container, a backfill, a test. `NOTIFICATIONS_ENABLED=true` does the opposite: it turns on a run that passed `enabled=False` and lifts the suffix rule, so a suffixed test run can post on purpose. Unset, the run's own settings apply.

## When nothing arrives

1. Search the run's log for `Teams`. `not set` means the env var is missing in that environment. `failed to send` names the error, and a certificate error wants `SSL_CERT_FILE`. No line at all means `main` passed `notifications=None`, or the run was suffixed and did not opt in.
2. `sent` in the log but no card means the Workflows flow failed after accepting the request. Check its run history.
