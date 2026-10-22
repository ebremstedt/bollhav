[Home](index.md) › [Model](MODEL.md) › **Notifications**

# Notifications

Where a run's outcome gets reported. Set `notifications=Notifications(...)` on a model and the framework posts a message when the run that included it finishes: one for success, one for failure, each to its own channel. Nothing in your `main()` changes, and a notification can never fail a run.

Models default to **no notifications** (`notifications=None`).

## Fields

| Field | Type | Default | Purpose |
|---|---|---|---|
| `on_any` | channel | `None` | Where to report either outcome |
| `on_success` | channel | `None` | Where to report a clean run; overrides `on_any` for that outcome |
| `on_failure` | channel | `None` | Where to report a raise; overrides `on_any` for that outcome |
| `detail` | `Detail` | `Detail.MINIMAL` | `MINIMAL`: the outcome, the model count and the total time. `MODELS`: every model with its run time, and on a failure the models that completed before it |
| `mention` | `Mention` | `Mention.NOBODY` | When to address the model's [`people`](MODEL.md): `ON_FAILURE` pings them when the run raises, `ALWAYS` on every message. Each person once per message |
| `mention_owners` | `bool` | `True` | Include `people.owners` in the mention |
| `mention_maintainers` | `bool` | `True` | Include `people.maintainers` |
| `mention_creators` | `bool` | `False` | Include `people.creators` |
| `enabled` | `bool` | `True` | The switch. [`NOTIFICATIONS_ENABLED`](ENV.md#notifications_enabled) in the environment overrides it either way |
| `suffixed_runs` | `bool` | `False` | Whether a run with a [schema or table suffix](ENV.md#schema_suffix), a dev run, is reported. Off, so a developer's test run never posts to the team channel |

A `mention` other than `NOBODY` with all three `mention_*` flags off raises `NotificationsError` at construction, since it would address nobody.

So one channel for everything is `Notifications(on_any=...)`, two separate ones are `Notifications(on_success=..., on_failure=...)`, and `Notifications(on_any=..., on_failure=...)` sends everything to one place with failures diverted.

```python
from bollhav.channels import MicrosoftTeams
from bollhav.model import Model, Notifications

# one channel for both outcomes
Model(
    target=...,
    notifications=Notifications(on_any=MicrosoftTeams(webhook_env="TEAMS_WEBHOOK")),
)

# successes and failures apart
Model(
    target=...,
    notifications=Notifications(
        on_success=MicrosoftTeams(webhook_env="TEAMS_WEBHOOK_SUCCESS"),
        on_failure=MicrosoftTeams(webhook_env="TEAMS_WEBHOOK_ERRORS"),
    ),
)
```

## The Microsoft Teams channel

`bollhav.channels.MicrosoftTeams(webhook_env="...")` posts an Adaptive Card to a Teams incoming webhook. It takes the **name of an env var**, never the URL: the URL is a secret and belongs in the pod's environment, and an unset env var means nothing is sent, so local runs stay quiet without any configuration.

- **Every send logs one line**, so the run's log says what happened: `Teams notification sent via TEAMS_WEBHOOK`, `TEAMS_WEBHOOK not set, skipping Teams notification`, or `Teams notification via TEAMS_WEBHOOK failed to send: <error>`. A failed send is logged, never raised.
- **Oversized messages are capped.** Teams rejects cards above about 28 KB, so the whole payload is kept under 26 KB. The text is cut in the middle, where nothing of interest sits, and a note in the gap says how many bytes went. A failure card keeps its header and model line at the top and the last lines of the error at the bottom.
- **Mentions** become real @mentions for contacts with an email, which is the UPN Teams resolves. A contact without one is named in plain text

### Getting a webhook URL

In the Teams channel: the channel menu, **Workflows**, then **Post to a channel when a webhook request is received**. The flow gives a URL; put it in a secret and inject it as the env var the model names. The flow answers `202 Accepted` as soon as it receives the request, so a card that never shows up while the log says `sent` means the flow itself failed afterwards. Its run history in Power Automate says why.

### Behind a corporate CA

The channel posts with Python's standard library, which trusts the system certificate store and reads `SSL_CERT_FILE`, not `REQUESTS_CA_BUNDLE`. A pod that only sets the latter gets `CERTIFICATE_VERIFY_FAILED` in the log. Set `SSL_CERT_FILE` to the same bundle.

### Your own channel

A channel is any object with `send(text: str, mentions: tuple[Contact, ...] = ())`. The text uses the Markdown subset Teams renders: bold, backticks and list lines. A channel that cannot mention may ignore `mentions`. Models are grouped by channel equality, so make yours a frozen dataclass, or otherwise equal when configured alike, and a shared channel still gets one message.

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

## Full example

```python
from bollhav.channels import MicrosoftTeams
from bollhav.model import Contact, Detail, Mention, Model, Notifications, People, Target

Model(
    target=Target(name="orders", schema="warehouse", ...),
    people=People(
        owners=(Contact(name="Anna Andersson", email="anna.andersson@example.com"),),
        creators=(Contact(name="Build bot"),),
        maintainers=(Contact(name="Erik Eriksson", email="erik.eriksson@example.com"),),
    ),
    notifications=Notifications(
        on_any=MicrosoftTeams(webhook_env="TEAMS_WEBHOOK"),            # everything goes here...
        on_failure=MicrosoftTeams(webhook_env="TEAMS_WEBHOOK_ERRORS"),  # ...except failures
        detail=Detail.MODELS,                                           # list every model and its time
        mention=Mention.ON_FAILURE,                                     # ping Anna and Erik when it breaks
        mention_creators=False,                                         # (the default) leave the bot out
    ),
)
```

With `TEAMS_WEBHOOK` and `TEAMS_WEBHOOK_ERRORS` set in the pod's environment, a clean run posts one card per channel listing the models. A failure posts to the errors channel, names the model and the exception, lists what had completed, and @mentions the owners and maintainers of the models that asked for it, but not the creator, since `mention_creators` is off.

## What gets sent, and when

`@model_lifecycle` records each model's outcome and run time. When `main()` returns or raises, `@load_models` sends **one message per channel**, so models that share a channel appear together. Every message starts with the host and the run's `TAGS`. The host is the pod name on Kubernetes and `TAGS` the run's tag expression, so a message says which job it came from without any configuration.

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

A channel shared by models with different `detail` uses the more detailed level.

### Failure

When `main()` raises, each model that raised reports on its `on_failure` channel with its exception. If the run then died of something else, because a model's error was caught and the loop went on until a connection dropped, say, that exception is named under the model lines. At `Detail.MODELS` the models that completed before it are listed too:

```
Pipeline failure: **daily-orders-29876070-b2z6r**

TAGS: `[orders]`

- warehouse.order_lines after 0m 48s: ProgrammingError: Invalid object name 'staging.order_lines'

Done before the failure:
- warehouse.orders: 4m 12s
```

A run that fails before reaching any model, a connection that never opened for instance, reports to every matched model's failure channel instead, with only the exception:

```
Pipeline failure: **daily-orders-29876070-b2z6r**

TAGS: `[orders]`

OperationalError: could not connect to server: Connection refused
```

A failed run sends no success message, even for the models that completed before the failure. The exception is re-raised once the message is out, so the job still fails exactly as before. A failure in a model's own setup, its table DDL or state bootstrap, counts as that model's failure and is reported under its name.

Sending itself can never fail a run: a channel that raises while building or posting a message is logged with its traceback, the run's own outcome stands, and on a failure the original exception is what gets re-raised.

## Keeping it quiet

Notifications are per model, so a job with thirty models and one channel still sends one message. Leave `notifications` off the models nobody needs to hear about, or point an `on_success` and an `on_failure` at different channels to keep an alerts channel free of green cards.

A run with `SCHEMA_SUFFIX` or `TABLE_SUFFIX` in effect is a dev run and sends nothing, whatever the channels say, unless the model sets `suffixed_runs=True`.

To silence a whole run without touching the models, set `NOTIFICATIONS_ENABLED=false` in its environment: a dev container, a backfill, a test. `NOTIFICATIONS_ENABLED=true` does the opposite: it turns on models that declare `enabled=False` and lifts the suffix rule, so a suffixed test run can post on purpose. Unset, each model's own settings apply.

## When nothing arrives

1. Search the run's log for `Teams`. `not set` means the env var is missing in that environment. `failed to send` names the error, and a certificate error wants `SSL_CERT_FILE`. No line at all means the models that ran carry no notifications, or the run was suffixed and nothing opted in.
2. `sent` in the log but no card means the Workflows flow failed after accepting the request. Check its run history.
