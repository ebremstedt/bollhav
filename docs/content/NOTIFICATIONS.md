[Home](index.md) › [Model](MODEL.md) › **Notifications**

# Notifications

Where a run's outcome gets reported. Set `notifications=Notifications(...)` on a model and the framework sends a message when the run that included it finishes: one for success, one for failure, each to its own channel. Nothing in your `main()` changes.

Models default to **no notifications** (`notifications=None`).

## Fields

| Field | Type | Default | Purpose |
|---|---|---|---|
| `on_any` | channel | `None` | Where to report either outcome |
| `on_success` | channel | `None` | Where to report a clean run; overrides `on_any` for that outcome |
| `on_failure` | channel | `None` | Where to report a raise; overrides `on_any` for that outcome |

So one channel for everything is `Notifications(on_any=...)`, two separate ones are `Notifications(on_success=..., on_failure=...)`, and `Notifications(on_any=..., on_failure=...)` sends everything to one place with failures diverted.

A channel is any object with `send(text: str)`. One ships:

| Channel | Fields | Behaviour |
|---|---|---|
| `Teams(webhook_env)` | the **name of an env var** holding a Teams incoming-webhook URL | Posts an Adaptive Card. Env var unset → nothing is sent, so local runs stay quiet. A failed send is logged, never raised. |

```python
from bollhav.model import Model, Notifications, Teams

# one channel for both outcomes
Model(
    target=...,
    notifications=Notifications(on_any=Teams(webhook_env="TEAMS_WEBHOOK")),
)

# successes and failures apart
Model(
    target=...,
    notifications=Notifications(
        on_success=Teams(webhook_env="TEAMS_WEBHOOK_SUCCESS"),
        on_failure=Teams(webhook_env="TEAMS_WEBHOOK_ERRORS"),
    ),
)
```

## What gets sent, and when

`@model_lifecycle` records each model's outcome and run time. When `main()` returns, `@load_models` sends **one message per channel**, so models that share a channel appear together:

```
Pipeline success: **daily-orders-29876070-b2z6r**

TAGS: `[orders]`

- warehouse.orders: 4m 12s
- warehouse.order_lines: 0m 48s

2 model(s) done in 5m 0s
```

When `main()` raises, the models that raised report on their `on_failure` channel with the exception. A run that fails before reaching any model, a connection that never opened for instance, reports to every matched model's failure channel instead. A failed run sends no success message, even for the models that completed before the failure.

The host name is the pod name on Kubernetes, and `TAGS` is the run's tag expression, so a message says which job it came from without any configuration.

## Keeping it quiet

Notifications are per model, so a job with thirty models and one channel still sends one message. Leave `notifications` off the models nobody needs to hear about, or point an `on_success` and an `on_failure` at different channels to keep an alerts channel free of green cards.
