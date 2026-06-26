---
title: "State modes"
body: "How a re-run treats existing state."
---

`discover` (default) preserves applied rows and runs only what's pending. `bulldozer` resets existing rows to pending so the window re-runs. `torch` DELETEs every row then prefills fresh — for changing chunk grain or wiping a backlog.
