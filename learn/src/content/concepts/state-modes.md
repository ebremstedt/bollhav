---
title: "State modes 🔁"
body: "What a re-run does with state that's already there"
---

When you run a model again, the state mode decides what happens to intervals that already have rows. `discover` (the default) leaves applied intervals alone and runs only the ones still pending. `bulldozer` resets existing intervals back to pending, forcing the whole window to run again. `torch` clears every row and refills the state from scratch — what you reach for when changing the chunk size or wiping out a backlog.
