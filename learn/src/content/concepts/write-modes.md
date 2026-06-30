---
title: "Write modes ✍️"
body: "How should data be written to the target?"
---

A write mode decides how a run's rows meet what's already in the target. `UPSERT_NO_DELETE` merges on the key — updating matched rows and adding new ones, but never deleting the rest — so re-running a window only refreshes its own rows. Because no write depends on another landing first, windows can arrive in any order; that's what makes flexible intervals safe.
