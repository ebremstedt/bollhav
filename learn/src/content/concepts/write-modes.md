---
title: "Write modes"
body: "How rows land in the target."
---

`UPSERT_NO_DELETE` MERGEs on the key and never deletes. Staging writes to a side table first for an atomic incremental load. Order-independent writes are what make flexible intervals safe.
