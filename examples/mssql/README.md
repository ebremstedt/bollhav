# MSSQL examples

Runnable SQL Server examples. Each is self-contained and runs entirely
in Docker — no host ODBC driver or Python setup. Just `docker compose up
--build` from the example's directory (Docker Desktop running is the only
prerequisite).

| Example | Shows |
|---------|-------|
| [interval_batch](interval_batch/) | Interval windows (`batching.interval`) + read-time row batching (`batching.size`) — the two chunking knobs working together, written to MSSQL with `bollhav.mssql.write`. |
