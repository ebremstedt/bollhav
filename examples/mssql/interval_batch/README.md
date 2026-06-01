# MSSQL — interval windows + read-time batch sizing

Demonstrates the two chunking knobs working together against SQL Server.
Everything runs in Docker — **no host ODBC driver or Python setup.**

- **`batching.interval=@daily`** → the `(since, until)` windows. Bounds
  span 2024-01-01 .. 2024-01-04, so there are **3 intervals** (one per
  day). The runner loops them and hands each pair to `execute`.
- **`batching.size=5000`** → rows per read chunk. Each day generates
  12 000 rows, so the read function streams each day as **5000 + 5000 +
  2000** — three write chunks per interval.

The interval is the *recovery unit*; `size` is the *streaming unit*.
They're independent — see [CHUNKING.md](../../../docs/content/CHUNKING.md).

## Run

```bash
docker compose up --build
```

That starts SQL Server, waits for it to be healthy, then runs the
example in the `app` container. Watch the per-day output — three
`read chunk` lines of 5000/5000/2000 are `size` being honored by the
read function ([read.py](read.py)) while the framework loops the daily
intervals:

```
warehouse.events  3 interval(s), size=5000
  2024-01-01 → 2024-01-02
      read chunk:  5000 rows  (size=5000)
      read chunk:  5000 rows  (size=5000)
      read chunk:  2000 rows  (size=5000)
  2024-01-02 → 2024-01-03
      ...
✓ warehouse.events now holds 36000 rows
```

> First run pulls the SQL Server image and (on Apple Silicon) boots it
> under amd64 emulation — give it a minute. The `app` container blocks
> on the DB healthcheck, so it won't connect early.

## What the pieces do

| File | Role |
|------|------|
| [src/models/events.py](src/models/events.py) | the model — `Batch(interval=@daily, size=5000)`, APPEND, MSSQL target |
| [read.py](read.py) | read function: filters to the interval, yields `model.batching.size`-row frames |
| [execute.py](execute.py) | per-interval: connect + `bollhav.mssql.write` |
| [main.py](main.py) | ensure DB, reset target, loop `model.intervals` |
| [Dockerfile](Dockerfile) | Python + ODBC Driver 18 + bollhav + pyodbc |

> **Note** — APPEND, no staging, no `State()`. MSSQL state coordination
> isn't implemented yet, so the run isn't gated on applied rows; `main.py`
> drops the target first so reruns stay clean.

## Verify

```bash
docker exec -it bollhav-mssql /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P 'Bollhav_Passw0rd1' -No -d bollhav \
  -Q "SELECT COUNT(*) AS rows FROM warehouse.events"
```

Expect `36000` rows (3 days × 12 000).

## Teardown

```bash
docker compose down -v
```

---

### Running it on the host instead (optional)

If you'd rather run `python main.py` directly against the container DB,
you need Microsoft's ODBC Driver 18 on your Mac:

```bash
brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
HOMEBREW_ACCEPT_EULA=Y brew install msodbcsql18 unixodbc
pip install -e ../../.. && pip install -r requirements.txt
export BOLLHAV_MSSQL_DSN='DRIVER={ODBC Driver 18 for SQL Server};SERVER=127.0.0.1,1433;DATABASE=bollhav;UID=sa;PWD=Bollhav_Passw0rd1;TrustServerCertificate=yes;Encrypt=no'
docker compose up -d mssql      # DB only
python main.py
```
