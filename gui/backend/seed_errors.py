"""Seed a pile of historic errors into the errors table so the GUI's **Errors**
tab has something to page through (50 / 100 / 200 / 1000).

Additive — it only INSERTs into the errors table, never drops anything. Targets
prod (`z_bollhav`) by default; pass a schema to target a suffixed env:

    python gui/backend/seed_errors.py            # -> z_bollhav, ~300 errors
    python gui/backend/seed_errors.py 500         # -> z_bollhav, ~500 errors
    python gui/backend/seed_errors.py 300 z_bollhav_demoenv
"""

import random
import sys
import uuid
from datetime import datetime, timedelta, timezone

import psycopg
from psycopg import sql

from bollhav.postgres.state import ERRORS_TABLE, LIBRARY_SCHEMA, LIBRARY_TABLE

from seed import DSN

NOW = datetime(2026, 6, 17, 12, 0, 0, tzinfo=timezone.utc)

# (error_type, message template) — a realistic spread of pipeline failures.
TEMPLATES = [
    ("ConnectionResetError", "source connection dropped mid-read"),
    ("OperationalError", "deadlock detected; interval retried"),
    ("ValueError", "null {col} key after join"),
    ("AssertionError", "row count {n} below sanity threshold"),
    ("TimeoutError", "upstream read exceeded {n}s timeout"),
    ("IntegrityError", "duplicate key on ({col}) — staging merge aborted"),
    ("KeyError", "expected column {col!r} missing from source frame"),
    ("MemoryError", "frame of {n} rows exceeded the worker budget"),
    ("PermissionError", "role lacks INSERT on the target table"),
    ("DataError", "value out of range for {col} (numeric overflow)"),
    ("OperationalError", "could not serialize access due to concurrent update"),
    ("RuntimeError", "contract check failed: upstream {col} not yet applied"),
]
COLS = ["customer_id", "order_date", "total", "channel", "fulfilment_key", "region_id"]


def _traceback(etype: str, msg: str) -> str:
    return (
        "Traceback (most recent call last):\n"
        '  File "/app/run_interval.py", line 42, in execute\n'
        "    write(conn=data_conn, run=run, df_gen=transform(read(run, interval)))\n"
        '  File "/app/transform.py", line 17, in transform\n'
        "    raise %s(%r)\n"
        "%s: %s"
    ) % (etype, msg, etype, msg)


def _model_names(conn, schema: str) -> list[str]:
    rows = conn.execute(
        sql.SQL("SELECT full_name FROM {}.{}").format(
            sql.Identifier(schema), sql.Identifier(LIBRARY_TABLE)
        )
    ).fetchall()
    return [r[0] for r in rows]


def main() -> None:
    count = int(sys.argv[1]) if len(sys.argv) > 1 else 300
    schema = sys.argv[2] if len(sys.argv) > 2 else LIBRARY_SCHEMA
    rng = random.Random(1337)  # deterministic spread

    insert = sql.SQL(
        "INSERT INTO {schema}.{table} "
        "(full_name, run_id, since, until, error_type, error_message, "
        "traceback, created_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    ).format(schema=sql.Identifier(schema), table=sql.Identifier(ERRORS_TABLE))

    with psycopg.connect(DSN) as conn:
        names = _model_names(conn, schema)
        if not names:
            print(f"no models registered in {schema}; run seed.py first")
            return
        with conn.transaction():
            for i in range(count):
                etype, tmpl = rng.choice(TEMPLATES)
                msg = tmpl.format(col=rng.choice(COLS), n=rng.randint(2, 90_000))
                # spread over ~90 days, newest first; a few per "day"
                created = NOW - timedelta(
                    minutes=i * rng.randint(180, 520) + rng.randint(0, 120)
                )
                # ~⅔ have a daily window, ⅓ are whole-table (NULL window)
                if rng.random() < 0.66:
                    day = created.replace(hour=0, minute=0, second=0, microsecond=0)
                    since, until = day - timedelta(days=1), day
                else:
                    since = until = None
                conn.execute(
                    insert,
                    [
                        rng.choice(names),
                        str(uuid.uuid4()),
                        since,
                        until,
                        etype,
                        msg,
                        _traceback(etype, msg),
                        created,
                    ],
                )
        conn.commit()
    print(f"seeded {count} errors into {schema}.{ERRORS_TABLE}")


if __name__ == "__main__":
    main()
