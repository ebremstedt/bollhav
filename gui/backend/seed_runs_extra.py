"""Give each TEMPORAL model a realistic run history so the grid has many cells
and the lineage status-lights make sense.

Each model OWNS its history here (its state table is cleared, then refilled with
`days` daily intervals), so we control exactly what the lineage lights show. Per
a per-model profile:

  * healthy  — every interval applied (no lights)
  * running  — history applied, the LATEST interval still running
  * blocked  — history applied, the LATEST interval blocked
  * flaky    — a few scattered historical intervals failed (error)

Transient states (running / blocked) only ever sit on the most-recent interval —
never stuck on an old one — so a model never lights up every status at once.

    python gui/backend/seed_runs_extra.py            # 60 days into z_bollhav
    python gui/backend/seed_runs_extra.py 90 z_bollhav_demoenv
"""

import random
import sys
import uuid
from datetime import datetime, timedelta, timezone

import psycopg
from psycopg import sql

from bollhav.postgres.state import LIBRARY_SCHEMA, LIBRARY_TABLE

from seed import DSN

NOW = datetime(2026, 6, 17, 0, 0, 0, tzinfo=timezone.utc)

# cycled across the temporal models (ordered by name) — most healthy, a few with
# one issue each, so every light is demoed but none stack nonsensically.
PROFILES = ["healthy", "running", "flaky", "blocked", "stale", "flaky"]

# completeness block (orange) vs freshness block (blue) — the latter must carry
# the word "stale" so `_blocked_kinds` classifies it as the stale badge.
BLOCK_REASON = "upstream orders has no applied row for this window"
STALE_REASON = "upstream present but stale — older than the 1d freshness window"


def _temporal_models(conn, schema):
    return conn.execute(
        sql.SQL(
            "SELECT full_name, state_schema, state_table FROM {}.{} "
            "WHERE state_table IS NOT NULL AND temporality = 'temporal' "
            "ORDER BY full_name"
        ).format(sql.Identifier(schema), sql.Identifier(LIBRARY_TABLE))
    ).fetchall()


def main():
    days = int(sys.argv[1]) if len(sys.argv) > 1 else 60
    schema = sys.argv[2] if len(sys.argv) > 2 else LIBRARY_SCHEMA
    rng = random.Random(7)

    with psycopg.connect(DSN) as conn:
        models = _temporal_models(conn, schema)
        if not models:
            print(f"no temporal models in {schema}; run seed.py first")
            return
        total = 0
        for idx, (full_name, st_schema, st_table) in enumerate(models):
            profile = PROFILES[idx % len(PROFILES)]
            insert = sql.SQL(
                "INSERT INTO {schema}.{table} "
                "(model_name, run_id, since, until, status, blocked_reason, "
                "applied_at, temporality) VALUES (%s, %s, %s, %s, %s, %s, %s, 'temporal')"
            ).format(schema=sql.Identifier(st_schema), table=sql.Identifier(st_table))
            with conn.transaction():
                # own the history for a coherent picture
                conn.execute(
                    sql.SQL("DELETE FROM {}.{}").format(
                        sql.Identifier(st_schema), sql.Identifier(st_table)
                    )
                )
                for d in range(days):
                    until = NOW - timedelta(days=d)
                    since = until - timedelta(days=1)
                    blocked_reason = None
                    if d == 0 and profile == "running":
                        status = "running"
                    elif d == 0 and profile in ("blocked", "stale"):
                        status = "blocked"
                        blocked_reason = (
                            STALE_REASON if profile == "stale" else BLOCK_REASON
                        )
                    elif profile == "flaky" and rng.random() < 0.08:
                        status = "error"
                    else:
                        status = "applied"
                    applied_at = (
                        until + timedelta(hours=2, minutes=rng.randint(0, 50))
                        if status in ("applied", "error")
                        else None
                    )
                    conn.execute(
                        insert,
                        [
                            full_name,
                            str(uuid.uuid4()),
                            since,
                            until,
                            status,
                            blocked_reason,
                            applied_at,
                        ],
                    )
                    total += 1
        conn.commit()
    print(f"seeded {total} run rows across {len(models)} temporal models in {schema}")


if __name__ == "__main__":
    main()
