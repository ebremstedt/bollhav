"""The made-up source — stands in for the MSSQL `viewreader` reads.

In intelligence-src-entity-raw, `read.py` wraps a pyodbc cursor and yields
polars frames of `fetch_size` rows. This one invents the rows, but keeps the
contract the rest of the pipeline relies on:

  * whatever columns the source view happens to have, plus
  * `TimestampRead` — the column the model's upstream names as
    `partitioned_by`, which the transform turns into `_data_modified`.

Rows are deterministic per (hospital, view, interval), so rerunning a window
reproduces the same payloads. `TimestampRead` is handed back as a NAIVE
Stockholm wall-clock time, the way the real view does, so `transform._to_utc`
does the same localisation work it does in production.
"""

from __future__ import annotations

import random
from collections.abc import Generator
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import polars as pl

STOCKHOLM = ZoneInfo("Europe/Stockholm")
ROWS_PER_INTERVAL = 50


def _care_units(rng: random.Random, i: int) -> dict:
    return {
        "CareUnitId": 1000 + i,
        "CareUnitName": f"Avd {rng.randint(1, 40)}",
        "CareUnitType": rng.choice(["ward", "clinic", "lab", "icu"]),
    }


def _drugs(rng: random.Random, i: int) -> dict:
    return {
        "DrugId": 5000 + i,
        "DrugName": rng.choice(["Alvedon", "Ipren", "Trombyl", "Losec", "Levaxin"]),
        "ATC": rng.choice(["N02BE01", "M01AE01", "B01AC06", "A02BC01", "H03AA01"]),
        "StrengthMg": round(rng.uniform(1, 500), 1),
    }


def _professions(rng: random.Random, i: int) -> dict:
    return {
        "ProfessionId": i,
        "ProfessionName": rng.choice(
            ["Läkare", "Sjuksköterska", "Undersköterska", "Fysioterapeut"]
        ),
    }


def _users(rng: random.Random, i: int) -> dict:
    return {
        "UserId": 9000 + i,
        "UserName": f"user{i:04d}",
        "ProfessionId": rng.randint(1, 6),
        "Active": rng.random() > 0.2,
    }


def _generic(rng: random.Random, i: int) -> dict:
    return {"Id": i, "Value": rng.randint(0, 1_000)}


_GENERATORS = {
    "vCodes_CareUnits": _care_units,
    "vCodes_Drugs": _drugs,
    "vCodes_Professions": _professions,
    "vCodes_Users": _users,
}


def read_source(
    *,
    table: str,
    hospital: str,
    since: datetime | None,
    until: datetime | None,
    fetch_size: int,
) -> Generator[pl.DataFrame, None, None]:
    if since is None or until is None:
        raise ValueError("read_source needs a [since, until) window")

    generate = _GENERATORS.get(table, _generic)
    rng = random.Random(f"{hospital}:{table}:{since.isoformat()}")
    span = (until - since).total_seconds()

    rows: list[dict] = []
    for i in range(ROWS_PER_INTERVAL):
        read_at = since + timedelta(seconds=rng.uniform(0, span))
        row = generate(rng, i)
        row["Hospital"] = hospital.upper()
        row["TimestampRead"] = read_at.astimezone(STOCKHOLM).replace(tzinfo=None)
        rows.append(row)

    for start in range(0, len(rows), fetch_size):
        yield pl.DataFrame(rows[start : start + fetch_size], infer_schema_length=None)
