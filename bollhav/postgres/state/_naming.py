from __future__ import annotations

import hashlib

# ── deterministic state-table naming ─────────────────────────────────
#
# Per-model STATE tables live in the one bollhav schema (`z_bollhav`, or
# `z_bollhav_<suffix>` for a dev branch — see `_library_schema`), named by a
# pure function of the model's `canonical_full_name` (base schema, no
# suffix/appendix). The schema carries the suffix + weekly appendix; the table
# name does not — so it stays stable across that rotation (one cleanly-named
# table per model per schema). ERRORS are NOT per-model: every model logs to
# one shared `errors` table in the same schema, keyed by `full_name`.

_VOWELS = frozenset("aeiou")
_SLUG_CAP = 44  # max slug chars; + "_" + 16-hex digest = ≤ 61 (under 63)
_CONTEXT_CAP = 16  # max chars for the de-vowelled catalog/schema context
_HEX_LEN = 16  # 64-bit digest → ~1e-12 collision risk at 10k models


def _devowel(s: str) -> str:
    return "".join(c for c in s if c.lower() not in _VOWELS)


def _name_digest(full_name: str) -> str:
    """16-hex (64-bit) blake2b digest of the model's full name — the
    uniqueness tail of every state/error table name and the stem of their
    index names. Deterministic (blake2b, not the salted built-in hash)."""
    return hashlib.blake2b(
        full_name.encode("utf-8"), digest_size=_HEX_LEN // 2
    ).hexdigest()


def state_table_name(full_name: str) -> str:
    """Deterministic, collision-safe, ≤63-char name for a model's STATE table
    in `z_bollhav_state`.

    Catalog/schema are de-vowelled (compressed, cosmetic) and the table name
    is kept readable; the budget favours the table so it survives. Identity
    rides in the digest, which is hashed over the FULL, unmodified name — so
    truncating the readable slug can never cause a collision.

        intelligence_raw_dan.vPatInfo
            -> ntllgnc_rw_dn_vpatinfo_de90fb57d928ba26
    """
    digest = _name_digest(full_name)
    parts = full_name.lower().replace("-", "_").split(".")
    table = parts[-1]
    context = "_".join(_devowel(p) for p in parts[:-1])[:_CONTEXT_CAP]
    table_budget = _SLUG_CAP - len(context) - 1 if context else _SLUG_CAP
    table = table[:table_budget]
    slug = f"{context}_{table}" if context else table
    return f"{slug}_{digest}"
