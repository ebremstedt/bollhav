[← Model](MODEL.md)

# Bounds

The historical envelope for backfill mode. Bollhav uses `begin` and `end` to decide what range to walk when `BACKFILL_ENABLED=true`. In `latest` mode these are ignored — latest reads from `now()`.

## begin

Type: `datetime` · Default: `None`

Backfill start. Must be UTC-aware (`tzinfo=timezone.utc`); naive datetimes raise at validation.

## end

Type: `datetime` · Default: `None`

Backfill end. Must be UTC-aware. When `None`, backfill walks up to `now()`.
