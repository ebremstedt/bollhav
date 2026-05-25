[← Target](TARGET.md)

# TargetSchema

The schema part of a `Target`. Plain string `name` is enough for most cases — `suffix` and `suffix_appendix` exist for dev/staging schema isolation that auto-rotates over time (e.g. per ISO week).

## name

Type: `str` · Default: `""`

Schema name.

## suffix

Type: `str` · Default: `""`

When set, appended to `name` with an underscore to produce the resolved schema name (e.g. `name="public"`, `suffix="dev"` → `public_dev`).

## suffix_appendix

Type: `str | None` · Default: `"%y%V"`

`strftime` format appended to `name_suffix` to add a rotating timestamp segment — useful for ephemeral dev schemas that recycle weekly. The default `"%y%V"` is the 2-digit year + ISO week number. Set to `None` to drop the rotating segment entirely.

## Computed: `resolved`

`@property` that returns the final schema name:

- `suffix` unset → just `name`
- `suffix` set, `suffix_appendix` unset → `name_suffix`
- both set → `name_suffix_YYWW_` (e.g. `public_dev_2422_`)

The trailing underscore on the timestamped form is intentional — it makes the timestamped portion visually obvious in database UIs.
