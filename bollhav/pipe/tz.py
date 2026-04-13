from datetime import datetime, tzinfo
from zoneinfo import ZoneInfo
from roskarl import env_var


def _resolve_tz_override() -> tzinfo | None:
    raw = env_var(name="TIMEZONE_OVERRIDE", should_print_unset=False)
    if raw is None:
        return None
    try:
        return ZoneInfo(raw)
    except KeyError:
        raise ValueError(f"TIMEZONE_OVERRIDE is not a valid IANA timezone: {raw!r}")


def _apply_tz(dt: datetime | None, tz: tzinfo) -> datetime | None:
    if dt is None:
        return None
    return dt.replace(tzinfo=tz)
