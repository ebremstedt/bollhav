import re
from dataclasses import dataclass

from bollhav.model.batch import ChunkMode, _CRON_ALIASES, validate_batch_size


@dataclass
class PotentialTagMatch:
    candidates: list[str]
    reload: bool
    reload_mode: ChunkMode | None = None
    reload_batch_size: int | None = None
    reload_interval_expression: str | None = None
    negate: bool = False


@dataclass
class PotentialTagGroup:
    tags: list[PotentialTagMatch]
    negate: bool = False


# r: or reload:                       -> plain reload
# r_row_<N>: or reload_row_<N>:       -> ROW mode, batch_size=N
# r_interval_@alias: etc.             -> INTERVAL mode, interval_expression=alias
#
# Only the known cron aliases (@hourly, @daily, ...) are accepted inside
# r_interval_ tags. For custom cron expressions, configure the model
# statically or use the pipe-level INTERVAL_EXPRESSION_OVERRIDE env var.
_RELOAD_PREFIX_RE = re.compile(r"(?:r|reload)(?:_row_(\d+)|_interval_(@\w+))?:")
_RELOAD_PREFIX_TOKEN = r"(?:r|reload)(?:_row_\d+|_interval_@\w+)?:"


def _validate_cron_alias(alias: str) -> None:
    if alias not in _CRON_ALIASES:
        allowed = ", ".join(sorted(_CRON_ALIASES))
        raise ValueError(
            f"r_interval_ tag got unknown cron alias {alias!r} — "
            f"allowed aliases: {allowed}"
        )


def _interpret_reload_match(
    m: re.Match,
) -> tuple[ChunkMode | None, int | None, str | None]:
    """Pull (mode, batch_size, interval_expression) out of a regex match on
    _RELOAD_PREFIX_RE. Validates numeric caps and cron aliases."""
    if m.group(1):
        size = int(m.group(1))
        validate_batch_size(size, "r_row_ tag")
        return ChunkMode.ROW, size, None
    if m.group(2):
        alias = m.group(2)
        _validate_cron_alias(alias)
        return ChunkMode.INTERVAL, None, alias
    return None, None, None


def _strip_reload_prefix(
    part: str,
) -> tuple[bool, ChunkMode | None, int | None, str | None, str]:
    """If `part` starts with a reload prefix, consume it. Returns
    (reload, reload_mode, reload_batch_size, reload_interval_expression, remaining)."""
    m = _RELOAD_PREFIX_RE.match(part)
    if not m:
        return False, None, None, None, part
    mode, size, expr = _interpret_reload_match(m)
    return True, mode, size, expr, part[m.end() :]


def _scan_reload_in_prefix(
    prefix: str,
) -> tuple[bool, ChunkMode | None, int | None, str | None]:
    """Find a reload token anywhere in a group-level prefix and extract its
    settings."""
    m = _RELOAD_PREFIX_RE.search(prefix)
    if not m:
        return False, None, None, None
    mode, size, expr = _interpret_reload_match(m)
    return True, mode, size, expr


def _parse_candidates(part: str) -> list[str]:
    or_match = re.fullmatch(r"\(([^)]+)\)", part)
    if or_match:
        return [c.strip() for c in or_match.group(1).split("|")]
    if "|" in part:
        return [c.strip() for c in part.split("|")]
    return [part.strip()]


def _parse_potential_match(
    part: str,
    group_reload: bool,
    group_reload_mode: ChunkMode | None,
    group_reload_batch_size: int | None,
    group_reload_interval_expression: str | None,
) -> PotentialTagMatch:
    part = part.strip()
    (
        tag_reload,
        tag_reload_mode,
        tag_reload_batch_size,
        tag_reload_interval_expression,
        part,
    ) = _strip_reload_prefix(part)
    reload = group_reload or tag_reload
    reload_mode = tag_reload_mode or group_reload_mode
    reload_batch_size = tag_reload_batch_size or group_reload_batch_size
    reload_interval_expression = (
        tag_reload_interval_expression or group_reload_interval_expression
    )
    negate = part.startswith("not:")
    if negate:
        part = part[4:]
    return PotentialTagMatch(
        candidates=_parse_candidates(part),
        reload=reload,
        reload_mode=reload_mode,
        reload_batch_size=reload_batch_size,
        reload_interval_expression=reload_interval_expression,
        negate=negate,
    )


def _parse_group(prefix: str, group_content: str) -> PotentialTagGroup:
    (
        group_reload,
        group_reload_mode,
        group_reload_batch_size,
        group_reload_interval_expression,
    ) = _scan_reload_in_prefix(prefix)
    group_negate = "not:" in prefix
    tags = [
        _parse_potential_match(
            part,
            group_reload,
            group_reload_mode,
            group_reload_batch_size,
            group_reload_interval_expression,
        )
        for part in group_content.split("&")
    ]
    return PotentialTagGroup(tags=tags, negate=group_negate)


def parse_expression(expr: str) -> list[PotentialTagGroup]:
    groups = re.findall(rf"((?:{_RELOAD_PREFIX_TOKEN}|not:)*)?\[([^\]]+)\]", expr)
    if not groups:
        raise ValueError(f"Invalid tag expression: {expr!r}. Must use [group] syntax.")
    return [_parse_group(prefix, content) for prefix, content in groups]


def _tag_matches(model_tags: set[str], tag: PotentialTagMatch) -> bool:
    hit = any(opt in model_tags for opt in tag.candidates)
    return (not hit) if tag.negate else hit


def group_matches(model_tags: set[str], group: PotentialTagGroup) -> bool:
    result = all(_tag_matches(model_tags, tag) for tag in group.tags)
    return (not result) if group.negate else result


def tags_match(model_tags: set[str], parsed: list[PotentialTagGroup]) -> bool:
    return any(group_matches(model_tags, group) for group in parsed)
