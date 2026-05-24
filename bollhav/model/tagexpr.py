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


def explain_groups(expression: str) -> list[tuple[str, str]]:
    """Return per-group (raw, plain-English) pairs in expression order.

    Useful for rendering a side-by-side breakdown:

        [r:sales & finance][not:legacy]
        →  [(r:sales & finance, sales and finance (reload)),
            (not:[legacy], not legacy)]
    """
    raw_groups = re.findall(
        rf"((?:{_RELOAD_PREFIX_TOKEN}|not:)*)?\[([^\]]+)\]", expression
    )
    parsed = parse_expression(expression)
    out: list[tuple[str, str]] = []
    for (prefix, content), group in zip(raw_groups, parsed):
        raw = f"{prefix}[{content}]"
        out.append((raw, _explain_group(group, parens_when_compound=False)))
    return out


def explain(expression: str) -> str:
    """Render a tag expression in plain English.

    Examples:
        [clean]|[orders]|[customers]   → clean or orders or customers
        [foo & bar]                    → foo and bar
        [(foo|bar) & baz]              → (foo or bar) and baz
        [not:foo]                      → not foo
        not:[foo & bar]                → not (foo and bar)
        [r:foo]                        → foo (reload)
        r:[foo & bar]                  → foo and bar (reload)
        [r_row_100:vPAS]               → vPAS (reload, row mode, 100 rows/chunk)
        [r_interval_@daily:sales]      → sales (reload, daily)
    """
    groups = parse_expression(expression)
    return " or ".join(_explain_group(g, len(groups) > 1) for g in groups)


def _explain_group(group: PotentialTagGroup, parens_when_compound: bool) -> str:
    # Lift the reload suffix to the group when every tag carries the
    # same reload settings — avoids "foo (reload) and bar (reload)".
    keys = {
        (t.reload, t.reload_mode, t.reload_batch_size, t.reload_interval_expression)
        for t in group.tags
    }
    uniform_reload = len(keys) == 1 and next(iter(keys))[0]

    single = len(group.tags) == 1
    tags_text = " and ".join(
        _explain_tag(t, omit_reload=uniform_reload, single_tag_in_group=single)
        for t in group.tags
    )

    needs_parens = len(group.tags) > 1 and (
        parens_when_compound or group.negate or uniform_reload
    )
    if needs_parens:
        tags_text = f"({tags_text})"

    if group.negate:
        tags_text = f"not {tags_text}"
    if uniform_reload:
        tags_text = f"{tags_text} ({_explain_reload(group.tags[0])})"

    return tags_text


def _explain_tag(
    tag: PotentialTagMatch,
    *,
    omit_reload: bool,
    single_tag_in_group: bool,
) -> str:
    cands = " or ".join(tag.candidates)
    # Parens around multi-candidate tags are needed when they coexist
    # with AND-joined siblings (else "foo or bar and baz" is ambiguous)
    # OR when negation wraps them ("not (foo or bar)" vs the wrong
    # "not foo or bar"). Otherwise they're noise.
    if len(tag.candidates) > 1 and (not single_tag_in_group or tag.negate):
        cands = f"({cands})"
    text = f"not {cands}" if tag.negate else cands
    if tag.reload and not omit_reload:
        text = f"{text} ({_explain_reload(tag)})"
    return text


def _explain_reload(tag: PotentialTagMatch) -> str:
    parts = ["reload"]
    # The interval expression already implies INTERVAL mode and the row
    # batch size implies ROW mode — naming the mode in those cases is
    # redundant ("reload, interval mode, daily"). Only show the mode
    # when there's no more specific hint.
    if tag.reload_mode is not None and not (
        tag.reload_batch_size is not None or tag.reload_interval_expression is not None
    ):
        parts.append(f"{tag.reload_mode.value.lower()} mode")
    if tag.reload_batch_size is not None:
        parts.append(f"row mode, {tag.reload_batch_size} rows/chunk")
    if tag.reload_interval_expression is not None:
        parts.append(tag.reload_interval_expression.lstrip("@"))
    return ", ".join(parts)
