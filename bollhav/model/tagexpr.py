import re
from dataclasses import dataclass


@dataclass
class PotentialTagMatch:
    candidates: list[str]
    reload: bool
    negate: bool = False


@dataclass
class PotentialTagGroup:
    tags: list[PotentialTagMatch]
    negate: bool = False


_RELOAD_PREFIX_RE = re.compile(r"(?:r|reload):")
_RELOAD_PREFIX_TOKEN = r"(?:r|reload):"


def _strip_reload_prefix(part: str) -> tuple[bool, str]:
    """If `part` starts with a reload prefix, consume it. Returns
    (reload, remaining)."""
    m = _RELOAD_PREFIX_RE.match(part)
    if not m:
        return False, part
    return True, part[m.end() :]


def _scan_reload_in_prefix(prefix: str) -> bool:
    """True if a reload token appears anywhere in a group-level prefix."""
    return _RELOAD_PREFIX_RE.search(prefix) is not None


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
) -> PotentialTagMatch:
    part = part.strip()
    tag_reload, part = _strip_reload_prefix(part)
    reload = group_reload or tag_reload
    negate = part.startswith("not:")
    if negate:
        part = part[4:]
    return PotentialTagMatch(
        candidates=_parse_candidates(part),
        reload=reload,
        negate=negate,
    )


def _parse_group(prefix: str, group_content: str) -> PotentialTagGroup:
    group_reload = _scan_reload_in_prefix(prefix)
    group_negate = "not:" in prefix
    tags = [
        _parse_potential_match(part, group_reload) for part in group_content.split("&")
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
    """
    groups = parse_expression(expression)
    return " or ".join(_explain_group(g, len(groups) > 1) for g in groups)


def _explain_group(group: PotentialTagGroup, parens_when_compound: bool) -> str:
    # Lift the reload suffix to the group when every tag carries the
    # same reload flag — avoids "foo (reload) and bar (reload)".
    uniform_reload = len({t.reload for t in group.tags}) == 1 and group.tags[0].reload

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
        tags_text = f"{tags_text} (reload)"

    return tags_text


def _explain_tag(
    tag: PotentialTagMatch,
    *,
    omit_reload: bool,
    single_tag_in_group: bool,
) -> str:
    cands = " or ".join(tag.candidates)
    if len(tag.candidates) > 1 and (not single_tag_in_group or tag.negate):
        cands = f"({cands})"
    text = f"not {cands}" if tag.negate else cands
    if tag.reload and not omit_reload:
        text = f"{text} (reload)"
    return text
