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


def _parse_candidates(part: str) -> list[str]:
    or_match = re.fullmatch(r"\(([^)]+)\)", part)
    if or_match:
        return [c.strip() for c in or_match.group(1).split("|")]
    if "|" in part:
        return [c.strip() for c in part.split("|")]
    return [part.strip()]


def _parse_potential_match(part: str, group_reload: bool) -> PotentialTagMatch:
    part = part.strip()
    reload = group_reload or part.startswith("r:")
    if part.startswith("r:"):
        part = part[2:]
    negate = part.startswith("not:")
    if part.startswith("not:"):
        part = part[4:]
    return PotentialTagMatch(
        candidates=_parse_candidates(part), reload=reload, negate=negate
    )


def _parse_group(prefix: str, group_content: str) -> PotentialTagGroup:
    group_reload = "r:" in prefix
    group_negate = "not:" in prefix
    tags = [
        _parse_potential_match(part, group_reload) for part in group_content.split("&")
    ]
    return PotentialTagGroup(tags=tags, negate=group_negate)


def parse_expression(expr: str) -> list[PotentialTagGroup]:
    groups = re.findall(r"((?:r:|not:)*)?\[([^\]]+)\]", expr)
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
