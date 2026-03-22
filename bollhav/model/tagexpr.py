import re
from dataclasses import dataclass


@dataclass
class PotentialTagMatch:
    candidates: list[str]
    reload: bool


@dataclass
class PotentialTagGroup:
    tags: list[PotentialTagMatch]


def _parse_candidates(part: str) -> list[str]:
    or_match = re.fullmatch(r"\(([^)]+)\)", part)
    if or_match:
        return or_match.group(1).split("|")
    if "|" in part:
        return part.split("|")
    return [part]


def _parse_potential_match(part: str, group_reload: bool) -> PotentialTagMatch:
    part = part.strip()
    reload = group_reload or part.startswith("!")
    if part.startswith("!"):
        part = part[1:]
    return PotentialTagMatch(candidates=_parse_candidates(part), reload=reload)


def _parse_group(bang: str, group_content: str) -> PotentialTagGroup:
    group_reload = bang == "!"
    tags = [
        _parse_potential_match(part, group_reload) for part in group_content.split("&")
    ]
    return PotentialTagGroup(tags=tags)


def parse_expression(expr: str) -> list[PotentialTagGroup]:
    groups = re.findall(r"(!?)\[([^\]]+)\]", expr)
    if not groups:
        raise ValueError(f"Invalid tag expression: {expr!r}. Must use [group] syntax.")
    return [_parse_group(bang, content) for bang, content in groups]


def group_matches(model_tags: set[str], tags: list[PotentialTagMatch]) -> bool:
    return all(any(opt in model_tags for opt in tag.candidates) for tag in tags)


def tags_match(model_tags: set[str], parsed: list[PotentialTagGroup]) -> bool:
    return any(group_matches(model_tags, group.tags) for group in parsed)
