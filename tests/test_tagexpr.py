import pytest
from bollhav.model.tagexpr import (
    PotentialTagMatch,
    PotentialTagGroup,
    parse_expression,
    group_matches,
    tags_match,
)


# --- parse_expression ---


class TestParseExpression:
    def test_single_tag(self):
        result = parse_expression("[foo]")
        assert result == [
            PotentialTagGroup(
                tags=[PotentialTagMatch(candidates=["foo"], reload=False)]
            )
        ]

    def test_single_tag_reload(self):
        result = parse_expression("[!foo]")
        assert result == [
            PotentialTagGroup(tags=[PotentialTagMatch(candidates=["foo"], reload=True)])
        ]

    def test_and_two_tags(self):
        result = parse_expression("[foo & bar]")
        assert result == [
            PotentialTagGroup(
                tags=[
                    PotentialTagMatch(candidates=["foo"], reload=False),
                    PotentialTagMatch(candidates=["bar"], reload=False),
                ]
            )
        ]

    def test_or_with_pipe(self):
        result = parse_expression("[foo|bar]")
        assert result == [
            PotentialTagGroup(
                tags=[PotentialTagMatch(candidates=["foo", "bar"], reload=False)]
            )
        ]

    def test_or_with_parens(self):
        result = parse_expression("[(foo|bar)]")
        assert result == [
            PotentialTagGroup(
                tags=[PotentialTagMatch(candidates=["foo", "bar"], reload=False)]
            )
        ]

    def test_or_with_reload(self):
        result = parse_expression("[!(foo|bar)]")
        assert result == [
            PotentialTagGroup(
                tags=[PotentialTagMatch(candidates=["foo", "bar"], reload=True)]
            )
        ]

    def test_group_reload_applies_to_all_tags(self):
        result = parse_expression("![foo & bar]")
        assert result == [
            PotentialTagGroup(
                tags=[
                    PotentialTagMatch(candidates=["foo"], reload=True),
                    PotentialTagMatch(candidates=["bar"], reload=True),
                ]
            )
        ]

    def test_and_mixed_reload(self):
        result = parse_expression("[!foo & bar]")
        assert result == [
            PotentialTagGroup(
                tags=[
                    PotentialTagMatch(candidates=["foo"], reload=True),
                    PotentialTagMatch(candidates=["bar"], reload=False),
                ]
            )
        ]

    def test_multiple_groups(self):
        result = parse_expression("[foo][bar]")
        assert result == [
            PotentialTagGroup(
                tags=[PotentialTagMatch(candidates=["foo"], reload=False)]
            ),
            PotentialTagGroup(
                tags=[PotentialTagMatch(candidates=["bar"], reload=False)]
            ),
        ]

    def test_complex_and_or(self):
        result = parse_expression("[foo & (bar|baz)]")
        assert result == [
            PotentialTagGroup(
                tags=[
                    PotentialTagMatch(candidates=["foo"], reload=False),
                    PotentialTagMatch(candidates=["bar", "baz"], reload=False),
                ]
            )
        ]

    def test_invalid_expression_raises(self):
        with pytest.raises(ValueError):
            parse_expression("foo & bar")


# --- reload flag ---


class TestReloadFlag:
    def test_no_reload_by_default(self):
        parsed = parse_expression("[foo & bar]")
        assert all(t.reload is False for t in parsed[0].tags)

    def test_tag_level_reload(self):
        parsed = parse_expression("[!foo & bar]")
        foo, bar = parsed[0].tags
        assert foo.reload is True
        assert bar.reload is False

    def test_group_level_reload_sets_all(self):
        parsed = parse_expression("![foo & bar]")
        assert all(t.reload is True for t in parsed[0].tags)

    def test_or_group_reload(self):
        parsed = parse_expression("[!(foo|bar)]")
        assert parsed[0].tags[0].reload is True
        assert parsed[0].tags[0].candidates == ["foo", "bar"]

    def test_mixed_groups_reload(self):
        parsed = parse_expression("[!foo][bar]")
        assert parsed[0].tags[0].reload is True
        assert parsed[1].tags[0].reload is False


# --- tags_match ---


class TestTagsMatch:
    def test_single_tag_matches(self):
        assert tags_match({"foo"}, parse_expression("[foo]")) is True

    def test_single_tag_no_match(self):
        assert tags_match({"bar"}, parse_expression("[foo]")) is False

    def test_and_both_present(self):
        assert tags_match({"foo", "bar"}, parse_expression("[foo & bar]")) is True

    def test_and_one_missing(self):
        assert tags_match({"foo"}, parse_expression("[foo & bar]")) is False

    def test_or_first_matches(self):
        assert tags_match({"foo"}, parse_expression("[foo|bar]")) is True

    def test_or_second_matches(self):
        assert tags_match({"bar"}, parse_expression("[foo|bar]")) is True

    def test_or_none_matches(self):
        assert tags_match({"baz"}, parse_expression("[foo|bar]")) is False

    def test_multiple_groups_first_matches(self):
        assert tags_match({"foo"}, parse_expression("[foo][bar]")) is True

    def test_multiple_groups_second_matches(self):
        assert tags_match({"bar"}, parse_expression("[foo][bar]")) is True

    def test_multiple_groups_none_match(self):
        assert tags_match({"baz"}, parse_expression("[foo][bar]")) is False

    def test_complex_and_or(self):
        parsed = parse_expression("[foo & (bar|baz)]")
        assert tags_match({"foo", "bar"}, parsed) is True
        assert tags_match({"foo", "baz"}, parsed) is True
        assert tags_match({"foo"}, parsed) is False
        assert tags_match({"bar", "baz"}, parsed) is False

    def test_empty_model_tags(self):
        assert tags_match(set(), parse_expression("[foo]")) is False

    def test_extra_model_tags_still_match(self):
        assert tags_match({"foo", "bar", "baz"}, parse_expression("[foo]")) is True
