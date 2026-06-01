import pytest
from bollhav.model.tagexpr import (
    PotentialTagMatch,
    PotentialTagGroup,
    explain,
    explain_groups,
    parse_expression,
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
        result = parse_expression("[r:foo]")
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
        result = parse_expression("[r:(foo|bar)]")
        assert result == [
            PotentialTagGroup(
                tags=[PotentialTagMatch(candidates=["foo", "bar"], reload=True)]
            )
        ]

    def test_group_reload_applies_to_all_tags(self):
        result = parse_expression("r:[foo & bar]")
        assert result == [
            PotentialTagGroup(
                tags=[
                    PotentialTagMatch(candidates=["foo"], reload=True),
                    PotentialTagMatch(candidates=["bar"], reload=True),
                ]
            )
        ]

    def test_and_mixed_reload(self):
        result = parse_expression("[r:foo & bar]")
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

    def test_single_tag_negate(self):
        result = parse_expression("[not:foo]")
        assert result == [
            PotentialTagGroup(
                tags=[PotentialTagMatch(candidates=["foo"], reload=False, negate=True)]
            )
        ]

    def test_negate_with_and(self):
        result = parse_expression("[all & not:debug]")
        assert result == [
            PotentialTagGroup(
                tags=[
                    PotentialTagMatch(candidates=["all"], reload=False, negate=False),
                    PotentialTagMatch(candidates=["debug"], reload=False, negate=True),
                ]
            )
        ]

    def test_negate_or_group(self):
        result = parse_expression("[not:(foo|bar)]")
        assert result == [
            PotentialTagGroup(
                tags=[
                    PotentialTagMatch(
                        candidates=["foo", "bar"], reload=False, negate=True
                    )
                ]
            )
        ]

    def test_group_level_negate(self):
        result = parse_expression("not:[foo & bar]")
        assert result == [
            PotentialTagGroup(
                tags=[
                    PotentialTagMatch(candidates=["foo"], reload=False, negate=False),
                    PotentialTagMatch(candidates=["bar"], reload=False, negate=False),
                ],
                negate=True,
            )
        ]

    def test_group_level_reload_and_negate(self):
        result = parse_expression("r:not:[foo]")
        assert result == [
            PotentialTagGroup(
                tags=[PotentialTagMatch(candidates=["foo"], reload=True, negate=False)],
                negate=True,
            )
        ]

    def test_spaces_in_tag_names_are_trimmed(self):
        result = parse_expression("[ foo & bar ]")
        assert result == [
            PotentialTagGroup(
                tags=[
                    PotentialTagMatch(candidates=["foo"], reload=False),
                    PotentialTagMatch(candidates=["bar"], reload=False),
                ]
            )
        ]

    def test_spaces_in_or_candidates_are_trimmed(self):
        result = parse_expression("[foo | bar]")
        assert result == [
            PotentialTagGroup(
                tags=[PotentialTagMatch(candidates=["foo", "bar"], reload=False)]
            )
        ]

    def test_spaces_in_parens_or_candidates_are_trimmed(self):
        result = parse_expression("[(foo | bar)]")
        assert result == [
            PotentialTagGroup(
                tags=[PotentialTagMatch(candidates=["foo", "bar"], reload=False)]
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
        parsed = parse_expression("[r:foo & bar]")
        foo, bar = parsed[0].tags
        assert foo.reload is True
        assert bar.reload is False

    def test_group_level_reload_sets_all(self):
        parsed = parse_expression("r:[foo & bar]")
        assert all(t.reload is True for t in parsed[0].tags)

    def test_or_group_reload(self):
        parsed = parse_expression("[r:(foo|bar)]")
        assert parsed[0].tags[0].reload is True
        assert parsed[0].tags[0].candidates == ["foo", "bar"]

    def test_mixed_groups_reload(self):
        parsed = parse_expression("[r:foo][bar]")
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

    def test_negate_excludes_tag(self):
        assert (
            tags_match({"foo", "debug"}, parse_expression("[foo & not:debug]")) is False
        )

    def test_negate_allows_without_tag(self):
        assert tags_match({"foo"}, parse_expression("[foo & not:debug]")) is True

    def test_negate_or_excludes_any(self):
        assert (
            tags_match({"foo", "bar"}, parse_expression("[foo & not:(bar|baz)]"))
            is False
        )
        assert (
            tags_match({"foo", "baz"}, parse_expression("[foo & not:(bar|baz)]"))
            is False
        )

    def test_negate_or_allows_without(self):
        assert tags_match({"foo"}, parse_expression("[foo & not:(bar|baz)]")) is True

    def test_group_level_negate_inverts_all(self):
        assert tags_match({"foo", "bar"}, parse_expression("not:[foo & bar]")) is False
        assert tags_match({"foo"}, parse_expression("not:[foo & bar]")) is True
        assert tags_match({"baz"}, parse_expression("not:[foo & bar]")) is True

    def test_negate_only(self):
        assert tags_match({"foo"}, parse_expression("[not:foo]")) is False
        assert tags_match({"bar"}, parse_expression("[not:foo]")) is True

    def test_negate_across_groups(self):
        # groups are OR'd: match if group 1 OR group 2 matches
        assert tags_match({"foo"}, parse_expression("[foo][not:bar]")) is True
        assert tags_match({"bar"}, parse_expression("[foo][not:bar]")) is False
        assert tags_match({"baz"}, parse_expression("[foo][not:bar]")) is True
        assert tags_match({"foo", "bar"}, parse_expression("[foo][not:bar]")) is True

    def test_reload_and_negate_tag_level(self):
        # r:sales & not:debug — match sales without debug, reload matched
        parsed = parse_expression("[r:sales & not:debug]")
        assert tags_match({"sales"}, parsed) is True
        assert tags_match({"sales", "debug"}, parsed) is False
        assert tags_match({"debug"}, parsed) is False
        # reload flag should be set on the sales tag
        assert parsed[0].tags[0].reload is True
        assert parsed[0].tags[1].negate is True

    def test_reload_and_negate_group_level(self):
        # r:not:[foo] — match everything without foo, reload all
        parsed = parse_expression("r:not:[foo]")
        assert tags_match({"bar"}, parsed) is True
        assert tags_match({"foo"}, parsed) is False
        assert tags_match({"baz", "qux"}, parsed) is True
        # group should have both reload and negate
        assert parsed[0].negate is True
        assert parsed[0].tags[0].reload is True


# --- reload alias ("reload" == "r") ---


class TestReloadAlias:
    def test_plain_reload_word(self):
        parsed = parse_expression("[reload:foo]")
        assert parsed[0].tags[0].reload is True

    def test_group_level_reload_word(self):
        parsed = parse_expression("reload:[foo & bar]")
        assert all(t.reload is True for t in parsed[0].tags)

    def test_reload_alias_and_r_produce_identical_matches(self):
        a = parse_expression("[r:foo]")
        b = parse_expression("[reload:foo]")
        assert a == b


# --- explain / explain_groups ---


class TestExplain:
    def test_single_tag(self):
        assert explain("[clean]") == "clean"

    def test_multiple_groups_or(self):
        assert explain("[clean]|[orders]|[customers]") == "clean or orders or customers"

    def test_and(self):
        assert explain("[foo & bar]") == "foo and bar"

    def test_or_inside_group(self):
        assert explain("[foo|bar]") == "foo or bar"

    def test_or_inside_with_and(self):
        assert explain("[(foo|bar) & baz]") == "(foo or bar) and baz"

    def test_negate_tag(self):
        assert explain("[not:foo]") == "not foo"

    def test_negate_group(self):
        assert explain("not:[foo & bar]") == "not (foo and bar)"

    def test_reload_tag(self):
        assert explain("[r:foo]") == "foo (reload)"

    def test_reload_group_uniform(self):
        # r:[foo & bar] applies reload to both — should be lifted to group level
        assert explain("r:[foo & bar]") == "(foo and bar) (reload)"

    def test_two_groups_compound(self):
        assert explain("[a & b][c]") == "(a and b) or c"

    def test_negated_or_candidates_keeps_parens(self):
        # "not foo or bar" is ambiguous — the parens have to stay so it
        # reads as "not (foo or bar)" not "(not foo) or bar".
        assert explain("[not:(foo|bar)]") == "not (foo or bar)"

    def test_mixed_reload_in_group_not_lifted(self):
        # When only one tag in an AND-group reloads, the suffix stays on
        # that tag — lifting would falsely imply both reload.
        assert explain("[r:foo & bar]") == "foo (reload) and bar"

    def test_multi_candidate_with_tag_reload(self):
        # Single-tag-in-group + multi-candidates + reload → no parens
        # needed on the candidates, but the reload suffix should hang
        # off the whole tag.
        assert explain("[r:(foo|bar)]") == "foo or bar (reload)"

    def test_multiple_groups_mixed_reload(self):
        # Each group is rendered independently; one reloads, one doesn't.
        assert explain("[r:foo][bar]") == "foo (reload) or bar"

    def test_group_level_reload_with_group_negate(self):
        assert explain("r:not:[foo]") == "not foo (reload)"

    def test_invalid_expression_raises(self):
        import pytest as _pytest

        with _pytest.raises(ValueError, match="Must use \\[group\\] syntax"):
            explain("foo")


class TestExplainGroups:
    def test_pairs_per_group(self):
        pairs = explain_groups("[clean]|[orders]")
        assert pairs == [("[clean]", "clean"), ("[orders]", "orders")]

    def test_pair_preserves_raw_prefix(self):
        pairs = explain_groups("r:[foo & bar]")
        assert pairs == [("r:[foo & bar]", "(foo and bar) (reload)")]

    def test_pair_negate_group(self):
        pairs = explain_groups("not:[foo]")
        assert pairs == [("not:[foo]", "not foo")]
