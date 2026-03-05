import types
import unittest
from unittest.mock import MagicMock
from bollhav.match import _parse_tag_expression, _module_matches


class TestParseTagExpression(unittest.TestCase):
    def test_single_or_group(self):
        self.assertEqual(_parse_tag_expression("[wee|x]"), [[["wee", "x"]]])

    def test_single_tag(self):
        self.assertEqual(_parse_tag_expression("[wee]"), [[["wee"]]])

    def test_and_group(self):
        self.assertEqual(_parse_tag_expression("[xyz&abc]"), [[["xyz"], ["abc"]]])

    def test_and_with_or_parens(self):
        self.assertEqual(_parse_tag_expression("[xyz&(c|e)]"), [[["xyz"], ["c", "e"]]])

    def test_multiple_groups(self):
        self.assertEqual(
            _parse_tag_expression("[wee|x],[xyz&(c|e)]"),
            [[["wee", "x"]], [["xyz"], ["c", "e"]]],
        )

    def test_no_brackets_raises(self):
        with self.assertRaises(ValueError):
            _parse_tag_expression("wee|x")

    def test_empty_raises(self):
        with self.assertRaises(ValueError):
            _parse_tag_expression("")


class TestModuleMatches(unittest.TestCase):
    def _make_module(self, tags: set[str]) -> types.ModuleType:
        module = MagicMock()
        model = MagicMock()
        model.tags = tags
        module.model = model
        return module

    def test_or_match(self):
        module = self._make_module({"wee"})
        self.assertTrue(_module_matches(module, _parse_tag_expression("[wee|x]")))

    def test_or_no_match(self):
        module = self._make_module({"abc"})
        self.assertFalse(_module_matches(module, _parse_tag_expression("[wee|x]")))

    def test_and_match(self):
        module = self._make_module({"xyz", "abc"})
        self.assertTrue(_module_matches(module, _parse_tag_expression("[xyz&abc]")))

    def test_and_missing_one(self):
        module = self._make_module({"xyz"})
        self.assertFalse(_module_matches(module, _parse_tag_expression("[xyz&abc]")))

    def test_and_with_or_parens_match(self):
        module = self._make_module({"xyz", "e"})
        self.assertTrue(_module_matches(module, _parse_tag_expression("[xyz&(c|e)]")))

    def test_and_with_or_parens_no_match(self):
        module = self._make_module({"xyz"})
        self.assertFalse(_module_matches(module, _parse_tag_expression("[xyz&(c|e)]")))

    def test_multiple_groups_first_matches(self):
        module = self._make_module({"wee"})
        self.assertTrue(
            _module_matches(module, _parse_tag_expression("[wee|x],[xyz&(c|e)]"))
        )

    def test_multiple_groups_second_matches(self):
        module = self._make_module({"xyz", "c"})
        self.assertTrue(
            _module_matches(module, _parse_tag_expression("[wee|x],[xyz&(c|e)]"))
        )

    def test_multiple_groups_none_match(self):
        module = self._make_module({"abc"})
        self.assertFalse(
            _module_matches(module, _parse_tag_expression("[wee|x],[xyz&(c|e)]"))
        )

    def test_no_model_attribute(self):
        module = MagicMock(spec=[])
        self.assertFalse(_module_matches(module, _parse_tag_expression("[wee]")))


if __name__ == "__main__":
    unittest.main()
