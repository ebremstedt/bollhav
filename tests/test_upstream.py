"""Unit tests for the upstream contract layer (bollhav.model.upstream).

Pure logic, no DB: the `UpstreamContract` ladder exposes the right level
values, and UpstreamCheck composes the satisfied verdict + the single
STATE_002 blocked reason from its blockers.
"""

import pytest

from bollhav.model.state import BlockCode, format_block_reason
from bollhav.model.upstream import UpstreamCheck, UpstreamContract


class TestUpstreamContract:
    def test_level_values(self):
        # The on-the-wire strings the state backend keys on.
        assert UpstreamContract.EXISTS.value == "exists"
        assert UpstreamContract.EXACT.value == "exact"
        assert UpstreamContract.ENCAPSULATE.value == "encapsulate"
        assert UpstreamContract.THROUGH.value == "through"
        assert UpstreamContract.WHOLE.value == "whole"

    def test_is_str_enum(self):
        # A `str` Enum, so a member compares equal to its value and serializes
        # as the plain string.
        assert UpstreamContract.ENCAPSULATE == "encapsulate"

    def test_no_window_member(self):
        # WINDOW was removed (folded into ENCAPSULATE) — referencing it raises.
        assert not hasattr(UpstreamContract, "WINDOW")
        with pytest.raises(ValueError):
            UpstreamContract("window")

    def test_exactly_five_levels(self):
        assert {c.value for c in UpstreamContract} == {
            "exists",
            "exact",
            "encapsulate",
            "through",
            "whole",
        }
        assert len(list(UpstreamContract)) == 5


class TestUpstreamCheck:
    def test_no_blockers_is_satisfied(self):
        check = UpstreamCheck()
        assert check.satisfied is True
        assert check.reason is None

    def test_default_blockers_empty(self):
        assert UpstreamCheck().blockers == ()

    def test_blockers_means_unsatisfied(self):
        check = UpstreamCheck(blockers=("orders (interval)",))
        assert check.satisfied is False

    def test_reason_is_single_state_002_code(self):
        check = UpstreamCheck(blockers=("orders (interval)",))
        expected = format_block_reason(
            BlockCode.UPSTREAM_NOT_SATISFIED, "orders (interval)"
        )
        assert check.reason == expected
        assert check.reason.startswith("STATE_002")

    def test_reason_joins_every_blocker_without_repeating_the_code(self):
        check = UpstreamCheck(blockers=("orders (interval)", "customers (view)"))
        # One STATE code over the whole list, blockers comma-joined.
        assert check.reason == format_block_reason(
            BlockCode.UPSTREAM_NOT_SATISFIED,
            "orders (interval), customers (view)",
        )
        assert check.reason.count("STATE_002") == 1
