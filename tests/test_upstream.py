"""Unit tests for the upstream contract layer (bollhav.model.upstream).

Pure logic, no DB: the three Contract subclasses expose the right `kind`,
the abstract base refuses to, and UpstreamCheck composes the satisfied
verdict + the single STATE_002 blocked reason from its blockers.
"""

import pytest

from bollhav.model.state import BlockCode, format_block_reason
from bollhav.model.upstream import (
    Contract,
    IntervalContract,
    MonolithicContract,
    UpstreamCheck,
    ViewContract,
)


class TestContractKind:
    def test_interval_contract_kind(self):
        assert IntervalContract().kind == "interval"

    def test_view_contract_kind(self):
        assert ViewContract().kind == "view"

    def test_monolithic_contract_kind(self):
        assert MonolithicContract().kind == "monolithic"

    def test_base_contract_kind_is_abstract(self):
        # The base class is pure gating policy with no satisfaction semantics.
        with pytest.raises(NotImplementedError):
            _ = Contract().kind

    def test_carries_no_name(self):
        # A Contract is gating policy only — the Source it sits on owns the
        # upstream's identity, so the contract takes no constructor args.
        assert not hasattr(IntervalContract(), "name")

    def test_is_frozen(self):
        c = ViewContract()
        with pytest.raises(Exception):
            c.x = "other"  # frozen dataclass

    def test_kind_matches_model_vocabulary(self):
        # The contract kinds are exactly the strings Model.kind / the state
        # backend key on.
        kinds = {
            IntervalContract().kind,
            ViewContract().kind,
            MonolithicContract().kind,
        }
        assert kinds == {"interval", "view", "monolithic"}


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
