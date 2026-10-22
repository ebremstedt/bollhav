from unittest.mock import MagicMock
import os
import sys

import pytest

# roskarl is mocked at import time so tests don't need the real package
# installed. The cron submodule has to be registered separately because
# bollhav imports `roskarl.cron.INTERVAL_EXPRESSION_SHORTCUTS` directly,
# and Python won't synthesize a submodule from a MagicMock parent.
roskarl_mock = MagicMock()
roskarl_cron_mock = MagicMock()
roskarl_cron_mock.INTERVAL_EXPRESSION_SHORTCUTS = {
    "@minutely": "* * * * *",
    "@minute": "* * * * *",
    "@hourly": "0 * * * *",
    "@hour": "0 * * * *",
    "@daily": "0 0 * * *",
    "@day": "0 0 * * *",
    "@weekly": "0 0 * * 0",
    "@week": "0 0 * * 0",
    "@monthly": "0 0 1 * *",
    "@month": "0 0 1 * *",
    "@yearly": "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
}
sys.modules["roskarl"] = roskarl_mock
sys.modules["roskarl.cron"] = roskarl_cron_mock


def parse_env_var_bool(
    name: str,
    default: bool | None = None,
    should_print_unset: bool = True,
    required: bool = False,
) -> bool | None:
    """roskarl's `env_var_bool` contract, reading the real environment: True /
    False for a true/false value (any case), the default when unset, ValueError
    otherwise. roskarl itself is mocked above and tested in its own repo; this
    stands in for it where a test exercises a path that reads it."""
    value = os.environ.get(name)
    if value:
        if value.upper() == "TRUE":
            return True
        if value.upper() == "FALSE":
            return False
        raise ValueError(f"Bool must be set to true or false, not: {value!r}")
    return default


@pytest.fixture
def env_var_bool_from_environ(monkeypatch):
    """`NOTIFICATIONS_ENABLED` read for real on the notification path."""
    monkeypatch.setattr("bollhav.model.notifications.env_var_bool", parse_env_var_bool)
