from unittest.mock import MagicMock
import sys

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
