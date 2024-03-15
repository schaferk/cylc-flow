# -*- coding: utf-8 -*-

# THIS FILE IS PART OF THE CYLC SUITE ENGINE.
# Copyright (C) 2008-2019 NIWA & British Crown (Met Office) & Contributors.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""xtrigger function to check a remote suite state.
This is an extension to the built-in suite_state xtrigger, adding
in timeouts.
"""

from cylc.cycling.iso8601 import SuiteSpecifics
from cylc.xtriggers.suite_state import suite_state
from isodatetime.parsers import DurationParser
from timeout_handler import xtrig_allow_timeout

SuiteSpecifics.interval_parser = DurationParser()


@xtrig_allow_timeout
def suite_state_plus(
    suite=None,
    task=None,
    point=None,
    offset=None,
    status="succeeded",
    message=None,
    cylc_run_dir=None,
    debug=False,
    # The following kwargs are required for the decorators
    # always specify these as kwargs
    # Timeout handling:
    delay_first_poll_until=None,
    timeout_cycle_offset=None,
):
    """Connect to a suite DB and query the requested task state.

    Reports satisfied only if the remote suite state has been achieved,
    or the poll timeout has been satisfied.
    Returns all suite state args to pass on to triggering tasks. Also,
    a key is added 'success' which advises if the trigger ended due to a
    timeout or not.

    Always check the 'success' key in the returned dictionary to determine if
    the checks really passed, or if the trigger just reached its time limit.

    Please see the documentation in the 'timeout_handler' class for how the
    timeouts handling works.
    """
    satisfied, results = suite_state(
        suite, task, point, offset, status, message, cylc_run_dir, debug
    )
    results["success"] = satisfied
    return (satisfied, results)
