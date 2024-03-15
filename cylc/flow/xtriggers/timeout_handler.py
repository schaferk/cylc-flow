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

import functools
import inspect
import os
from isodatetime.parsers import DurationParser, TimePointParser
from isodatetime.timezone import get_local_time_zone
from cylc.xtriggers.wall_clock import wall_clock
import cylc.cycling.iso8601 as iso8601
from cylc.cycling.iso8601 import point_parse, SuiteSpecifics
from get_func_defaults import get_defaults
from require_previous import previous_finished

# for now assume UTC
SuiteSpecifics.DUMP_FORMAT = iso8601.DATE_TIME_FORMAT + "Z"
SuiteSpecifics.ASSUMED_TIME_ZONE = (0, 0)

# Setup parsers
SuiteSpecifics.interval_parser = DurationParser()
SuiteSpecifics.point_parser = TimePointParser(
    allow_only_basic=False,
    allow_truncated=True,
    num_expanded_year_digits=0,
    dump_format=SuiteSpecifics.DUMP_FORMAT,
    assumed_time_zone=SuiteSpecifics.ASSUMED_TIME_ZONE,
)


def get_point_as_seconds(point):
    """This is largely the same as the get_point_as_seconds from the
    task_proxy.py code, but with the self aspects removed. Ideally this
    would be adjusted in there."""

    iso_timepoint = point_parse(str(point))
    point_as_seconds = int(iso_timepoint.get("seconds_since_unix_epoch"))
    if iso_timepoint.time_zone.unknown:
        utc_offset_hours, utc_offset_minutes = get_local_time_zone()
        utc_offset_in_seconds = 3600 * utc_offset_hours + 60 * utc_offset_minutes
        point_as_seconds += utc_offset_in_seconds
    return point_as_seconds


def xtrig_allow_timeout(func):
    """This decorator function allows any xtrigger to have an end time before
    returning a 'True' result so that the xtrigger will pass. If it has timed
    out, then the first item in the returned tuple will be True, but the
    second item, the dictionary, should be set to have a 'success' key, or
    otherwise, which has the real result. That is up to any individual xtrigger
    to implement.

    Required kwargs:
      suite : the suite name, defined by %(suite)s in Cylc
      point : the cycle point, defined by %(point)s in Cylc
      dependent_task : the task this xtrigger will trigger, defined by
                       %(name)s in Cylc
      suite_share_dir : the path to the suites share directory, defined by
                        %(suite_share_dir)s in Cylc

    Semi-optional kwargs:
      required_previous_status : required status for the previous cycles
                                 dependent_task before allowing this xtrigger
                                 to start executing
                                 Suggested values in:
                                 ('finish', 'start', 'submit', 'fail',
                                  'succeed')
                                 The first run can only happen after this is
                                 satisfied
      delay_first_poll_until : only start polling after this time past the cycle point
                               (e.g. PT8H)
      timeout_first_run : timeout value for after the first run (e.g. PT5M)
      timeout_cycle_offset : timeout value for after the cycle time
                             (e.g. PT4H30M)
    """

    @functools.wraps(func)
    def handle(*args, **kwargs):
        if handle.handler is not None:
            # A parent method is handling the timeout
            return func(*args, **kwargs)

        suite = kwargs.get("suite", defaults["suite"])
        point = kwargs.get("point", defaults["point"])
        dependent_task = kwargs.get(
            "dependent_task", defaults.get("dependent_task", None)
        )
        share_dir = kwargs.get("suite_share_dir", defaults.get("suite_share_dir", ""))
        delay_first_poll_until = kwargs.get(
            "delay_first_poll_until", defaults.get("delay_first_poll_until", None)
        )
        timeout_first_run = kwargs.get(
            "timeout_first_run", defaults.get("timeout_first_run", None)
        )
        timeout_cycle_offset = kwargs.get(
            "timeout_cycle_offset", defaults.get("timeout_cycle_offset", None)
        )
        required_previous_status = kwargs.get("required_previous_status", None)

        # First call to the decorator, handle timeouts with this one
        handle.handler = TimeoutHandler(
            point,
            dependent_task,
            share_dir,
            delay_first_poll_until,
            timeout_first_run,
            timeout_cycle_offset,
        )

        if not handle.handler.has_start_timeout_expired() or (
            required_previous_status is not None
            and dependent_task is not None
            and not previous_finished(
                suite, share_dir, point, dependent_task, required_previous_status
            )
        ):
            return (False, {})

        # Retrieve the return values from the actual originating function call
        # and update them based on the timeout_handler
        satisfied, results = func(*args, **kwargs)
        satisfied = handle.handler.has_timeout_expired(satisfied)
        handle.handler.cleanup(satisfied)
        return (satisfied, results)

    # Figure out default kwargs
    defaults = get_defaults(func)

    # Cache the TimeoutHandler object so only one is created
    handle.handler = None

    return handle


class TimeoutHandler:
    """Xtrigger to handle timeout checking.

    At least one of timeout_first_run and timeout_cycle_offset must
    be set to a non-None value.
    """

    def __init__(
        self,
        point,
        dependent_task,
        suite_share_dir,
        delay_first_poll_until=None,
        timeout_first_run=None,
        timeout_cycle_offset=None,
    ):
        """Init

        Required Args:
          point : cycle point
          dependent_task : task that this xtrigger will trigger
          suite_share_dir : path to the share directory
        Provide one of these Args:
          timeout_first_run : Suitable for any suite type, but most likely
                              this is useful for integer cycling suites.
                              This option takes precedence over the
                              'timeout_cycle_offset' option.
                              If this option is used, 'suite_share_dir' must
                              be provided.

          timeout_cycle_offset : Suitable for cycling suites to only poll for a
                                 period after the cycle point. This option will
                                 not be used if 'timeout_first_run' is set.

        Also you can provide:
          delay_first_poll_until : Don't start polling until this interval has
                                   been reached.

        After the specified time period, a True result will be returned.
        """
        parent = inspect.currentframe().f_back.f_code.co_name
        self.point = point
        self.tmpdir = os.path.join(suite_share_dir, "data")
        self.tmpfile = os.path.join(
            self.tmpdir,
            "xtrigger.{parent}.{name}.{point}".format(
                parent=parent, name=dependent_task, point=point
            ),
        )
        self.delay_first_poll_until = delay_first_poll_until
        self.timeout_first_run = timeout_first_run
        self.timeout_cycle_offset = timeout_cycle_offset

    def has_start_timeout_expired(self):
        if self.delay_first_poll_until is None:
            return True

        point_as_seconds = get_point_as_seconds(self.point)
        return wall_clock(
            offset=self.delay_first_poll_until, point_as_seconds=point_as_seconds
        )

    def has_timeout_expired(self, found):
        if found:
            return True

        tmpfile = self.tmpfile
        if self.timeout_first_run is not None:
            if not os.path.exists(self.tmpdir):
                os.makedirs(self.tmpdir)
            if not os.path.exists(tmpfile):
                with open(tmpfile, "a") as tf:
                    os.utime(tf, None)
            start_time = os.path.getmtime(tmpfile)
            found = wall_clock(
                offset=self.timeout_first_run, point_as_seconds=start_time
            )
        elif self.timeout_cycle_offset is not None:
            point_as_seconds = get_point_as_seconds(self.point)
            found = wall_clock(
                offset=self.timeout_cycle_offset, point_as_seconds=point_as_seconds
            )

        return found

    def cleanup(self, found):
        if found and os.path.exists(self.tmpfile):
            os.remove(self.tmpfile)
