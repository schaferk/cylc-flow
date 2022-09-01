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

"""xtrigger function to check if a remote file, or files, exist.

"""

import os
import time
from datetime import datetime
from glob import glob
from cylc.cylc_subproc import procopen
from cylc.cycling.iso8601 import SuiteSpecifics
from isodatetime.parsers import (DurationParser, parse_timepoint_expression,
                                 ISO8601SyntaxError)
from timeout_handler import xtrig_allow_timeout

SuiteSpecifics.interval_parser = DurationParser()

LOCAL_HOST_EQUIVALENTS = ('localhost', '127.0.0.1')
RSYNC_RETRY_VALUES = (0, 23, 24)


@xtrig_allow_timeout
def file_exists(user=None, host='localhost', path=os.sep, point=-1,
                max_age=None, actioned_file_log=None, num_expected=None,
                strict_retry=False,
                # The following kwargs are required for the decorators
                # always specify these as kwargs
                # Timeout handling:
                delay_first_poll_until=None,
                timeout_first_run=None, timeout_cycle_offset=None,
                # Set to the status the previous cycles dependent task
                # needs to be before this xtrigger can run, None is off
                required_previous_status=None,
                # These should be defined using the %(...)s notation
                dependent_task='', suite='', suite_share_dir=''):
    """Return true if the path exists or the timeout has been reached.

    Always check the 'success' key in the returned dictionary to determine if
    the checks really passed, or if the trigger just reached its time limit.

    Please see the documentation in the 'timeout_handler' class for how the
    timeouts handling works.

    Required:
      path : path to file to look for. Can be a glob pattern. Also allowed are
             items related to the cycle point.
             e.g. path='my_file&Y&m&d&H&M' will create the filename from
                  the provided 'point' variable. This will only work for
                  non-integer cycling suites. If you want zero padded month,
                  day, hour, minute, then use &0m, &0d, &0H, &0M respecively.
       point : %(point)s - always use this notation in Cylc to ensure the
                           xtrigger is always unique

      point : %(point)s - always use this notation in Cylc to ensure the
                          xtrigger is always unique

    Optional:
      user : username for remote host SSH. Default=None
      host : remote host to look at file on. Default=localhost
      max_age : maximum age a file can be to count as existing, e.g. PT5M, PT1H
      actioned_file_log : log of which filenames have been actioned. This
                          tool will not update the log file, that must be
                          managed by downstream tasks.
      num_expected : Number of files you expect to match on. If specified,
                     this will only pass if that exact number are found.
      strict_retry: Trigger only retries if rsync returns a value in
                    RSYNC_RETRY_VALUES, and will pass for any other exit code.
                    Subsequent tasks should check the <triggername>_success
                    variable to see if the file was really found. If false,
                    trigger will keep retrying for any exit code other than 0.
                    Default is 'False'.

    Special Optionals:
      required_previous_status : status of the dependent task from the PREVIOUS
                                 cycle that is required before this xtrigger
                                 will execute
      dependent_task : %(name)s - the task that this xtrigger will trigger.
                                  Please make it only one task
      suite : %(suite)s - this suite name
      suite_share_dir : %(suite_share_dir)s - the path to this suites share
                                              directory
    """

    try:
        timepoint = parse_timepoint_expression(point)
    except ISO8601SyntaxError:
        timepoint = None

    # Do some string replacement for the provided path if accepted patterns
    # exist and we are a non-integer cycling suite
    # Technically, if the point>10, this replacement will happen even
    # for integer cycling suites
    if timepoint is not None:
        path = path.replace("&Y", str(timepoint.year)) \
            .replace("&m", str(timepoint.month_of_year)) \
            .replace("&0m", str(timepoint.month_of_year).zfill(2)) \
            .replace("&d", str(timepoint.day_of_month)) \
            .replace("&0d", str(timepoint.day_of_month).zfill(2)) \
            .replace("&H", str(timepoint.hour_of_day)) \
            .replace("&0H", str(timepoint.hour_of_day).zfill(2)) \
            .replace("&M", str(timepoint.minute_of_hour)) \
            .replace("&0M", str(timepoint.minute_of_hour).zfill(2))

    # Construct the results dictionary
    results_dict = {'host': host, 'path': path}

    if max_age is None:
        check_seconds = 0
    else:
        check_seconds = time.time() - SuiteSpecifics.interval_parser.parse(
            max_age).get_seconds()

    if actioned_file_log is not None and os.path.exists(actioned_file_log):
        with open(actioned_file_log, 'r') as log:
            log_text = log.read()
    else:
        log_text = ''

    # Check for the file
    if host in LOCAL_HOST_EQUIVALENTS:
        # We assume that everthing is world readable and we do not
        # need to escalate privileges to see the file, so 'user' does
        # not matter
        found, items = _get_glob_results(glob(path), log_text, check_seconds)
    else:
        fullpath = get_fully_specified_remote_path(user, host, path)
        found, out, ret = _run_rsync(fullpath)
        if found:
            found, items = _get_rsync_results(out, log_text,
                                              check_seconds,
                                              user, host, path)

    if not strict_retry:
        results_dict['success'] = found
    else:
        if found:
            # trigger successful
            results_dict['success'] = True
        elif not found and ret in RSYNC_RETRY_VALUES:
            # retry the trigger
            results_dict['success'] = False
        else:
            # exit the trigger, not successful
            found = True
            results_dict['success'] = False

    # Ensure, if specified, the number of files found matches the expected
    # number
    if ( found and results_dict['success']
         and num_expected is not None and len(items) != num_expected ):
        found = False

    # Update the final dictionary that gets returned
    if found and results_dict['success']:
        results_dict['all_paths'] = ",".join(items)
        results_dict['newest_path'] = items[0]
        results_dict['oldest_path'] = items[-1]

    return (found, results_dict)


def _run_rsync(path):
    cmd = ['rsync', '--relative', '--no-implied-dirs', '--timeout=300',
           '--list-only',
           '--rsh=ssh -oBatchMode=yes -oConnectTimeout=10', path]
    with open(os.devnull, 'wb') as devnull:
        proc = procopen(cmd, stdoutpipe=True, stderr=devnull)
        found = proc.wait() == 0
        out = proc.stdout.read().splitlines()
        ret = proc.returncode
    return (found, out, ret)


def get_fully_specified_remote_path(user, host, path):
    """Construct full path to remote file"""
    path = '{host}:{path}'.format(host=host, path=path)
    if user:
        path = '{user}@{path}'.format(user=user, path=path)
    return path


def _get_glob_results(glob_items, log_text, check_seconds):
    """
    Analyse results from the glob list of items and determine if any
    suitable files are found.
    """
    if glob_items:
        # Get the list of suitable items with the newest file first
        items = sorted([f for f in glob_items
                        if (os.path.getmtime(f) >= check_seconds
                            and os.path.basename(f) not in log_text)],
                       key=os.path.getmtime, reverse=True)
        found = bool(items)
    else:
        items = []
        found = False
    return (found, items)


def _get_rsync_results(output, log_text, check_seconds,
                       user, host, path):
    """
    Analyse results from the rsync list of items and determine if any
    suitable files are found.
    """
    # Get the list of suitable items with the newest file first
    files_and_dts = [_get_filename_and_mtime(f, user, host, path)
                     for f in output]
    # Confirm the file isn't too old, and it hasn't been actioned yet.
    # As this script doesn't know what people put in the actioned file log,
    # make sure the full path to the filename and the filename itself are
    # not in there.
    file_dt = [f for f in files_and_dts
               if f[2] >= check_seconds
               and f[1] not in log_text
               and os.path.basename(f[1]) not in log_text]
    found = bool(file_dt)
    if found:
        file_dt.sort(key=lambda x: x[2], reverse=True)
        items = [f[0] for f in file_dt]
    else:
        items = []
    return (found, items)


def _get_filename_and_mtime(rsync_line, user, host, path):
    _, _, d, t, f = rsync_line.split()
    if path[0] == os.path.sep and f[0] != os.path.sep:
        f = os.path.sep + f
    fullpath = get_fully_specified_remote_path(user, host, f)
    dt = datetime.strptime(d + ' ' + t, '%Y/%m/%d %H:%M:%S')
    return (fullpath, f, int(dt.strftime('%s')))
