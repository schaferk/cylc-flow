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

"""xtrigger function to check if a file contains specific text.

"""

import os
import re
import shutil
from tempfile import mkdtemp
from cylc.cylc_subproc import procopen

import file_exists
from timeout_handler import xtrig_allow_timeout


@xtrig_allow_timeout
def file_contains(text=None, path=None, regex=False, user=None,
                  host='localhost', point=-1, min_num_lines=None,
                  strict_retry=False,
                  # The following kwargs are required for the decorators
                  # always specify these as kwargs.
                  # Timeout handling:
                  delay_first_poll_until=None,
                  timeout_first_run=None, timeout_cycle_offset=None,
                  # Set to the status the previous cycles dependent task
                  # needs to be before this xtrigger can run, None is off
                  required_previous_status=None,
                  # These should be defined using the %(...)s notation
                  dependent_task='', suite='', suite_share_dir=''):
    """Return true if the input_file contains the provided text, or
    the timeout has expired.

    Always check the 'success' key in the returned dictionary to determine if
    the checks really passed, or if the trigger just reached its time limit.

    Please see the documentation in the 'timeout_handler' class for how the
    timeouts handling works.

    Required:
      text or regex : what to look for
      path : path to file to look in. See 'file_exists.py' for what is accepted
      point : %(point)s - always use this notation in Cylc to ensure the
                          xtrigger is always unique
    Optional:
      user : username for remote host SSH. Default=None
      host : remote host to look at file on. Default=localhost
      min_num_lines : How many lines should exist in the file at minmum
      strict_retry : Trigger will only retry if (a) it doesn't find the file or
                     (b) it it doesn't find the desired text in the file, and any
                     other rsync return code will make the trigger pass. Subsequent
                     tasks should check the <triggername>_success variable to see
                     if the trigger was really successful. If false, the trigger
                     always retries if it does not succeed. Default is False.

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

    response = {}
    file_status, file_results = file_exists.file_exists(path=path, user=user,
                                                        host=host, point=point,
                                                        strict_retry=strict_retry)

    if file_status and file_results['success']:
        # Use the path provided by the file_exists check as this will have
        # done some pattern matching if required
        path = file_results['path']
        if host in file_exists.LOCAL_HOST_EQUIVALENTS:
            with open(path, 'r') as open_file:
                data = open_file.read()
        else:
            data = _get_remote_file_contents(user, host, path)

        if regex:
            match = re.search(text, data)
            satisfied = match is not None
            if satisfied:
                # The result will only contain the first match
                result = match.group()
        else:
            satisfied = text in data
            result = text
        response = {'text': result, 'path': path, 'host': host}

        if satisfied and min_num_lines is not None:
            satisfied = len(data.splitlines()) >= min_num_lines

        response['success'] = satisfied

    elif strict_retry and file_status and not file_results['success']:
        # file_exists tells us to pass, and indicate trigger failed.
        satisfied = True
        response['success'] = False
    else:
        # retry
        satisfied = False
        response['success'] = False

    return (satisfied, response)


def _get_remote_file_contents(user, host, path):
    # Copy the file over, wrapped in try/finally block to ensure it is deleted
    tmpdir = None
    data = ''
    try:
        tmpdir = mkdtemp()
        tmpfile = os.path.join(tmpdir, 'tmpfile')
        cmd = ['rsync', '--timeout=1800',
               '--rsh=ssh -oBatchMode=yes -oConnectTimeout=10',
               file_exists.get_fully_specified_remote_path(user, host, path),
               tmpfile]
        devnull = open(os.devnull, 'wb')
        proc = procopen(cmd, stdout=devnull, stderr=devnull)
        if proc.wait() == 0:
            with open(tmpfile, 'r') as open_file:
                data = open_file.read()
    finally:
        if tmpdir and os.path.isdir(tmpdir):
            shutil.rmtree(tmpdir)

    return data
