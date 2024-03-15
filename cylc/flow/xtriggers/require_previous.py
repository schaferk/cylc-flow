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

import os
import sqlite3
from cylc.dbstatecheck import CylcSuiteDBChecker
from isodatetime.parsers import TimePointParser


def previous_finished(suite, share_dir, point, dependent_task,
                      required_status):
    try:
        checker = CylcSuiteDBChecker(os.path.dirname(share_dir), suite)
    except (OSError, sqlite3.Error):
        # Failed to connect to DB; target suite may not be started
        return False
    fmt = checker.get_remote_point_format()
    if fmt:
        my_parser = TimePointParser()
        point = str(my_parser.parse(point, dump_format=fmt))

    # Find the two most recent cycles and we will only check the status for
    # these. If we do all cycles since the beginning of time, there could
    # be quite a few task states to check

    query = ('SELECT cycle FROM task_states'
             ' WHERE name==?')
    cursors = [dependent_task, point]
    if point.isdigit():
        query += (' AND CAST(cycle AS int)<CAST(? AS int)'
                  ' ORDER BY CAST(cycle AS int) DESC')
    else:
        # cast the cycles to ints and compare those, as well as the raw cycle
        # 20190223T1200Z == 20190223T0000Z with the cast, so <=, and compare
        # the raw cycle point with a < to account for the 12Z vs 00Z. We need
        # to cast to int in case it is an integer cycling suite. If doing a
        # string comparison of 11 vs 2, 11<2, so we would get incorrect cycles

        # We also order by the integer cycle first, and then the string cycle
        # for the same reasons as the WHERE clause
        query += (' AND CAST(cycle AS int)<=CAST(? AS int)'
                  ' AND cycle<?'
                  ' ORDER BY CAST(cycle AS int) DESC, cycle DESC')

        # Add another point to the cursor as we need two instead of one
        cursors.append(point)

    # Now add on the limiting
    query += ' LIMIT 2'

    # Check the cycles to make sure they have finished
    for cp in checker.conn.execute(query, cursors):
        if not checker.task_state_met(dependent_task, cp[0],
                                      status=required_status):
            return False
    return True
