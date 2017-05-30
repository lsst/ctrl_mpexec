#
# LSST Data Management System
# Copyright 2017 LSST Corporation.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
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
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.
#
"""
Few utility methods used by the rest of a package.
"""

from __future__ import print_function

__all__ = ["profile", "printTable", "fixPath"]

#--------------------------------
#  Imports of standard modules --
#--------------------------------
import contextlib
import os

#-----------------------------
# Imports for other modules --
#-----------------------------
#----------------------------------
# Local non-exported definitions --
#----------------------------------

#------------------------
# Exported definitions --
#------------------------

@contextlib.contextmanager
def profile(filename, log=None):
    """!Context manager for profiling with cProfile

    @param filename     filename to which to write profile (profiling disabled if None or empty)
    @param log          log object for logging the profile operations

    If profiling is enabled, the context manager returns the cProfile.Profile object (otherwise
    it returns None), which allows additional control over profiling.  You can obtain this using
    the "as" clause, e.g.:

        with profile(filename) as prof:
            runYourCodeHere()

    The output cumulative profile can be printed with a command-line like:

        python -c 'import pstats; pstats.Stats("<filename>").sort_stats("cumtime").print_stats(30)'
    """
    if not filename:
        # Nothing to do
        yield
        return
    from cProfile import Profile

    prof = Profile()
    if log is not None:
        log.info("Enabling cProfile profiling")
    prof.enable()
    yield prof
    prof.disable()
    prof.dump_stats(filename)
    if log is not None:
        log.info("cProfile stats written to %s" % filename)


def printTable(rows, header):
    """Nice formatting of 2-column table.

    Parameters
    ----------
    rows : `list` of `tuple`
        Each item in the list is a 2-tuple containg left and righ column values
    header: `tuple` or `None`
        If `None` then table header are not prined, otherwise it's a 2-tuple
        with column headings.
    """
    if not rows:
        return
    width = max(len(x[0]) for x in rows)
    if header:
        width = max(width, len(header[0]))
        print(header[0].ljust(width), header[1])
        print("".ljust(width, "-"), "".ljust(len(header[1]), "-"))
    for col1, col2 in rows:
        print(col1.ljust(width), col2)


def fixPath(defName, path):
    """!Apply environment variable as default root, if present, and abspath

    @param[in] defName  name of environment variable containing default root path;
        if the environment variable does not exist then the path is relative
        to the current working directory
    @param[in] path     path relative to default root path
    @return abspath: path that has been expanded, or None if the environment variable does not exist
        and path is None
    """
    defRoot = os.environ.get(defName)
    if defRoot is None:
        if path is None:
            return None
        return os.path.abspath(path)
    return os.path.abspath(os.path.join(defRoot, path or ""))
