# This file is part of ctrl_mpexec.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This software is dual licensed under the GNU General Public License and also
# under a 3-clause BSD license. Recipients may choose which of these licenses
# to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# respectively.  If you choose the GPL option then the following text applies
# (but note that there is still no warranty even if you opt for BSD instead):
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

"""Few utility methods used by the rest of a package.
"""

__all__ = ["profile", "printTable", "filterTasks", "subTaskIter"]

# -------------------------------
#  Imports of standard modules --
# -------------------------------
import contextlib
import logging
from collections.abc import Iterator

# -----------------------------
#  Imports for other modules --
# -----------------------------
import lsst.pex.config as pexConfig
from lsst.pipe.base import Pipeline, TaskDef

# ----------------------------------
#  Local non-exported definitions --
# ----------------------------------

# ------------------------
#  Exported definitions --
# ------------------------


@contextlib.contextmanager
def profile(filename: str, log: logging.Logger | None = None) -> Iterator:
    """Context manager for profiling with cProfile

    Parameters
    ----------
    filename :
        Filename to which to write profile (profiling disabled if `None`
        or empty).
    log :
        Log object for logging the profile operations.

    Yields
    ------
    profile : `cProfile.Profile` or `None`
        If profiling is enabled, the context manager returns the
        `cProfile.Profile` object (otherwise it returns `None`)

    Notes
    -----
    The returned profile object allows additional control
    over profiling.  You can obtain this using the ``as`` clause, e.g.:

    .. code-block:: py

        with profile(filename) as prof:
            runYourCodeHere()

    The output cumulative profile can be printed with a command-line like:

    .. code-block:: bash

        python -c 'import pstats; pstats.Stats("<filename>").\
                   sort_stats("cumtime").print_stats(30)'
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


def printTable(rows: list[tuple], header: tuple | None) -> None:
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


def filterTasks(pipeline: Pipeline, name: str | None) -> list[TaskDef]:
    """Find list of tasks matching given name.

    For matching task either task label or task name after last dot should
    be identical to `name`. If task label is non-empty then task name is not
    checked.

    Parameters
    ----------
    pipeline : `Pipeline`
        Pipeline to examine.
    name : `str` or `None`
        If empty or `None` then all tasks are returned.

    Returns
    -------
    tasks : `list` [ `lsst.pipe.base.TaskDef`]
        List of `~lsst.pipe.base.TaskDef` instances that match.
    """
    if not name:
        return list(pipeline.toExpandedPipeline())
    tasks = []
    for taskDef in pipeline.toExpandedPipeline():
        if taskDef.label:
            if taskDef.label == name:
                tasks.append(taskDef)
        elif taskDef.taskName.split(".")[-1] == name:
            tasks.append(taskDef)
    return tasks


def subTaskIter(config: pexConfig.Config) -> Iterator[tuple[str, str]]:
    """Recursively generates subtask names.

    Parameters
    ----------
    config : `lsst.pex.config.Config`
        Configuration of the task

    Returns
    -------
    names : `collections.abc.Iterator` [ `tuple` [ `str`, `str` ] ]
        Iterator which returns tuples of (configFieldPath, taskName).
    """
    for fieldName, field in sorted(config.items()):
        if hasattr(field, "value") and hasattr(field, "target"):
            subConfig = field.value
            if isinstance(subConfig, pexConfig.Config):
                try:
                    taskName = f"{field.target.__module__}.{field.target.__name__}"
                except Exception:
                    taskName = repr(field.target)
                yield fieldName, taskName
                for subFieldName, taskName in subTaskIter(subConfig):
                    yield fieldName + "." + subFieldName, taskName
