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

"""Few utility methods used by the rest of a package."""

__all__ = ["filterTaskNodes", "printTable", "subTaskIter"]

from collections.abc import Iterator

import lsst.pex.config as pexConfig
from lsst.pipe.base import PipelineGraph
from lsst.pipe.base.pipeline_graph import TaskNode


def printTable(rows: list[tuple], header: tuple | None) -> None:
    """Nice formatting of 2-column table.

    Parameters
    ----------
    rows : `list` of `tuple`
        Each item in the list is a 2-tuple containg left and righ column
        values.
    header : `tuple` or `None`
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


def filterTaskNodes(pipeline_graph: PipelineGraph, name: str | None) -> list[TaskNode]:
    """Find list of tasks matching given name.

    For matching task either task label or task name after last dot should
    be identical to `name`. If task label is non-empty then task name is not
    checked.

    Parameters
    ----------
    pipeline_graph : `~lsst.pipe.base.PipelineGraph`
        Pipeline graph to examine.
    name : `str` or `None`
        If empty or `None` then all tasks are xreturned.

    Returns
    -------
    tasks : `list` [ `lsst.pipe.base.pipeline_graph.TaskNode`]
        List of task nodes that match.
    """
    if not name:
        return list(pipeline_graph.tasks.values())
    tasks = []
    for task_node in pipeline_graph.tasks.values():
        if task_node.label == name:
            tasks.append(task_node)
        elif task_node.task_class_name.split(".")[-1] == name:
            tasks.append(task_node)
    return tasks


def subTaskIter(config: pexConfig.Config) -> Iterator[tuple[str, str]]:
    """Recursively generates subtask names.

    Parameters
    ----------
    config : `lsst.pex.config.Config`
        Configuration of the task.

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
