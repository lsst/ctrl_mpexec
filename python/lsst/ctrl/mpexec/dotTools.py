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

"""Module defining few methods to generate GraphViz diagrams from pipelines
or quantum graphs.
"""

from __future__ import annotations

__all__ = ["graph2dot", "pipeline2dot"]

from collections.abc import Iterable
from typing import Any

from deprecated.sphinx import deprecated
from lsst.pipe.base import Pipeline, QuantumGraph, TaskDef
from lsst.pipe.base.dot_tools import graph2dot as _graph2dot
from lsst.pipe.base.dot_tools import pipeline2dot as _pipeline2dot


@deprecated(
    "graph2dot should now be imported from lsst.pipe.base.dot_tools. Will be removed in v29.",
    version="v27.0",
    category=FutureWarning,
)
def graph2dot(qgraph: QuantumGraph, file: Any) -> None:
    """Convert QuantumGraph into GraphViz digraph.

    This method is mostly for documentation/presentation purposes.

    Parameters
    ----------
    qgraph : `lsst.pipe.base.QuantumGraph`
        QuantumGraph instance.
    file : `str` or file object
        File where GraphViz graph (DOT language) is written, can be a file name
        or file object.

    Raises
    ------
    OSError
        Raised if the output file cannot be opened.
    ImportError
        Raised if the task class cannot be imported.
    """
    _graph2dot(qgraph, file)


@deprecated(
    "pipeline2dot should now be imported from lsst.pipe.base.dot_tools. Will be removed in v29.",
    version="v27.0",
    category=FutureWarning,
)
def pipeline2dot(pipeline: Pipeline | Iterable[TaskDef], file: Any) -> None:
    """Convert `~lsst.pipe.base.Pipeline` into GraphViz digraph.

    This method is mostly for documentation/presentation purposes.
    Unlike other methods this method does not validate graph consistency.

    Parameters
    ----------
    pipeline : `lsst.pipe.base.Pipeline`
        Pipeline description.
    file : `str` or file object
        File where GraphViz graph (DOT language) is written, can be a file name
        or file object.

    Raises
    ------
    OSError
        Raised if the output file cannot be opened.
    ImportError
        Raised if the task class cannot be imported.
    """
    _pipeline2dot(pipeline, file)
