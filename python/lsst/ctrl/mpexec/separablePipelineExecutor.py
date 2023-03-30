# This file is part of ctrl_mpexec.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
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


from __future__ import annotations

__all__ = [
    "SeparablePipelineExecutor",
]


from typing import Iterable

import lsst.pipe.base
import lsst.resources
from lsst.daf.butler import Butler

from .taskFactory import TaskFactory


class SeparablePipelineExecutor:
    """An executor that allows each step of pipeline execution to be
    run independently.

    The executor can run any or all of the following steps:

        * pre-execution initialization
        * pipeline building
        * quantum graph generation
        * quantum graph execution

    Any of these steps can also be handed off to external code without
    compromising the remaining ones.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        A Butler whose ``collections`` and ``run`` attributes contain the input
        and output collections to use for processing.
    clobber_output : `bool`, optional
        If set, the pipeline execution overwrites existing output files.
        Otherwise, any conflict between existing and new outputs is an error.
    skip_existing_in : iterable [`str`], optional
        If not empty, the pipeline execution searches the listed collections
        for existing outputs, and skips any quanta that have run to completion
        (or have no work to do). Otherwise, all tasks are attempted (subject
        to ``clobber_output``).
    task_factory : `lsst.pipe.base.TaskFactory`, optional
        A custom task factory for use in pre-execution and execution. By
        default, a new instance of `lsst.ctrl.mpexec.TaskFactory` is used.
    """

    def __init__(
        self,
        butler: Butler,
        clobber_output: bool = False,
        skip_existing_in: Iterable[str] | None = None,
        task_factory: lsst.pipe.base.TaskFactory | None = None,
    ):
        self._butler = Butler(butler=butler, collections=butler.collections, run=butler.run)
        if not self._butler.collections:
            raise ValueError("Butler must specify input collections for pipeline.")
        if not self._butler.run:
            raise ValueError("Butler must specify output run for pipeline.")

        self._clobber_output = clobber_output
        self._skip_existing_in = list(skip_existing_in) if skip_existing_in else []

        self._task_factory = task_factory if task_factory else TaskFactory()

    def make_pipeline(self, pipeline_uri: str | lsst.resources.ResourcePath) -> lsst.pipe.base.Pipeline:
        """Build a pipeline from pipeline and configuration information.

        Parameters
        ----------
        pipeline_uri : `str` or `lsst.resources.ResourcePath`
            URI to a file containing a pipeline definition. A URI fragment may
            be used to specify a subset of the pipeline, as described in
            :ref:`pipeline-running-intro`.

        Returns
        -------
        pipeline : `lsst.pipe.base.Pipeline`
            The fully-built pipeline.
        """
        return lsst.pipe.base.Pipeline.from_uri(pipeline_uri)
