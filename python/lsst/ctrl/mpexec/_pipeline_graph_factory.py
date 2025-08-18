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

from __future__ import annotations

__all__ = ("PipelineGraphFactory",)

from lsst.daf.butler import Butler
from lsst.pipe.base import Pipeline, PipelineGraph


class PipelineGraphFactory:
    """A factory for building and caching a PipelineGraph.

    Parameters
    ----------
    pipeline : `lsst.pipe.base.Pipeline`
        Pipeline definition to start from.
    butler : `lsst.daf.butler.Butler` or `None`, optional
        Butler that can be used to resolve dataset type definitions and get
        dimension schema.
    select_tasks : `str`, optional
        String expression that filters the tasks in the pipeline graph.
    pipeline_graph : `lsst.pipe.base.pipeline_graph.PipelineGraph`, optional
        Already-constructed pipeline graph.
    """

    def __init__(
        self,
        pipeline: Pipeline | None = None,
        butler: Butler | None = None,
        select_tasks: str = "",
        *,
        pipeline_graph: PipelineGraph | None = None,
    ):
        if pipeline is None and pipeline_graph is None:
            raise TypeError("At least one of 'pipeline' and 'pipeline_graph' must not be `None`.")
        self._pipeline = pipeline
        self._registry = butler.registry if butler is not None else None
        self._select_tasks = select_tasks
        self._pipeline_graph: PipelineGraph | None = pipeline_graph
        self._resolved: bool = False
        self._for_visualization_only: bool = False

    def __call__(self, *, resolve: bool = True, visualization_only: bool = False) -> PipelineGraph:
        if self._pipeline_graph is None:
            assert self._pipeline is not None, "Guaranteed at construction."
            self._pipeline_graph = self._pipeline.to_graph()
            if self._select_tasks:
                self._pipeline_graph = self._pipeline_graph.select(self._select_tasks)
        if resolve and not self._resolved:
            self._pipeline_graph.resolve(self._registry, visualization_only=visualization_only)
            self._resolved = True
            self._for_visualization_only = self._registry is None
        elif resolve and not visualization_only and self._for_visualization_only:
            raise RuntimeError("Cannot resolve pipeline graph without butler.")
        return self._pipeline_graph

    @property
    def pipeline(self) -> Pipeline:
        """The original pipeline definition."""
        if self._pipeline is None:
            raise RuntimeError("Cannot obtain pipeline from pipeline graph.")
        if self._select_tasks:
            raise RuntimeError(
                "The --select-tasks option cannot be used with operations that return or display a "
                "pipeline as YAML, since it only operates on the pipeline graph."
            )
        return self._pipeline

    def __bool__(self) -> bool:
        if self._pipeline is not None:
            return bool(self._pipeline)
        else:
            assert self._pipeline_graph is not None, "Guaranteed at construction."
            return bool(self._pipeline_graph.tasks)
