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

__all__ = ("SingleQuantumExecutor",)

import uuid
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any

from deprecated.sphinx import deprecated

import lsst.pipe.base.single_quantum_executor

if TYPE_CHECKING:
    from lsst.daf.butler import Butler, ButlerMetrics, LimitedButler, Quantum
    from lsst.pipe.base import ExecutionResources, PipelineTask, QuantumSuccessCaveats, TaskFactory
    from lsst.pipe.base.pipeline_graph import TaskNode


# TODO[DM-51962]: Remove this module.
@deprecated(
    "The SingleQuantumExecutor class has moved to lsst.pipe.base.single_quantum_executor. "
    "This forwarding shim will be removed after v30.",
    version="v30",
    category=FutureWarning,
)
class SingleQuantumExecutor(lsst.pipe.base.single_quantum_executor.SingleQuantumExecutor):
    """Executor class which runs one Quantum at a time.

    This is a deprecated backwards-compatibility shim for
    `lsst.pipe.base.single_quantum_executor.SingleQuantumExecutor`, which has
    the same functionality with very minor interface changes.

    Parameters
    ----------
    butler : `~lsst.daf.butler.Butler` or `None`
        Data butler, `None` means that Quantum-backed butler should be used
        instead.
    taskFactory : `~lsst.pipe.base.TaskFactory`
        Instance of a task factory.
    skipExistingIn : `~typing.Any`
        Expressions representing the collections to search for existing output
        datasets. See :ref:`daf_butler_ordered_collection_searches` for allowed
        types. This class only checks for the presence of butler output run in
        the list of collections. If the output run is present in the list then
        the quanta whose complete outputs exist in the output run will be
        skipped. `None` or empty string/sequence disables skipping.
    clobberOutputs : `bool`, optional
        If `True`, then outputs from a quantum that exist in output run
        collection will be removed prior to executing a quantum. If
        ``skipExistingIn`` contains output run, then only partial outputs from
        a quantum will be removed. Only used when ``butler`` is not `None`.
    enableLsstDebug : `bool`, optional
        Enable debugging with ``lsstDebug`` facility for a task.
    limited_butler_factory : `Callable`, optional
        A method that creates a `~lsst.daf.butler.LimitedButler` instance for a
        given Quantum. This parameter must be defined if ``butler`` is `None`.
        If ``butler`` is not `None` then this parameter is ignored.
    resources : `~lsst.pipe.base.ExecutionResources`, optional
        The resources available to this quantum when executing.
    skipExisting : `bool`, optional
        If `True`, skip quanta whose metadata datasets are already stored.
        Unlike ``skipExistingIn``, this works with limited butlers as well as
        full butlers.  Always set to `True` if ``skipExistingIn`` matches
        ``butler.run``.
    assumeNoExistingOutputs : `bool`, optional
        If `True`, assume preexisting outputs are impossible (e.g. because this
        is known by higher-level code to be a new ``RUN`` collection), and do
        not look for them.  This causes the ``skipExisting`` and
        ``clobberOutputs`` options to be ignored, but unlike just setting both
        of those to `False`, it also avoids all dataset existence checks.
    raise_on_partial_outputs : `bool`, optional
        If `True` raise exceptions chained by
        `lsst.pipe.base.AnnotatedPartialOutputError` immediately, instead of
        considering the partial result a success and continuing to run
        downstream tasks.
    job_metadata : `~collections.abc.Mapping`
        Mapping with extra metadata to embed within the quantum metadata under
        the "job" key.  This is intended to correspond to information common
        to all quanta being executed in a single process, such as the time
        taken to load the quantum graph in a BPS job.
    """

    def __init__(
        self,
        butler: Butler | None,
        taskFactory: TaskFactory,
        skipExistingIn: Any = None,
        clobberOutputs: bool = False,
        enableLsstDebug: bool = False,
        limited_butler_factory: Callable[[Quantum], LimitedButler] | None = None,
        resources: ExecutionResources | None = None,
        skipExisting: bool = False,
        assumeNoExistingOutputs: bool = False,
        raise_on_partial_outputs: bool = True,
        job_metadata: Mapping[str, int | str | float] | None = None,
    ):
        super().__init__(
            butler=butler,
            task_factory=taskFactory,
            skip_existing_in=skipExistingIn,
            clobber_outputs=clobberOutputs,
            enable_lsst_debug=enableLsstDebug,
            limited_butler_factory=limited_butler_factory,
            resources=resources,
            skip_existing=skipExisting,
            assume_no_existing_outputs=assumeNoExistingOutputs,
            raise_on_partial_outputs=raise_on_partial_outputs,
            job_metadata=job_metadata,
        )

    def checkExistingOutputs(
        self, quantum: Quantum, task_node: TaskNode, /, limited_butler: LimitedButler
    ) -> bool:
        return super().check_existing_outputs(quantum, task_node, limited_butler=limited_butler)

    def updatedQuantumInputs(
        self, quantum: Quantum, task_node: TaskNode, /, limited_butler: LimitedButler
    ) -> Quantum:
        return super().updated_quantum_inputs(quantum, task_node, limited_butler=limited_butler)

    def runQuantum(
        self,
        task: PipelineTask,
        quantum: Quantum,
        task_node: TaskNode,
        /,
        limited_butler: LimitedButler,
        quantum_id: uuid.UUID | None = None,
    ) -> tuple[QuantumSuccessCaveats, list[uuid.UUID], ButlerMetrics]:
        return super().run_quantum(
            task, quantum, task_node, limited_butler=limited_butler, quantum_id=quantum_id
        )

    def writeMetadata(
        self, quantum: Quantum, metadata: Any, task_node: TaskNode, /, limited_butler: LimitedButler
    ) -> None:
        return super().write_metadata(quantum, metadata, task_node, limited_butler=limited_butler)

    def initGlobals(self, quantum: Quantum) -> None:
        return super().init_globals(quantum)
