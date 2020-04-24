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

__all__ = ['MPGraphExecutor']

# -------------------------------
#  Imports of standard modules --
# -------------------------------
import logging
import multiprocessing

# -----------------------------
#  Imports for other modules --
# -----------------------------
from .quantumGraphExecutor import QuantumGraphExecutor
from lsst.base import disableImplicitThreading

_LOG = logging.getLogger(__name__.partition(".")[2])


class MPGraphExecutorError(Exception):
    """Exception class for errors raised by MPGraphExecutor.
    """
    pass


class MPGraphExecutor(QuantumGraphExecutor):
    """Implementation of QuantumGraphExecutor using same-host multiprocess
    execution of Quanta.

    Parameters
    ----------
    numProc : `int`
        Number of processes to use for executing tasks.
    timeout : `float`
        Time in seconds to wait for tasks to finish.
    quantumExecutor : `QuantumExecutor`
        Executor for single quantum. For multiprocess-style execution when
        ``numProc`` is greater than one this instance must support pickle.
    executionGraphFixup : `ExecutionGraphFixup`, optional
        Instance used for modification of execution graph.
    """
    def __init__(self, numProc, timeout, quantumExecutor, *, executionGraphFixup=None):
        self.numProc = numProc
        self.timeout = timeout
        self.quantumExecutor = quantumExecutor
        self.executionGraphFixup = executionGraphFixup

    def execute(self, graph, butler):
        # Docstring inherited from QuantumGraphExecutor.execute
        quantaIter = self._fixupQuanta(graph.traverse())
        if self.numProc > 1:
            self._executeQuantaMP(quantaIter, butler)
        else:
            self._executeQuantaInProcess(quantaIter, butler)

    def _fixupQuanta(self, quantaIter):
        """Call fixup code to modify execution graph.

        Parameters
        ----------
        quantaIter : iterable of `~lsst.pipe.base.QuantumIterData`
            Quanta as originated from a quantum graph.

        Returns
        -------
        quantaIter : iterable of `~lsst.pipe.base.QuantumIterData`
            Possibly updated set of quanta, properly ordered for execution.

        Raises
        ------
        MPGraphExecutorError
            Raised if execution graph cannot be ordered after modification,
            i.e. it has dependency cycles.
        """
        if not self.executionGraphFixup:
            return quantaIter

        _LOG.debug("Call execution graph fixup method")
        quantaIter = self.executionGraphFixup.fixupQuanta(quantaIter)

        # need it correctly ordered as dependencies may have changed
        # after modification, so do topo-sort
        updatedQuanta = list(quantaIter)
        quanta = []
        ids = set()
        _LOG.debug("Re-ordering execution graph")
        while updatedQuanta:
            # find quantum that has all dependencies resolved already
            for i, qdata in enumerate(updatedQuanta):
                if ids.issuperset(qdata.dependencies):
                    _LOG.debug("Found next quanta to execute: %s", qdata)
                    del updatedQuanta[i]
                    ids.add(qdata.index)
                    # we could yield here but I want to detect cycles before
                    # returning anything from this method
                    quanta.append(qdata)
                    break
            else:
                # means remaining quanta have dependency cycle
                raise MPGraphExecutorError(
                    "Updated execution graph has dependency clycle.")

        return quanta

    def _executeQuantaInProcess(self, iterable, butler):
        """Execute all Quanta in current process.

        Parameters
        ----------
        iterable : iterable of `~lsst.pipe.base.QuantumIterData`
            Sequence if Quanta to execute. It is guaranteed that re-requisites
            for a given Quantum will always appear before that Quantum.
        butler : `lsst.daf.butler.Butler`
            Data butler instance
        """
        for qdata in iterable:
            _LOG.debug("Executing %s", qdata)
            self._executePipelineTask(taskDef=qdata.taskDef, quantum=qdata.quantum,
                                      butler=butler, executor=self.quantumExecutor)

    def _executeQuantaMP(self, iterable, butler):
        """Execute all Quanta in separate process pool.

        Parameters
        ----------
        iterable : iterable of `~lsst.pipe.base.QuantumIterData`
            Sequence if Quanta to execute. It is guaranteed that re-requisites
            for a given Quantum will always appear before that Quantum.
        butler : `lsst.daf.butler.Butler`
            Data butler instance
        """

        disableImplicitThreading()  # To prevent thread contention

        pool = multiprocessing.Pool(processes=self.numProc, maxtasksperchild=1)

        # map quantum id to AsyncResult
        results = {}

        # Add each Quantum to a pool, wait until it pre-requisites completed.
        # TODO: This is not super-efficient as it stops at the first Quantum
        # that cannot be executed (yet) and does not check other Quanta.
        for qdata in iterable:

            # check that task can run in sub-process
            taskDef = qdata.taskDef
            if not taskDef.taskClass.canMultiprocess:
                raise MPGraphExecutorError(f"Task {taskDef.taskName} does not support multiprocessing;"
                                           " use single process")

            # Wait for all dependencies
            for dep in qdata.dependencies:
                # Wait for max. timeout for this result to be ready.
                # This can raise on timeout or if remote call raises.
                _LOG.debug("Check dependency %s for %s", dep, qdata)
                results[dep].get(self.timeout)
                _LOG.debug("Result %s is ready", dep)

            # Add it to the pool and remember its result
            _LOG.debug("Sumbitting %s", qdata)
            kwargs = dict(taskDef=taskDef, quantum=qdata.quantum,
                          butler=butler, executor=self.quantumExecutor)
            results[qdata.index] = pool.apply_async(self._executePipelineTask, (), kwargs)

        # Everything is submitted, wait until it's complete
        _LOG.debug("Wait for all tasks")
        for qid, res in results.items():
            if res.ready():
                _LOG.debug("Result %d is ready", qid)
            else:
                _LOG.debug("Waiting for result %d", qid)
                res.get(self.timeout)

    @staticmethod
    def _executePipelineTask(*, taskDef, quantum, butler, executor):
        """Execute PipelineTask on a single data item.

        Parameters
        ----------
        taskDef : `~lsst.pipe.base.TaskDef`
            Task definition structure.
        quantum : `~lsst.daf.butler.Quantum`
            Quantum for this execution.
        butler : `~lsst.daf.butler.Butler`
            Data butler instance.
        executor : `QuantumExecutor`
            Executor for single quantum.
        """
        return executor.execute(taskDef, quantum, butler)
