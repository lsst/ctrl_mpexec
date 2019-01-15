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
from .singleQuantumExecutor import SingleQuantumExecutor
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
    """
    def __init__(self, numProc, timeout):
        self.numProc = numProc
        self.timeout = timeout

    def executeQuanta(self, iterable, butler, taskFactory):
        # Docstring inherited from QuantumGraphExecutor.executeQuanta
        if self.numProc > 1:
            self._executeQuantaMP(iterable, butler, taskFactory)
        else:
            self._executeQuantaInProcess(iterable, butler, taskFactory)

    def _executeQuantaInProcess(self, iterable, butler, taskFactory):
        """Execute all Quanta in current process.

        Parameters
        ----------
        iterable : iterable of `~lsst.pipe.base.QuantumIterData`
            Sequence if Quanta to execute. It is guaranteed that re-requisites
            for a given Quantum will always appear before that Quantum.
        butler : `lsst.daf.butler.Butler`
            Data butler instance
        taskFactory : `~lsst.pipe.base.TaskFactory`
            Task factory.
        """
        for qdata in iterable:
            _LOG.debug("Executing %s", qdata)
            taskDef = qdata.taskDef
            self._executePipelineTask(taskDef.taskClass, taskDef.config, qdata.quantum, butler, taskFactory)

    def _executeQuantaMP(self, iterable, butler, taskFactory):
        """Execute all Quanta in separate process pool.

        Parameters
        ----------
        iterable : iterable of `~lsst.pipe.base.QuantumIterData`
            Sequence if Quanta to execute. It is guaranteed that re-requisites
            for a given Quantum will always appear before that Quantum.
        butler : `lsst.daf.butler.Butler`
            Data butler instance
        taskFactory : `~lsst.pipe.base.TaskFactory`
            Task factory.
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

            # Wait for all pre-reqs
            for preReq in qdata.prerequisites:
                # Wait for max. timeout for this result to be ready.
                # This can raise on timeout or if remote call raises.
                _LOG.debug("Check pre-req %s for %s", preReq, qdata)
                results[preReq].get(self.timeout)
                _LOG.debug("Result %s is ready", preReq)

            # Add it to the pool and remember its result
            _LOG.debug("Sumbitting %s", qdata)
            args = (taskDef.taskClass, taskDef.config, qdata.quantum, butler, taskFactory)
            results[qdata.quantumId] = pool.apply_async(self._executePipelineTask, args)

        # Everything is submitted, wait until it's complete
        _LOG.debug("Wait for all tasks")
        for qid, res in results.items():
            if res.ready():
                _LOG.debug("Result %d is ready", qid)
            else:
                _LOG.debug("Waiting for result %d", qid)
                res.get(self.timeout)

    @staticmethod
    def _executePipelineTask(taskClass, config, quantum, butler, taskFactory):
        """Execute super-task on a single data item.

        Parameters
        ----------
        taskClass : `type`
            Sub-class of PipelineTask.
        config : `~lsst.pipe.base.PipelineTaskConfig`
            Task configuration.
        quantum : `~lsst.daf.butler.Quantum`
            Quantum for this execution.
        butler : `~lsst.daf.butler.Butler`
            Data butler instance.
        taskFactory : `~lsst.pipe.base.TaskFactory`
            Task factory.
        """
        executor = SingleQuantumExecutor(butler, taskFactory)
        return executor.execute(taskClass, config, quantum)
