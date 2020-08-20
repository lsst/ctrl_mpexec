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

__all__ = ["MPGraphExecutor", "MPGraphExecutorError", "MPTimeoutError"]

# -------------------------------
#  Imports of standard modules --
# -------------------------------
import logging
import multiprocessing
import time

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


class MPTimeoutError(MPGraphExecutorError):
    """Exception raised when task execution times out.
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
    failFast : `bool`, optional
        If set to ``True`` then stop processing on first error from any task.
    executionGraphFixup : `ExecutionGraphFixup`, optional
        Instance used for modification of execution graph.
    """
    def __init__(self, numProc, timeout, quantumExecutor, *, failFast=False, executionGraphFixup=None):
        self.numProc = numProc
        self.timeout = timeout
        self.quantumExecutor = quantumExecutor
        self.failFast = failFast
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
        """Execute all Quanta in separate processes.

        Parameters
        ----------
        iterable : iterable of `~lsst.pipe.base.QuantumIterData`
            Sequence if Quanta to execute. It is guaranteed that re-requisites
            for a given Quantum will always appear before that Quantum.
        butler : `lsst.daf.butler.Butler`
            Data butler instance
        """

        disableImplicitThreading()  # To prevent thread contention

        # map quantum id to AsyncResult and QuantumIterData
        process = {}
        qdataMap = {}

        # we may need to iterate several times so make it a list
        qdataItems = list(iterable)

        # check that all tasks can run in sub-process
        for qdata in qdataItems:
            taskDef = qdata.taskDef
            if not taskDef.taskClass.canMultiprocess:
                raise MPGraphExecutorError(f"Task {taskDef.taskName} does not support multiprocessing;"
                                           " use single process")

        # quantum ids which are processing and start time for each
        runningJobs = {}
        # quantum ids which finished successfully
        finishedJobs = set()
        # quantum ids which failed
        failedJobs = set()

        while qdataItems or runningJobs:

            _LOG.debug("qdataItems: %s", len(qdataItems))
            _LOG.debug("runningJobs: %s", len(runningJobs))

            # See if any jobs have finished, updating while iterating so make
            # a copy of keys.
            for qid in list(runningJobs.keys()):
                proc = process[qid]
                if not proc.is_alive():
                    _LOG.debug("ready: %s", qid)
                    # finished
                    del runningJobs[qid]
                    if proc.exitcode == 0:
                        finishedJobs.add(qid)
                        _LOG.debug("finished: %s", qid)
                    else:
                        _LOG.debug("failed: %s", qid)
                        failed_qdata = qdataMap[qid]
                        if self.failFast:
                            raise MPGraphExecutorError(
                                f"Task {failed_qdata.taskDef} failed while processing quantum with "
                                f"dataId={failed_qdata.quantum.dataId}, exit code={proc.exitcode}"
                            )
                        else:
                            _LOG.error(
                                "Task %s failed while processing quantum with dataId=%s; "
                                "processing will continue for remaining tasks.",
                                failed_qdata.taskDef, failed_qdata.quantum.dataId)
                            failedJobs.add(qid)
                else:
                    # check for timeout
                    now = time.time()
                    if now - runningJobs[qid] > self.timeout:
                        _LOG.debug("timeout: %s", qid)
                        del runningJobs[qid]
                        failedJobs.add(qid)
                        failed_qdata = qdataMap[qid]
                        if self.failFast:
                            raise MPTimeoutError(
                                f"Timeout ({self.timeout}sec) for task {failed_qdata.taskDef} while "
                                f"processing quantum with dataId={failed_qdata.quantum.dataId}"
                            )
                        else:
                            _LOG.error(
                                "Timeout (%s sec) for task %s while processing quantum with dataId=%s; "
                                "task is killed, processing continues for remaining tasks.",
                                self.timeout, failed_qdata.taskDef, failed_qdata.quantum.dataId
                            )
                            # try to kill and then kill harder
                            _LOG.debug("Terminating process %s", proc.name)
                            proc.terminate()
                            for i in range(10):
                                time.sleep(0.1)
                                if not proc.is_alive():
                                    break
                            else:
                                _LOG.debug("Killing process %s", proc.name)
                                proc.kill()

            # see if we can start more jobs
            remainingQdataItems = []
            for qdata in qdataItems:

                # check all dependencies
                if qdata.dependencies & failedJobs:
                    # upstream job has failed, skipping this
                    failedJobs.add(qdata.index)
                    _LOG.error(
                        "Upstream job failed for task %s with dataId=%s, skipping this task.",
                        qdata.taskDef, qdata.quantum.dataId
                    )
                elif qdata.dependencies <= finishedJobs:
                    # all dependencies are completed, start new job
                    if len(runningJobs) < self.numProc:
                        _LOG.debug("Sumbitting %s: %s %s", qdata.index, qdata.taskDef, qdata.quantum.dataId)
                        kwargs = dict(taskDef=qdata.taskDef, quantum=qdata.quantum,
                                      butler=butler, executor=self.quantumExecutor)
                        process[qdata.index] = multiprocessing.Process(
                            target=self._executePipelineTask, args=(), kwargs=kwargs,
                            name=f"task-{qdata.index}"
                        )
                        process[qdata.index].start()
                        qdataMap[qdata.index] = qdata
                        runningJobs[qdata.index] = time.time()
                    else:
                        remainingQdataItems.append(qdata)
                else:
                    # need to wait longer
                    remainingQdataItems.append(qdata)
            qdataItems = remainingQdataItems

            # Here we want to wait until one of the running jobs completes
            # but multiprocessing does not provide an API for that, for now
            # just sleep a little bit and go back to the loop.
            if runningJobs:
                time.sleep(0.1)

        # if any job failed raise an exception
        if failedJobs:
            raise MPGraphExecutorError("One or more tasks failed or timed out during execution.")

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
        executor.execute(taskDef, quantum, butler)
