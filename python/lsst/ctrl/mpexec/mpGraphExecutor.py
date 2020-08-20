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
import copy
from enum import Enum
import logging
import multiprocessing
import time

# -----------------------------
#  Imports for other modules --
# -----------------------------
from .quantumGraphExecutor import QuantumGraphExecutor
from lsst.base import disableImplicitThreading

_LOG = logging.getLogger(__name__.partition(".")[2])


# Possible states for the executing task:
#  - PENDING: job has not started yet
#  - RUNNING: job is currently executing
#  - FINISHED: job finished successfully
#  - FAILED: job execution failed (process returned non-zero status)
#  - TIMED_OUT: job is killed due to too long execution time
#  - FAILED_DEP: one of the dependencies of this job has failed/timed out
JobState = Enum("JobState", "PENDING RUNNING FINISHED FAILED TIMED_OUT FAILED_DEP")


class _Job:
    """Class representing a job running single task.

    Parameters
    ----------
    qdata : `~lsst.pipe.base.QuantumIterData`
        Quantum and some associated information.
    """
    def __init__(self, qdata):
        self.qdata = qdata
        self.process = None
        self.state = JobState.PENDING
        self.started = None

    def start(self, butler, quantumExecutor):
        """Start process which runs the task.

        Parameters
        ----------
        butler : `lsst.daf.butler.Butler`
            Data butler instance.
        quantumExecutor : `QuantumExecutor`
            Executor for single quantum.
        """
        # Butler can have live database connections which is a problem with
        # fork-type activation. Make a copy of butler, this guarantees that
        # no database is open right after copy.
        butler = copy.copy(butler)
        taskDef = self.qdata.taskDef
        quantum = self.qdata.quantum
        self.process = multiprocessing.Process(
            target=quantumExecutor.execute, args=(taskDef, quantum, butler),
            name=f"task-{self.qdata.index}"
        )
        self.process.start()
        self.started = time.time()
        self.state = JobState.RUNNING

    def stop(self):
        """Stop the process.
        """
        self.process.terminate()
        # give it 1 second to finish or KILL
        for i in range(10):
            time.sleep(0.1)
            if not self.process.is_alive():
                break
        else:
            _LOG.debug("Killing process %s", self.process.name)
            self.process.kill()

    def __str__(self):
        return f"<{self.qdata.taskDef} dataId={self.qdata.quantum.dataId}>"


class _JobList:
    """SImple list of _Job instances with few convenience methods.

    Parameters
    ----------
    iterable : iterable of `~lsst.pipe.base.QuantumIterData`
        Sequence if Quanta to execute. This has to be ordered according to
        task dependencies.
    """
    def __init__(self, iterable):
        self.jobs = [_Job(qdata) for qdata in iterable]

    def pending(self):
        """Return list of jobs that wait for execution.

        Returns
        -------
        jobs : `list` [`_Job`]
            List of jobs.
        """
        return [job for job in self.jobs if job.state == JobState.PENDING]

    def running(self):
        """Return list of jobs that are executing.

        Returns
        -------
        jobs : `list` [`_Job`]
            List of jobs.
        """
        return [job for job in self.jobs if job.state == JobState.RUNNING]

    def finishedIds(self):
        """Return set of jobs IDs that finished successfully (not failed).

        Job ID is the index of the corresponding quantum.

        Returns
        -------
        jobsIds : `set` [`int`]
            Set of integer job IDs.
        """
        return set(job.qdata.index for job in self.jobs if job.state == JobState.FINISHED)

    def failedIds(self):
        """Return set of jobs IDs that failed for any reason.

        Returns
        -------
        jobsIds : `set` [`int`]
            Set of integer job IDs.
        """
        return set(job.qdata.index for job in self.jobs
                   if job.state in (JobState.FAILED, JobState.FAILED_DEP, JobState.TIMED_OUT))

    def timedOutIds(self):
        """Return set of jobs IDs that timed out.

        Returns
        -------
        jobsIds : `set` [`int`]
            Set of integer job IDs.
        """
        return set(job.qdata.index for job in self.jobs if job.state == JobState.TIMED_OUT)


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
            self.quantumExecutor.execute(qdata.taskDef, qdata.quantum, butler)

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

        # re-pack input quantum data into jobs list
        jobs = _JobList(iterable)

        # check that all tasks can run in sub-process
        for job in jobs.jobs:
            taskDef = job.qdata.taskDef
            if not taskDef.taskClass.canMultiprocess:
                raise MPGraphExecutorError(f"Task {taskDef.taskName} does not support multiprocessing;"
                                           " use single process")

        while jobs.pending() or jobs.running():

            _LOG.debug("#pendingJobs: %s", len(jobs.pending()))
            _LOG.debug("#runningJobs: %s", len(jobs.running()))

            # See if any jobs have finished
            for job in jobs.running():
                proc = job.process
                if not proc.is_alive():
                    _LOG.debug("finished: %s", job)
                    # finished
                    if proc.exitcode == 0:
                        job.state = JobState.FINISHED
                        _LOG.debug("success: %s", job)
                    else:
                        job.state = JobState.FAILED
                        _LOG.debug("failed: %s", job)
                        if self.failFast:
                            raise MPGraphExecutorError(
                                f"Task {job} failed, exit code={proc.exitcode}."
                            )
                        else:
                            _LOG.error(
                                "Task %s failed; processing will continue for remaining tasks.", job
                            )
                else:
                    # check for timeout
                    now = time.time()
                    if now - job.started > self.timeout:
                        job.state = JobState.TIMED_OUT
                        _LOG.debug("Terminating job %s due to timeout", job)
                        job.stop()
                        if self.failFast:
                            raise MPTimeoutError(f"Timeout ({self.timeout} sec) for task {job}.")
                        else:
                            _LOG.error(
                                "Timeout (%s sec) for task %s; task is killed, processing continues "
                                "for remaining tasks.", self.timeout, job
                            )

            # see if we can start more jobs
            for job in jobs.pending():

                # check all dependencies
                if job.qdata.dependencies & jobs.failedIds():
                    # upstream job has failed, skipping this
                    job.state = JobState.FAILED_DEP
                    _LOG.error("Upstream job failed for task %s, skipping this task.", job)
                elif job.qdata.dependencies <= jobs.finishedIds():
                    # all dependencies have completed, can start new job
                    if len(jobs.running()) < self.numProc:
                        _LOG.debug("Sumbitting %s", job)
                        job.start(butler, self.quantumExecutor)

            # Here we want to wait until one of the running jobs completes
            # but multiprocessing does not provide an API for that, for now
            # just sleep a little bit and go back to the loop.
            if jobs.running():
                time.sleep(0.1)

        if jobs.failedIds():
            # print list of failed jobs
            _LOG.error("Failed jobs:")
            for job in jobs.jobs:
                if job.state != JobState.FINISHED:
                    _LOG.error("  - %s: %s", job.state, job)

            # if any job failed raise an exception
            if jobs.failedIds() == jobs.timedOutIds():
                raise MPTimeoutError("One or more tasks timed out during execution.")
            else:
                raise MPGraphExecutorError("One or more tasks failed or timed out during execution.")
