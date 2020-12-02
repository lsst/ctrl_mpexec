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
from enum import Enum
import logging
import multiprocessing
import pickle
import sys
import time

from lsst.pipe.base.graph.graph import QuantumGraph

# -----------------------------
#  Imports for other modules --
# -----------------------------
from .quantumGraphExecutor import QuantumGraphExecutor
from lsst.base import disableImplicitThreading
from lsst.daf.butler.cli.cliLog import CliLog

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
    qnode: `~lsst.pipe.base.QuantumNode`
        Quantum and some associated information.
    """
    def __init__(self, qnode):
        self.qnode = qnode
        self.process = None
        self.state = JobState.PENDING
        self.started = None

    def start(self, butler, quantumExecutor, startMethod=None):
        """Start process which runs the task.

        Parameters
        ----------
        butler : `lsst.daf.butler.Butler`
            Data butler instance.
        quantumExecutor : `QuantumExecutor`
            Executor for single quantum.
        startMethod : `str`, optional
            Start method from `multiprocessing` module.
        """
        # Butler can have live database connections which is a problem with
        # fork-type activation. Make a pickle of butler to pass that across
        # fork. Unpickling of quantum has to happen after butler, this is why
        # it is pickled manually here.
        butler_pickle = pickle.dumps(butler)
        quantum_pickle = pickle.dumps(self.qnode.quantum)
        taskDef = self.qnode.taskDef
        logConfigState = CliLog.configState
        mp_ctx = multiprocessing.get_context(startMethod)
        self.process = mp_ctx.Process(
            target=_Job._executeJob,
            args=(quantumExecutor, taskDef, quantum_pickle, butler_pickle, logConfigState),
            name=f"task-{self.qnode.nodeId.number}"
        )
        self.process.start()
        self.started = time.time()
        self.state = JobState.RUNNING

    @staticmethod
    def _executeJob(quantumExecutor, taskDef, quantum_pickle, butler_pickle, logConfigState):
        """Execute a job with arguments.

        Parameters
        ----------
        quantumExecutor : `QuantumExecutor`
            Executor for single quantum.
        taskDef : `bytes`
            Task definition structure.
        quantum_pickle : `bytes`
            Quantum for this task execution in pickled form.
        butler_pickle : `bytes`
            Data butler instance in pickled form.
        """
        if logConfigState and not CliLog.configState:
            # means that we are in a new spawned Python process and we have to
            # re-initialize logging
            CliLog.replayConfigState(logConfigState)

        butler = pickle.loads(butler_pickle)
        quantum = pickle.loads(quantum_pickle)
        quantumExecutor.execute(taskDef, quantum, butler)

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

    def cleanup(self):
        """Release processes resources, has to be called for each finished
        process.
        """
        if self.process and not self.process.is_alive():
            self.process.close()
            self.process = None

    def __str__(self):
        return f"<{self.qnode.taskDef} dataId={self.qnode.quantum.dataId}>"


class _JobList:
    """Simple list of _Job instances with few convenience methods.

    Parameters
    ----------
    iterable : iterable of `~lsst.pipe.base.QuantumIterData`
        Sequence if Quanta to execute. This has to be ordered according to
        task dependencies.
    """
    def __init__(self, iterable):
        self.jobs = [_Job(qnode) for qnode in iterable]

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

    def finishedNodes(self):
        """Return set of QuantumNodes that finished successfully (not failed).

        Returns
        -------
        QuantumNodes : `set` [`~lsst.pipe.base.QuantumNode`]
            Set of QuantumNodes that have successfully finished
        """
        return set(job.qnode for job in self.jobs if job.state == JobState.FINISHED)

    def failedNodes(self):
        """Return set of jobs IDs that failed for any reason.

        Returns
        -------
        QuantumNodes : `set` [`~lsst.pipe.base.QuantumNode`]
            Set of QUantumNodes that failed during processing
        """
        return set(job.qnode for job in self.jobs
                   if job.state in (JobState.FAILED, JobState.FAILED_DEP, JobState.TIMED_OUT))

    def timedOutIds(self):
        """Return set of jobs IDs that timed out.

        Returns
        -------
        jobsIds : `set` [`int`]
            Set of integer job IDs.
        """
        return set(job.qnode for job in self.jobs if job.state == JobState.TIMED_OUT)

    def cleanup(self):
        """Do periodic cleanup for jobs that did not finish correctly.

        If timed out jobs are killed but take too long to stop then regular
        cleanup will not work for them. Here we check all timed out jobs
        periodically and do cleanup if they managed to die by this time.
        """
        for job in self.jobs:
            if job.state == JobState.TIMED_OUT and job.process is not None:
                job.cleanup()


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
    startMethod : `str`, optional
        Start method from `multiprocessing` module, `None` selects the best
        one for current platform.
    failFast : `bool`, optional
        If set to ``True`` then stop processing on first error from any task.
    executionGraphFixup : `ExecutionGraphFixup`, optional
        Instance used for modification of execution graph.
    """
    def __init__(self, numProc, timeout, quantumExecutor, *,
                 startMethod=None, failFast=False, executionGraphFixup=None):
        self.numProc = numProc
        self.timeout = timeout
        self.quantumExecutor = quantumExecutor
        self.failFast = failFast
        self.executionGraphFixup = executionGraphFixup

        # We set default start method as spawn for MacOS and fork for Linux;
        # None for all other platforms to use multiprocessing default.
        if startMethod is None:
            methods = dict(linux="fork", darwin="spawn")
            startMethod = methods.get(sys.platform)
        self.startMethod = startMethod

    def execute(self, graph, butler):
        # Docstring inherited from QuantumGraphExecutor.execute
        graph = self._fixupQuanta(graph)
        if self.numProc > 1:
            self._executeQuantaMP(graph, butler)
        else:
            self._executeQuantaInProcess(graph, butler)

    def _fixupQuanta(self, graph: QuantumGraph):
        """Call fixup code to modify execution graph.

        Parameters
        ----------
        graph : `QuantumGraph`
            `QuantumGraph` to modify

        Returns
        -------
        graph : `QuantumGraph`
            Modified `QuantumGraph`.

        Raises
        ------
        MPGraphExecutorError
            Raised if execution graph cannot be ordered after modification,
            i.e. it has dependency cycles.
        """
        if not self.executionGraphFixup:
            return graph

        _LOG.debug("Call execution graph fixup method")
        graph = self.executionGraphFixup.fixupQuanta(graph)

        # Detect if there is now a cycle created within the graph
        if graph.findCycle():
            raise MPGraphExecutorError(
                "Updated execution graph has dependency cycle.")

        return graph

    def _executeQuantaInProcess(self, graph, butler):
        """Execute all Quanta in current process.

        Parameters
        ----------
        graph : `QuantumGraph`
            `QuantumGraph` that is to be executed
        butler : `lsst.daf.butler.Butler`
            Data butler instance
        """
        # Note that in non-MP case any failed task will generate an exception
        # and kill the whole thing. In general we cannot guarantee exception
        # safety so easiest and safest thing is to let it die.
        count, totalCount = 0, len(graph)
        for qnode in graph:
            _LOG.debug("Executing %s", qnode)
            self.quantumExecutor.execute(qnode.taskDef, qnode.quantum, butler)
            count += 1
            _LOG.info("Executed %d quanta, %d remain out of total %d quanta.",
                      count, totalCount - count, totalCount)

    def _executeQuantaMP(self, graph, butler):
        """Execute all Quanta in separate processes.

        Parameters
        ----------
        graph : `QuantumGraph`
            `QuantumGraph` that is to be executed.
        butler : `lsst.daf.butler.Butler`
            Data butler instance
        """

        disableImplicitThreading()  # To prevent thread contention

        _LOG.debug("Using %r for multiprocessing start method", self.startMethod)

        # re-pack input quantum data into jobs list
        jobs = _JobList(graph)

        # check that all tasks can run in sub-process
        for job in jobs.jobs:
            taskDef = job.qnode.taskDef
            if not taskDef.taskClass.canMultiprocess:
                raise MPGraphExecutorError(f"Task {taskDef.taskName} does not support multiprocessing;"
                                           " use single process")

        finished, failed = 0, 0
        while jobs.pending() or jobs.running():

            _LOG.debug("#pendingJobs: %s", len(jobs.pending()))
            _LOG.debug("#runningJobs: %s", len(jobs.running()))

            # See if any jobs have finished
            for job in jobs.running():
                if not job.process.is_alive():
                    _LOG.debug("finished: %s", job)
                    # finished
                    exitcode = job.process.exitcode
                    if exitcode == 0:
                        job.state = JobState.FINISHED
                        job.cleanup()
                        _LOG.debug("success: %s took %.3f seconds", job, time.time() - job.started)
                    else:
                        job.state = JobState.FAILED
                        job.cleanup()
                        _LOG.debug("failed: %s", job)
                        if self.failFast:
                            for stopJob in jobs.running():
                                if stopJob is not job:
                                    stopJob.stop()
                            raise MPGraphExecutorError(
                                f"Task {job} failed, exit code={exitcode}."
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
                        job.cleanup()
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
                if graph.determineInputsToQuantumNode(job.qnode) & jobs.failedNodes():
                    # upstream job has failed, skipping this
                    job.state = JobState.FAILED_DEP
                    _LOG.error("Upstream job failed for task %s, skipping this task.", job)
                elif graph.determineInputsToQuantumNode(job.qnode) <= jobs.finishedNodes():
                    # all dependencies have completed, can start new job
                    if len(jobs.running()) < self.numProc:
                        _LOG.debug("Sumbitting %s", job)
                        job.start(butler, self.quantumExecutor, self.startMethod)

            # Do cleanup for timed out jobs if necessary.
            jobs.cleanup()

            # Print progress message if something changed.
            newFinished, newFailed = len(jobs.finishedNodes()), len(jobs.failedNodes())
            if (finished, failed) != (newFinished, newFailed):
                finished, failed = newFinished, newFailed
                totalCount = len(jobs.jobs)
                _LOG.info("Executed %d quanta successfully, %d failed and %d remain out of total %d quanta.",
                          finished, failed, totalCount - finished - failed, totalCount)

            # Here we want to wait until one of the running jobs completes
            # but multiprocessing does not provide an API for that, for now
            # just sleep a little bit and go back to the loop.
            if jobs.running():
                time.sleep(0.1)

        if jobs.failedNodes():
            # print list of failed jobs
            _LOG.error("Failed jobs:")
            for job in jobs.jobs:
                if job.state != JobState.FINISHED:
                    _LOG.error("  - %s: %s", job.state, job)

            # if any job failed raise an exception
            if jobs.failedNodes() == jobs.timedOutIds():
                raise MPTimeoutError("One or more tasks timed out during execution.")
            else:
                raise MPGraphExecutorError("One or more tasks failed or timed out during execution.")
