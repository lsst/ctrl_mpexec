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

import gc
import importlib
import logging
import multiprocessing
import pickle
import signal
import sys
import time
from enum import Enum
from typing import Optional

from lsst.daf.butler.cli.cliLog import CliLog
from lsst.pipe.base import InvalidQuantumError
from lsst.pipe.base.graph.graph import QuantumGraph
from lsst.utils.threads import disable_implicit_threading

# -----------------------------
#  Imports for other modules --
# -----------------------------
from .quantumGraphExecutor import QuantumGraphExecutor
from .reports import ExecutionStatus, QuantumReport, Report

_LOG = logging.getLogger(__name__)


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
        self._state = JobState.PENDING
        self.started = None
        self._rcv_conn = None
        self._terminated = False

    @property
    def state(self):
        """Job processing state (JobState)"""
        return self._state

    @property
    def terminated(self):
        """Return True if job was killed by stop() method and negative exit
        code is returned from child process. (`bool`)"""
        return self._terminated and self.process.exitcode < 0

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
        # Unpickling of quantum has to happen after butler, this is why
        # it is pickled manually here.
        quantum_pickle = pickle.dumps(self.qnode.quantum)
        taskDef = self.qnode.taskDef
        self._rcv_conn, snd_conn = multiprocessing.Pipe(False)
        logConfigState = CliLog.configState
        mp_ctx = multiprocessing.get_context(startMethod)
        self.process = mp_ctx.Process(
            target=_Job._executeJob,
            args=(quantumExecutor, taskDef, quantum_pickle, butler, logConfigState, snd_conn),
            name=f"task-{self.qnode.quantum.dataId}",
        )
        self.process.start()
        self.started = time.time()
        self._state = JobState.RUNNING

    @staticmethod
    def _executeJob(quantumExecutor, taskDef, quantum_pickle, butler, logConfigState, snd_conn):
        """Execute a job with arguments.

        Parameters
        ----------
        quantumExecutor : `QuantumExecutor`
            Executor for single quantum.
        taskDef : `bytes`
            Task definition structure.
        quantum_pickle : `bytes`
            Quantum for this task execution in pickled form.
        butler : `lss.daf.butler.Butler`
            Data butler instance.
        snd_conn : `multiprocessing.Connection`
            Connection to send job report to parent process.
        """
        if logConfigState and not CliLog.configState:
            # means that we are in a new spawned Python process and we have to
            # re-initialize logging
            CliLog.replayConfigState(logConfigState)

        # have to reset connection pool to avoid sharing connections
        if butler is not None:
            butler.registry.resetConnectionPool()

        quantum = pickle.loads(quantum_pickle)
        try:
            quantumExecutor.execute(taskDef, quantum, butler)
        finally:
            # If sending fails we do not want this new exception to be exposed.
            try:
                report = quantumExecutor.getReport()
                snd_conn.send(report)
            except Exception:
                pass

    def stop(self):
        """Stop the process."""
        self.process.terminate()
        # give it 1 second to finish or KILL
        for i in range(10):
            time.sleep(0.1)
            if not self.process.is_alive():
                break
        else:
            _LOG.debug("Killing process %s", self.process.name)
            self.process.kill()
        self._terminated = True

    def cleanup(self):
        """Release processes resources, has to be called for each finished
        process.
        """
        if self.process and not self.process.is_alive():
            self.process.close()
            self.process = None
            self._rcv_conn = None

    def report(self) -> QuantumReport:
        """Return task report, should be called after process finishes and
        before cleanup().
        """
        try:
            report = self._rcv_conn.recv()
            report.exitCode = self.process.exitcode
        except Exception:
            # Likely due to the process killed, but there may be other reasons.
            report = QuantumReport.from_exit_code(
                exitCode=self.process.exitcode,
                dataId=self.qnode.quantum.dataId,
                taskLabel=self.qnode.taskDef.label,
            )
        if self.terminated:
            # Means it was killed, assume it's due to timeout
            report.status = ExecutionStatus.TIMEOUT
        return report

    def failMessage(self):
        """Return a message describing task failure"""
        exitcode = self.process.exitcode
        if exitcode < 0:
            # Negative exit code means it is killed by signal
            signum = -exitcode
            msg = f"Task {self} failed, killed by signal {signum}"
            # Just in case this is some very odd signal, expect ValueError
            try:
                strsignal = signal.strsignal(signum)
                msg = f"{msg} ({strsignal})"
            except ValueError:
                pass
        elif exitcode > 0:
            msg = f"Task {self} failed, exit code={exitcode}"
        else:
            msg = None
        return msg

    def __str__(self):
        return f"<{self.qnode.taskDef} dataId={self.qnode.quantum.dataId}>"


class _JobList:
    """Simple list of _Job instances with few convenience methods.

    Parameters
    ----------
    iterable : iterable of `~lsst.pipe.base.QuantumNode`
        Sequence of Quanta to execute. This has to be ordered according to
        task dependencies.
    """

    def __init__(self, iterable):
        self.jobs = [_Job(qnode) for qnode in iterable]
        self.pending = self.jobs[:]
        self.running = []
        self.finishedNodes = set()
        self.failedNodes = set()
        self.timedOutNodes = set()

    def submit(self, job, butler, quantumExecutor, startMethod=None):
        """Submit one more job for execution

        Parameters
        ----------
        job : `_Job`
            Job to submit.
        butler : `lsst.daf.butler.Butler`
            Data butler instance.
        quantumExecutor : `QuantumExecutor`
            Executor for single quantum.
        startMethod : `str`, optional
            Start method from `multiprocessing` module.
        """
        # this will raise if job is not in pending list
        self.pending.remove(job)
        job.start(butler, quantumExecutor, startMethod)
        self.running.append(job)

    def setJobState(self, job, state):
        """Update job state.

        Parameters
        ----------
        job : `_Job`
            Job to submit.
        state : `JobState`
            New job state, note that only FINISHED, FAILED, TIMED_OUT, or
            FAILED_DEP state is acceptable.
        """
        allowedStates = (JobState.FINISHED, JobState.FAILED, JobState.TIMED_OUT, JobState.FAILED_DEP)
        assert state in allowedStates, f"State {state} not allowed here"

        # remove job from pending/running lists
        if job.state == JobState.PENDING:
            self.pending.remove(job)
        elif job.state == JobState.RUNNING:
            self.running.remove(job)

        qnode = job.qnode
        # it should not be in any of these, but just in case
        self.finishedNodes.discard(qnode)
        self.failedNodes.discard(qnode)
        self.timedOutNodes.discard(qnode)

        job._state = state
        if state == JobState.FINISHED:
            self.finishedNodes.add(qnode)
        elif state == JobState.FAILED:
            self.failedNodes.add(qnode)
        elif state == JobState.FAILED_DEP:
            self.failedNodes.add(qnode)
        elif state == JobState.TIMED_OUT:
            self.failedNodes.add(qnode)
            self.timedOutNodes.add(qnode)
        else:
            raise ValueError(f"Unexpected state value: {state}")

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
    """Exception class for errors raised by MPGraphExecutor."""

    pass


class MPTimeoutError(MPGraphExecutorError):
    """Exception raised when task execution times out."""

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
    pdb : `str`, optional
        Debugger to import and use (via the ``post_mortem`` function) in the
        event of an exception.
    executionGraphFixup : `ExecutionGraphFixup`, optional
        Instance used for modification of execution graph.
    """

    def __init__(
        self,
        numProc,
        timeout,
        quantumExecutor,
        *,
        startMethod=None,
        failFast=False,
        pdb=None,
        executionGraphFixup=None,
    ):
        self.numProc = numProc
        self.timeout = timeout
        self.quantumExecutor = quantumExecutor
        self.failFast = failFast
        self.pdb = pdb
        self.executionGraphFixup = executionGraphFixup
        self.report: Optional[Report] = None

        # We set default start method as spawn for MacOS and fork for Linux;
        # None for all other platforms to use multiprocessing default.
        if startMethod is None:
            methods = dict(linux="fork", darwin="spawn")
            startMethod = methods.get(sys.platform)
        self.startMethod = startMethod

    def execute(self, graph, butler):
        # Docstring inherited from QuantumGraphExecutor.execute
        graph = self._fixupQuanta(graph)
        self.report = Report()
        try:
            if self.numProc > 1:
                self._executeQuantaMP(graph, butler)
            else:
                self._executeQuantaInProcess(graph, butler)
        except Exception as exc:
            self.report.set_exception(exc)
            raise

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
            raise MPGraphExecutorError("Updated execution graph has dependency cycle.")

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
        successCount, totalCount = 0, len(graph)
        failedNodes = set()
        for qnode in graph:

            # Any failed inputs mean that the quantum has to be skipped.
            inputNodes = graph.determineInputsToQuantumNode(qnode)
            if inputNodes & failedNodes:
                _LOG.error(
                    "Upstream job failed for task <%s dataId=%s>, skipping this task.",
                    qnode.taskDef,
                    qnode.quantum.dataId,
                )
                failedNodes.add(qnode)
                quantum_report = QuantumReport(
                    status=ExecutionStatus.SKIPPED, dataId=qnode.quantum.dataId, taskLabel=qnode.taskDef.label
                )
                self.report.quantaReports.append(quantum_report)
                continue

            _LOG.debug("Executing %s", qnode)
            try:
                self.quantumExecutor.execute(qnode.taskDef, qnode.quantum, butler)
                successCount += 1
            except Exception as exc:
                if self.pdb and sys.stdin.isatty() and sys.stdout.isatty():
                    _LOG.error(
                        "Task <%s dataId=%s> failed; dropping into pdb.",
                        qnode.taskDef,
                        qnode.quantum.dataId,
                        exc_info=exc,
                    )
                    try:
                        pdb = importlib.import_module(self.pdb)
                    except ImportError as imp_exc:
                        raise MPGraphExecutorError(
                            f"Unable to import specified debugger module ({self.pdb}): {imp_exc}"
                        ) from exc
                    if not hasattr(pdb, "post_mortem"):
                        raise MPGraphExecutorError(
                            f"Specified debugger module ({self.pdb}) can't debug with post_mortem",
                        ) from exc
                    pdb.post_mortem(exc.__traceback__)
                failedNodes.add(qnode)
                self.report.status = ExecutionStatus.FAILURE
                if self.failFast:
                    raise MPGraphExecutorError(
                        f"Task <{qnode.taskDef} dataId={qnode.quantum.dataId}> failed."
                    ) from exc
                else:
                    # Note that there could be exception safety issues, which
                    # we presently ignore.
                    _LOG.error(
                        "Task <%s dataId=%s> failed; processing will continue for remaining tasks.",
                        qnode.taskDef,
                        qnode.quantum.dataId,
                        exc_info=exc,
                    )
            finally:
                # sqlalchemy has some objects that can last until a garbage
                # collection cycle is run, which can happen at unpredictable
                # times, run a collection loop here explicitly.
                gc.collect()

                quantum_report = self.quantumExecutor.getReport()
                if quantum_report:
                    self.report.quantaReports.append(quantum_report)

            _LOG.info(
                "Executed %d quanta successfully, %d failed and %d remain out of total %d quanta.",
                successCount,
                len(failedNodes),
                totalCount - successCount - len(failedNodes),
                totalCount,
            )

        # Raise an exception if there were any failures.
        if failedNodes:
            raise MPGraphExecutorError("One or more tasks failed during execution.")

    def _executeQuantaMP(self, graph, butler):
        """Execute all Quanta in separate processes.

        Parameters
        ----------
        graph : `QuantumGraph`
            `QuantumGraph` that is to be executed.
        butler : `lsst.daf.butler.Butler`
            Data butler instance
        """

        disable_implicit_threading()  # To prevent thread contention

        _LOG.debug("Using %r for multiprocessing start method", self.startMethod)

        # re-pack input quantum data into jobs list
        jobs = _JobList(graph)

        # check that all tasks can run in sub-process
        for job in jobs.jobs:
            taskDef = job.qnode.taskDef
            if not taskDef.taskClass.canMultiprocess:
                raise MPGraphExecutorError(
                    f"Task {taskDef.taskName} does not support multiprocessing; use single process"
                )

        finishedCount, failedCount = 0, 0
        while jobs.pending or jobs.running:

            _LOG.debug("#pendingJobs: %s", len(jobs.pending))
            _LOG.debug("#runningJobs: %s", len(jobs.running))

            # See if any jobs have finished
            for job in jobs.running:
                if not job.process.is_alive():
                    _LOG.debug("finished: %s", job)
                    # finished
                    exitcode = job.process.exitcode
                    quantum_report = job.report()
                    self.report.quantaReports.append(quantum_report)
                    if exitcode == 0:
                        jobs.setJobState(job, JobState.FINISHED)
                        job.cleanup()
                        _LOG.debug("success: %s took %.3f seconds", job, time.time() - job.started)
                    else:
                        if job.terminated:
                            # Was killed due to timeout.
                            if self.report.status == ExecutionStatus.SUCCESS:
                                # Do not override global FAILURE status
                                self.report.status = ExecutionStatus.TIMEOUT
                            message = f"Timeout ({self.timeout} sec) for task {job}, task is killed"
                            jobs.setJobState(job, JobState.TIMED_OUT)
                        else:
                            self.report.status = ExecutionStatus.FAILURE
                            # failMessage() has to be called before cleanup()
                            message = job.failMessage()
                            jobs.setJobState(job, JobState.FAILED)

                        job.cleanup()
                        _LOG.debug("failed: %s", job)
                        if self.failFast or exitcode == InvalidQuantumError.EXIT_CODE:
                            # stop all running jobs
                            for stopJob in jobs.running:
                                if stopJob is not job:
                                    stopJob.stop()
                            if job.state is JobState.TIMED_OUT:
                                raise MPTimeoutError(f"Timeout ({self.timeout} sec) for task {job}.")
                            else:
                                raise MPGraphExecutorError(message)
                        else:
                            _LOG.error("%s; processing will continue for remaining tasks.", message)
                else:
                    # check for timeout
                    now = time.time()
                    if now - job.started > self.timeout:
                        # Try to kill it, and there is a chance that it
                        # finishes successfully before it gets killed. Exit
                        # status is handled by the code above on next
                        # iteration.
                        _LOG.debug("Terminating job %s due to timeout", job)
                        job.stop()

            # Fail jobs whose inputs failed, this may need several iterations
            # if the order is not right, will be done in the next loop.
            if jobs.failedNodes:
                for job in jobs.pending:
                    jobInputNodes = graph.determineInputsToQuantumNode(job.qnode)
                    if jobInputNodes & jobs.failedNodes:
                        quantum_report = QuantumReport(
                            status=ExecutionStatus.SKIPPED,
                            dataId=job.qnode.quantum.dataId,
                            taskLabel=job.qnode.taskDef.label,
                        )
                        self.report.quantaReports.append(quantum_report)
                        jobs.setJobState(job, JobState.FAILED_DEP)
                        _LOG.error("Upstream job failed for task %s, skipping this task.", job)

            # see if we can start more jobs
            if len(jobs.running) < self.numProc:
                for job in jobs.pending:
                    jobInputNodes = graph.determineInputsToQuantumNode(job.qnode)
                    if jobInputNodes <= jobs.finishedNodes:
                        # all dependencies have completed, can start new job
                        if len(jobs.running) < self.numProc:
                            _LOG.debug("Submitting %s", job)
                            jobs.submit(job, butler, self.quantumExecutor, self.startMethod)
                        if len(jobs.running) >= self.numProc:
                            # Cannot start any more jobs, wait until something
                            # finishes.
                            break

            # Do cleanup for timed out jobs if necessary.
            jobs.cleanup()

            # Print progress message if something changed.
            newFinished, newFailed = len(jobs.finishedNodes), len(jobs.failedNodes)
            if (finishedCount, failedCount) != (newFinished, newFailed):
                finishedCount, failedCount = newFinished, newFailed
                totalCount = len(jobs.jobs)
                _LOG.info(
                    "Executed %d quanta successfully, %d failed and %d remain out of total %d quanta.",
                    finishedCount,
                    failedCount,
                    totalCount - finishedCount - failedCount,
                    totalCount,
                )

            # Here we want to wait until one of the running jobs completes
            # but multiprocessing does not provide an API for that, for now
            # just sleep a little bit and go back to the loop.
            if jobs.running:
                time.sleep(0.1)

        if jobs.failedNodes:
            # print list of failed jobs
            _LOG.error("Failed jobs:")
            for job in jobs.jobs:
                if job.state != JobState.FINISHED:
                    _LOG.error("  - %s: %s", job.state.name, job)

            # if any job failed raise an exception
            if jobs.failedNodes == jobs.timedOutNodes:
                raise MPTimeoutError("One or more tasks timed out during execution.")
            else:
                raise MPGraphExecutorError("One or more tasks failed or timed out during execution.")

    def getReport(self) -> Optional[Report]:
        # Docstring inherited from base class
        if self.report is None:
            raise RuntimeError("getReport() called before execute()")
        return self.report
